"""
agents/agent6_lifecycle.py — Agent 6: Lifecycle Manager.
Monitors all NetBox objects, applies state machine transitions, adds journal entries.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

import pynetbox
import structlog
import urllib3

from core.database import (
    SessionLocal, InventorySnapshot, AgentRun,
    LifecycleStatus, InventoryObjectType
)
from core.settings import settings
from core.utils import OllamaClient

urllib3.disable_warnings()
log = structlog.get_logger(__name__)

# Mapping: NetBox status ↔ LifecycleStatus
NB_TO_LIFECYCLE = {
    "planned":         LifecycleStatus.PLANNED,
    "staged":          LifecycleStatus.STAGED,
    "active":          LifecycleStatus.ACTIVE,
    "offline":         LifecycleStatus.OFFLINE,
    "decommissioning": LifecycleStatus.DECOMMISSIONING,
    "retired":         LifecycleStatus.RETIRED,
}
LIFECYCLE_TO_NB = {v: k for k, v in NB_TO_LIFECYCLE.items()}


class Agent6LifecycleManager:
    AGENT_NAME = "agent6_lifecycle"

    def __init__(self) -> None:
        self.nb  = pynetbox.api(settings.netbox_url, token=settings.netbox_token)
        if not settings.netbox_ssl_verify:
            self.nb.http_session.verify = False
        self.llm = OllamaClient()

    # ── State transition rules ────────────────────────────────────────────────

    def _compute_target_status(
        self,
        snap: InventorySnapshot,
        now: datetime,
    ) -> Optional[LifecycleStatus]:
        """
        Given current snapshot state, determine if a lifecycle transition is warranted.
        Returns new LifecycleStatus or None if no change needed.
        """
        current = snap.lifecycle_status
        last_seen = snap.last_seen or snap.first_seen or now
        days_since_seen = (now - last_seen).days

        # Determine grace period by object type
        if snap.object_type in (
            InventoryObjectType.VIRTUAL_MACHINE,
            InventoryObjectType.K8S_NODE,
            InventoryObjectType.K8S_NAMESPACE,
        ):
            grace = settings.lifecycle_vm_grace_days
        elif snap.object_type in (
            InventoryObjectType.VOLUME,
            InventoryObjectType.NETWORK,
        ):
            grace = settings.lifecycle_vm_grace_days
        else:
            grace = settings.lifecycle_server_grace_days

        # State machine transitions
        if current == LifecycleStatus.DECOMMISSIONING:
            if days_since_seen >= grace:
                return LifecycleStatus.RETIRED

        if current == LifecycleStatus.ACTIVE:
            # Check canonical data for offline signals
            data = snap.canonical_data or {}
            status_field = data.get("status", "active").lower()
            if status_field in ("offline", "shutoff", "stopped", "suspended"):
                return LifecycleStatus.OFFLINE

        if current == LifecycleStatus.OFFLINE:
            # If back online in source
            data = snap.canonical_data or {}
            status_field = data.get("status", "offline").lower()
            if status_field in ("active", "on", "running", "poweredon"):
                return LifecycleStatus.ACTIVE
            # Grace period exceeded → decommission
            if days_since_seen >= grace:
                return LifecycleStatus.DECOMMISSIONING

        if current == LifecycleStatus.STAGED:
            data = snap.canonical_data or {}
            status_field = data.get("status", "staged").lower()
            if status_field in ("active", "running", "on"):
                return LifecycleStatus.ACTIVE

        return None

    # ── LLM ambiguity resolver ────────────────────────────────────────────────

    def _llm_resolve_transition(
        self,
        snap: InventorySnapshot,
        proposed: LifecycleStatus,
    ) -> Tuple[LifecycleStatus, str]:
        """
        Use LLM to validate ambiguous transitions (e.g., delete vs migrate).
        Returns (confirmed_status, reasoning).
        """
        prompt = (
            f"An IT inventory management system is considering changing a {snap.object_type} "
            f"named '{snap.name}' from status '{snap.lifecycle_status}' to '{proposed}'.\n"
            f"Current data: {snap.canonical_data}\n"
            f"Days since last seen: {(datetime.utcnow() - (snap.last_seen or datetime.utcnow())).days}\n\n"
            f"Is this transition correct? Reply with JSON only: "
            f'{{ "confirm": true/false, "reason": "brief explanation", "suggested_status": "{proposed}" }}'
        )
        try:
            raw = self.llm.chat(prompt, temperature=0.0, max_tokens=256)
            import json, re
            raw = re.sub(r"```json|```", "", raw).strip()
            data = json.loads(raw)
            if data.get("confirm"):
                return proposed, data.get("reason", "LLM confirmed")
            else:
                suggested = data.get("suggested_status", snap.lifecycle_status)
                return LifecycleStatus(suggested), data.get("reason", "LLM overrode")
        except Exception as exc:
            log.warning("LLM lifecycle resolve failed, using rule-based", error=str(exc))
            return proposed, "Rule-based (LLM unavailable)"

    # ── NetBox status update ──────────────────────────────────────────────────

    def _update_netbox_status(self, snap: InventorySnapshot, new_status: LifecycleStatus) -> bool:
        """Update the corresponding NetBox object's status."""
        if not snap.netbox_id or not snap.netbox_type:
            return False

        nb_status = LIFECYCLE_TO_NB.get(new_status, "active")

        try:
            app, model = snap.netbox_type.split(".")
            endpoint = getattr(getattr(self.nb, app), model + "s")
            obj = endpoint.get(snap.netbox_id)
            if obj:
                obj.status = nb_status
                obj.save()
                return True
        except Exception as exc:
            log.error("NetBox status update failed",
                      nb_id=snap.netbox_id, type=snap.netbox_type, error=str(exc))
        return False

    def _add_journal_entry(
        self, snap: InventorySnapshot, message: str, old_status: str, new_status: str
    ) -> None:
        """Write a journal entry to the NetBox object."""
        if not snap.netbox_id or not snap.netbox_type:
            return
        try:
            self.nb.extras.journal_entries.create(
                assigned_object_type=snap.netbox_type,
                assigned_object_id=snap.netbox_id,
                kind="info",
                comments=(
                    f"**Lifecycle Agent:** {message}\n"
                    f"- Previous status: `{old_status}`\n"
                    f"- New status: `{new_status}`\n"
                    f"- Agent timestamp: {datetime.utcnow().isoformat()}"
                ),
            )
        except Exception as exc:
            log.warning("Journal entry failed", error=str(exc))

    # ── Full scan ─────────────────────────────────────────────────────────────

    def _scan_all_snapshots(self, db) -> Tuple[int, int]:
        """Scan every snapshot and apply lifecycle transitions."""
        transitions = 0
        skipped = 0
        now = datetime.utcnow()

        snaps = db.query(InventorySnapshot).all()
        for snap in snaps:
            if snap.lifecycle_status == LifecycleStatus.RETIRED:
                skipped += 1
                continue

            target = self._compute_target_status(snap, now)
            if target is None:
                skipped += 1
                continue

            # Use LLM for ambiguous transitions (decommission → retire)
            use_llm = target in (LifecycleStatus.RETIRED, LifecycleStatus.DECOMMISSIONING)
            if use_llm:
                target, reasoning = self._llm_resolve_transition(snap, target)
            else:
                reasoning = "Rule-based transition"

            old_status = str(snap.lifecycle_status)
            snap.lifecycle_status = target
            db.add(snap)

            nb_updated = self._update_netbox_status(snap, target)
            self._add_journal_entry(snap, reasoning, old_status, str(target))

            log.info("Lifecycle transition applied",
                     name=snap.name, from_status=old_status,
                     to_status=str(target), nb_updated=nb_updated,
                     reason=reasoning)
            transitions += 1

        db.commit()
        return transitions, skipped

    # ── Retire objects missing from NetBox ────────────────────────────────────

    def _sync_retired_from_netbox(self, db) -> int:
        """
        Find snapshots with netbox_id where NetBox object no longer exists.
        Mark as retired.
        """
        retired_count = 0
        snaps = (
            db.query(InventorySnapshot)
            .filter(
                InventorySnapshot.netbox_id.isnot(None),
                InventorySnapshot.lifecycle_status != LifecycleStatus.RETIRED,
            )
            .all()
        )
        for snap in snaps:
            try:
                app, model = (snap.netbox_type or "dcim.device").split(".")
                endpoint = getattr(getattr(self.nb, app), model + "s")
                obj = endpoint.get(snap.netbox_id)
                if not obj:
                    snap.lifecycle_status = LifecycleStatus.RETIRED
                    db.add(snap)
                    retired_count += 1
                    log.info("Object retired — not found in NetBox", name=snap.name)
            except Exception:
                pass
        db.commit()
        return retired_count

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        log.info("Agent 6 lifecycle scan starting")
        run_record = AgentRun(
            agent_name=self.AGENT_NAME, run_type="lifecycle_scan", status="running"
        )
        db = SessionLocal()
        db.add(run_record)
        db.commit()

        try:
            transitions, skipped = self._scan_all_snapshots(db)
            retired_from_nb = self._sync_retired_from_netbox(db)

            run_record.status = "success"
            run_record.objects_processed = transitions + retired_from_nb
            run_record.finished_at = datetime.utcnow()
            run_record.meta = {
                "transitions": transitions,
                "skipped": skipped,
                "retired_from_netbox": retired_from_nb,
            }
            db.commit()

            result = {
                "transitions": transitions,
                "skipped": skipped,
                "retired_from_netbox": retired_from_nb,
            }
            log.info("Agent 6 lifecycle scan complete", **result)
            return result

        except Exception as exc:
            db.rollback()
            run_record.status = "failed"
            run_record.error_detail = str(exc)
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.error("Agent 6 failed", error=str(exc))
            raise
        finally:
            db.close()
