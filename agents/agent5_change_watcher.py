"""
agents/agent5_change_watcher.py — Agent 5: Change Detection & Incremental Sync.
Polls source systems, detects deltas via DeepDiff, enqueues changed objects.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog
from deepdiff import DeepDiff
from sqlalchemy.orm import Session

from collectors.hpe_openview import HPEOpenViewCollector
from collectors.aria_vrops import AriaVROPSCollector
from collectors.openstack import OpenStackCollector
from collectors.openshift import OpenShiftCollector
from collectors.hpe_3par import HPE3PARCollector
from core.database import (
    SessionLocal, InventorySnapshot, ChangeEvent, AgentRun,
    SourceSystem, InventoryObjectType, ChangeOperation, LifecycleStatus
)
from core.settings import settings
from core.utils import checksum, get_redis, log as root_log

log = structlog.get_logger(__name__)

CHANGE_QUEUE_KEY = "netbox_ai:change_queue"


class Agent5ChangeWatcher:
    AGENT_NAME = "agent5_change_watcher"

    def __init__(self) -> None:
        self.redis = get_redis()

    # ── Delta detection ───────────────────────────────────────────────────────

    def _detect_changes(
        self,
        db: Session,
        source: SourceSystem,
        obj_type: InventoryObjectType,
        source_id: str,
        new_canonical: Dict,
        name: str,
    ) -> Optional[ChangeEvent]:
        """Compare new data against snapshot — return ChangeEvent if changed."""
        new_cs = checksum(new_canonical)
        snap = (
            db.query(InventorySnapshot)
            .filter_by(source_system=source, source_id=source_id)
            .first()
        )

        if not snap:
            # New object
            snap = InventorySnapshot(
                source_system=source,
                object_type=obj_type,
                source_id=source_id,
                name=name,
                canonical_data=new_canonical,
                checksum=new_cs,
                lifecycle_status=LifecycleStatus.ACTIVE,
            )
            db.add(snap)
            event = ChangeEvent(
                source_system=source,
                object_type=obj_type,
                source_id=source_id,
                operation=ChangeOperation.CREATE,
                diff={"new": new_canonical},
            )
            db.add(event)
            log.info("New object detected", source=source, type=obj_type, name=name)
            return event

        if snap.checksum != new_cs:
            # Changed object — compute diff
            diff = DeepDiff(
                snap.canonical_data,
                new_canonical,
                ignore_order=True,
                ignore_numeric_type_changes=True,
            ).to_dict()

            snap.canonical_data = new_canonical
            snap.checksum       = new_cs
            snap.name           = name
            snap.last_seen      = datetime.utcnow()

            event = ChangeEvent(
                snapshot_id=snap.id,
                source_system=source,
                object_type=obj_type,
                source_id=source_id,
                operation=ChangeOperation.UPDATE,
                diff=diff,
            )
            db.add(event)
            log.info("Changed object detected",
                     source=source, type=obj_type, name=name, diff_keys=list(diff.keys()))
            return event

        # No change — just update last_seen
        snap.last_seen = datetime.utcnow()
        return None

    def _detect_deletions(
        self, db: Session, source: SourceSystem, seen_ids: List[str]
    ) -> List[ChangeEvent]:
        """Find objects that were in DB but not seen in latest poll."""
        events = []
        all_snaps = db.query(InventorySnapshot).filter_by(source_system=source).all()
        seen_set  = set(seen_ids)
        for snap in all_snaps:
            if snap.source_id not in seen_set and snap.lifecycle_status != LifecycleStatus.RETIRED:
                event = ChangeEvent(
                    snapshot_id=snap.id,
                    source_system=source,
                    object_type=snap.object_type,
                    source_id=snap.source_id,
                    operation=ChangeOperation.DELETE,
                    diff={"reason": "not_seen_in_poll"},
                )
                db.add(event)
                snap.lifecycle_status = LifecycleStatus.DECOMMISSIONING
                events.append(event)
                log.info("Deletion detected", source=source, name=snap.name)
        return events

    # ── Push to Redis change queue ─────────────────────────────────────────────

    def _enqueue_change(self, event: ChangeEvent) -> None:
        payload = json.dumps({
            "source_system": event.source_system,
            "object_type":   event.object_type,
            "source_id":     event.source_id,
            "operation":     event.operation,
            "diff":          event.diff,
            "enqueued_at":   datetime.utcnow().isoformat(),
        }, default=str)
        self.redis.lpush(CHANGE_QUEUE_KEY, payload)

    # ── Per-source polling ────────────────────────────────────────────────────

    def _poll_openview(self, db: Session) -> int:
        changes = 0
        try:
            devices = HPEOpenViewCollector().collect()
            seen_ids = [d.source_id for d in devices]
            for dev in devices:
                ev = self._detect_changes(
                    db, SourceSystem.HPE_OPENVIEW,
                    InventoryObjectType.DEVICE,
                    dev.source_id, dev.model_dump(mode="json"), dev.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
            self._detect_deletions(db, SourceSystem.HPE_OPENVIEW, seen_ids)
        except Exception as exc:
            log.error("OpenView poll failed", error=str(exc))
        return changes

    def _poll_vrops(self, db: Session) -> int:
        changes = 0
        try:
            clusters, vms = AriaVROPSCollector().collect()
            seen_ids = [c.source_id for c in clusters] + [v.source_id for v in vms]
            for cl in clusters:
                ev = self._detect_changes(
                    db, SourceSystem.ARIA_VROPS, InventoryObjectType.CLUSTER,
                    cl.source_id, cl.model_dump(mode="json"), cl.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
            for vm in vms:
                ev = self._detect_changes(
                    db, SourceSystem.ARIA_VROPS, InventoryObjectType.VIRTUAL_MACHINE,
                    vm.source_id, vm.model_dump(mode="json"), vm.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
        except Exception as exc:
            log.error("vROPS poll failed", error=str(exc))
        return changes

    def _poll_openstack(self, db: Session) -> int:
        changes = 0
        try:
            clusters, vms, nets, vols = OpenStackCollector().collect()
            for item in list(clusters) + list(vms) + list(nets) + list(vols):
                obj_type_map = {
                    "CanonicalCluster":       InventoryObjectType.CLUSTER,
                    "CanonicalVirtualMachine": InventoryObjectType.VIRTUAL_MACHINE,
                    "CanonicalNetwork":       InventoryObjectType.NETWORK,
                    "CanonicalVolume":        InventoryObjectType.VOLUME,
                }
                otype = obj_type_map.get(type(item).__name__, InventoryObjectType.VIRTUAL_MACHINE)
                ev = self._detect_changes(
                    db, SourceSystem.OPENSTACK, otype,
                    item.source_id, item.model_dump(mode="json"), item.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
        except Exception as exc:
            log.error("OpenStack poll failed", error=str(exc))
        return changes

    def _poll_openshift(self, db: Session) -> int:
        changes = 0
        try:
            clusters, nodes, namespaces = OpenShiftCollector().collect()
            for item in list(clusters) + list(nodes) + list(namespaces):
                otype_map = {
                    "CanonicalCluster":       InventoryObjectType.CLUSTER,
                    "CanonicalK8sNode":       InventoryObjectType.K8S_NODE,
                    "CanonicalK8sNamespace":  InventoryObjectType.K8S_NAMESPACE,
                }
                otype = otype_map.get(type(item).__name__, InventoryObjectType.K8S_NODE)
                ev = self._detect_changes(
                    db, SourceSystem.OPENSHIFT, otype,
                    item.source_id, item.model_dump(mode="json"), item.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
        except Exception as exc:
            log.error("OpenShift poll failed", error=str(exc))
        return changes

    def _poll_3par(self, db: Session) -> int:
        changes = 0
        try:
            systems, volumes = HPE3PARCollector().collect()
            for item in list(systems) + list(volumes):
                otype = (InventoryObjectType.STORAGE_SYSTEM
                         if type(item).__name__ == "CanonicalStorageSystem"
                         else InventoryObjectType.VOLUME)
                ev = self._detect_changes(
                    db, SourceSystem.HPE_3PAR, otype,
                    item.source_id, item.model_dump(mode="json"), item.name,
                )
                if ev:
                    self._enqueue_change(ev)
                    changes += 1
        except Exception as exc:
            log.error("3PAR poll failed", error=str(exc))
        return changes

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self) -> int:
        """Run one poll cycle across all sources. Returns total changes detected."""
        log.info("Agent 5 change watch cycle starting")
        run_record = AgentRun(agent_name=self.AGENT_NAME, run_type="poll", status="running")
        db = SessionLocal()
        db.add(run_record)
        db.commit()

        total_changes = 0
        try:
            total_changes += self._poll_openview(db)
            total_changes += self._poll_vrops(db)
            total_changes += self._poll_openstack(db)
            total_changes += self._poll_openshift(db)
            total_changes += self._poll_3par(db)

            db.commit()
            run_record.status = "success"
            run_record.objects_processed = total_changes
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.info("Agent 5 cycle complete", changes=total_changes)
            return total_changes

        except Exception as exc:
            db.rollback()
            run_record.status = "failed"
            run_record.error_detail = str(exc)
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.error("Agent 5 failed", error=str(exc))
            raise
        finally:
            db.close()

    def get_queue_depth(self) -> int:
        """How many unprocessed changes are in the Redis queue."""
        return self.redis.llen(CHANGE_QUEUE_KEY)

    def consume_change(self) -> Optional[Dict]:
        """Pop and return one change from the queue."""
        raw = self.redis.rpop(CHANGE_QUEUE_KEY)
        if raw:
            return json.loads(raw)
        return None
