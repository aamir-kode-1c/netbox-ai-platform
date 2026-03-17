"""
agents/agent4_populator.py — Agent 4: NetBox 4.4.1 Hierarchical Populator.
Populates NetBox via REST API in correct dependency order with upsert logic.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pynetbox
import structlog
import urllib3

from agents.agent3_transformer import NetBoxYAMLObject
from core.database import SessionLocal, AgentRun, NetBoxYAMLBatch, InventorySnapshot
from core.settings import settings
from core.utils import get_redis

urllib3.disable_warnings()
log = structlog.get_logger(__name__)

BATCH_SIZE = 50
RETRY_LIMIT = 3


class Agent4Populator:
    AGENT_NAME = "agent4_populator"

    def __init__(self) -> None:
        self.nb = pynetbox.api(settings.netbox_url, token=settings.netbox_token)
        if not settings.netbox_ssl_verify:
            self.nb.http_session.verify = False
        self.redis = get_redis()

    # ── Slug helper ───────────────────────────────────────────────────────────

    @staticmethod
    def _slugify(name: str) -> str:
        import re
        return re.sub(r"[^a-z0-9_-]", "-", name.lower().strip())[:50].strip("-")

    # ── Upsert helpers ────────────────────────────────────────────────────────

    def _upsert(self, endpoint, lookup: Dict, data: Dict) -> Tuple[Any, bool]:
        """Get-or-create with update. Returns (obj, created)."""
        for attempt in range(RETRY_LIMIT):
            try:
                existing = endpoint.get(**lookup)
                if existing:
                    # Update fields that may have changed
                    for k, v in data.items():
                        if k not in ("name", "slug"):
                            try:
                                setattr(existing, k, v)
                            except Exception:
                                pass
                    try:
                        existing.save()
                    except Exception:
                        pass
                    return existing, False
                else:
                    created = endpoint.create(**data)
                    return created, True
            except Exception as exc:
                if attempt == RETRY_LIMIT - 1:
                    log.error("Upsert failed after retries", lookup=lookup, error=str(exc))
                    raise
                time.sleep(2 ** attempt)

    # ── Foundation objects ────────────────────────────────────────────────────

    def _ensure_site(self, name: str) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.dcim.sites,
            {"slug": slug},
            {"name": name, "slug": slug, "status": "active"},
        )
        return obj

    def _ensure_manufacturer(self, name: str) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.dcim.manufacturers,
            {"slug": slug},
            {"name": name, "slug": slug},
        )
        return obj

    def _ensure_device_type(self, model: str, manufacturer_name: str) -> Any:
        slug = self._slugify(f"{manufacturer_name}-{model}")
        mfr = self._ensure_manufacturer(manufacturer_name)
        obj, _ = self._upsert(
            self.nb.dcim.device_types,
            {"slug": slug},
            {"model": model, "slug": slug, "manufacturer": mfr.id},
        )
        return obj

    def _ensure_device_role(self, name: str, vm_role: bool = False) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.dcim.device_roles,
            {"slug": slug},
            {"name": name, "slug": slug, "color": "0066cc", "vm_role": vm_role},
        )
        return obj

    def _ensure_cluster_type(self, name: str) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.virtualization.cluster_types,
            {"slug": slug},
            {"name": name, "slug": slug},
        )
        return obj

    def _ensure_tenant(self, name: str) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.tenancy.tenants,
            {"slug": slug},
            {"name": name, "slug": slug},
        )
        return obj

    def _ensure_tag(self, name: str) -> Any:
        slug = self._slugify(name)
        obj, _ = self._upsert(
            self.nb.extras.tags,
            {"slug": slug},
            {"name": name, "slug": slug, "color": "0099cc"},
        )
        return obj

    # ── Core population methods ───────────────────────────────────────────────

    def _populate_device(self, fields: Dict, source_id: str = None) -> Optional[Any]:
        try:
            name = fields.get("name")
            if not name:
                return None

            # Ensure foreign key objects exist
            site_name = (fields.get("site") or {}).get("name", "default")
            site = self._ensure_site(site_name)

            role_name = (fields.get("role") or fields.get("device_role") or {}).get("name", "server")
            role = self._ensure_device_role(role_name)

            dt_data = fields.get("device_type") or {}
            mfr_name = (dt_data.get("manufacturer") or {}).get("name", "Unknown")
            dt_model  = dt_data.get("model", "Unknown")
            device_type = self._ensure_device_type(dt_model, mfr_name)

            tags = []
            for tag in (fields.get("tags") or []):
                tag_name = tag.get("name") if isinstance(tag, dict) else tag
                t = self._ensure_tag(tag_name)
                tags.append(t.id)

            data = {
                "name": name,
                "site": site.id,
                "role": role.id,
                "device_type": device_type.id,
                "status": fields.get("status", "active"),
                "serial": fields.get("serial") or "",
                "comments": fields.get("comments", ""),
            }
            if fields.get("custom_fields"):
                data["custom_fields"] = {
                    k: str(v) for k, v in fields["custom_fields"].items()
                }
            if tags:
                data["tags"] = tags

            obj, created = self._upsert(self.nb.dcim.devices, {"name": name}, data)
            if created:
                log.info("Created NetBox device", name=name)
            return obj
        except Exception as exc:
            log.error("Device population failed", name=fields.get("name"), error=str(exc))
            return None

    def _populate_cluster(self, fields: Dict) -> Optional[Any]:
        try:
            name = fields.get("name")
            type_name = (fields.get("type") or {}).get("name", "vmware-esxi")
            cl_type = self._ensure_cluster_type(type_name)
            data = {
                "name": name,
                "type": cl_type.id,
                "status": fields.get("status", "active"),
            }
            if fields.get("site"):
                site = self._ensure_site(fields["site"]["name"])
                data["site"] = site.id
            obj, created = self._upsert(self.nb.virtualization.clusters, {"name": name}, data)
            if created:
                log.info("Created NetBox cluster", name=name)
            return obj
        except Exception as exc:
            log.error("Cluster population failed", error=str(exc))
            return None

    def _populate_vm(self, fields: Dict) -> Optional[Any]:
        try:
            name = fields.get("name")
            cluster_name = (fields.get("cluster") or {}).get("name", "default")
            cluster = self._ensure_cluster_type("vmware-esxi")  # ensure type exists

            # get cluster obj
            cl_obj = self.nb.virtualization.clusters.get(name=cluster_name)
            if not cl_obj:
                cl_obj = self.nb.virtualization.clusters.create(
                    name=cluster_name, type=cluster.id, status="active"
                )

            data = {
                "name": name,
                "cluster": cl_obj.id,
                "status": fields.get("status", "active"),
                "vcpus": fields.get("vcpus") or fields.get("cpu"),
                "memory": fields.get("memory") or fields.get("memory_mb"),
                "disk": fields.get("disk"),
            }
            data = {k: v for k, v in data.items() if v is not None}

            if fields.get("tenant"):
                t = self._ensure_tenant(fields["tenant"]["name"])
                data["tenant"] = t.id

            if fields.get("custom_fields"):
                data["custom_fields"] = {k: str(v) for k, v in fields["custom_fields"].items()}

            obj, created = self._upsert(self.nb.virtualization.virtual_machines, {"name": name}, data)
            if created:
                log.info("Created NetBox VM", name=name)
            return obj
        except Exception as exc:
            log.error("VM population failed", name=fields.get("name"), error=str(exc))
            return None

    def _populate_vlan(self, fields: Dict) -> Optional[Any]:
        try:
            vid  = fields.get("vid")
            name = fields.get("name", f"VLAN-{vid}")
            if not vid:
                return None
            data = {"vid": vid, "name": name, "status": fields.get("status", "active")}
            obj, created = self._upsert(self.nb.ipam.vlans, {"vid": vid}, data)
            if created:
                log.info("Created NetBox VLAN", vid=vid, name=name)
            return obj
        except Exception as exc:
            log.error("VLAN population failed", error=str(exc))
            return None

    # ── Process YAML batch queue from DB ──────────────────────────────────────

    def _process_db_queue(self) -> int:
        import yaml as yaml_lib
        db = SessionLocal()
        processed = 0
        try:
            pending = (
                db.query(NetBoxYAMLBatch)
                .filter_by(populated=False)
                .limit(BATCH_SIZE * 10)
                .all()
            )
            for batch in pending:
                try:
                    fields = yaml_lib.safe_load(batch.yaml_content) or {}
                    obj = None

                    if batch.netbox_model == "device":
                        obj = self._populate_device(fields)
                    elif batch.netbox_model == "virtual_machine":
                        obj = self._populate_vm(fields)
                    elif batch.netbox_model == "cluster":
                        obj = self._populate_cluster(fields)
                    elif batch.netbox_model == "vlan":
                        obj = self._populate_vlan(fields)

                    if obj:
                        batch.populated = True
                        batch.populated_at = datetime.utcnow()
                        # Update snapshot with netbox_id
                        snap = (
                            db.query(InventorySnapshot)
                            .filter_by(object_type=batch.object_type)
                            .first()
                        )
                        if snap:
                            snap.netbox_id = obj.id
                            snap.netbox_type = f"{batch.netbox_app}.{batch.netbox_model}"
                        processed += 1
                except Exception as exc:
                    log.error("Batch item failed", batch_id=batch.id, error=str(exc))

            db.commit()
        finally:
            db.close()
        return processed

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self, yaml_objects: List[NetBoxYAMLObject] | None = None) -> int:
        """
        Populate NetBox. If yaml_objects provided, uses them directly.
        Otherwise drains the DB queue from Agent 3.
        """
        log.info("Agent 4 starting NetBox population")
        run_record = AgentRun(agent_name=self.AGENT_NAME, run_type="populate", status="running")
        db = SessionLocal()
        db.add(run_record)
        db.commit()
        db.close()

        processed = 0
        try:
            if yaml_objects:
                # Population order: sites/roles/types first, then devices, then VMs
                order = ["cluster", "device", "virtual_machine", "vlan"]
                sorted_objs = sorted(
                    yaml_objects,
                    key=lambda o: order.index(o.netbox_model) if o.netbox_model in order else 99
                )
                for obj in sorted_objs:
                    try:
                        if obj.netbox_model == "device":
                            self._populate_device(obj.fields)
                        elif obj.netbox_model == "virtual_machine":
                            self._populate_vm(obj.fields)
                        elif obj.netbox_model == "cluster":
                            self._populate_cluster(obj.fields)
                        elif obj.netbox_model == "vlan":
                            self._populate_vlan(obj.fields)
                        processed += 1
                    except Exception as exc:
                        log.error("Object population failed", model=obj.netbox_model, error=str(exc))
            else:
                processed = self._process_db_queue()

            db = SessionLocal()
            run = db.query(AgentRun).filter_by(agent_name=self.AGENT_NAME, status="running").first()
            if run:
                run.status = "success"
                run.objects_processed = processed
                run.finished_at = datetime.utcnow()
                db.commit()
            db.close()

            log.info("Agent 4 complete", populated=processed)
            return processed

        except Exception as exc:
            db = SessionLocal()
            run = db.query(AgentRun).filter_by(agent_name=self.AGENT_NAME, status="running").first()
            if run:
                run.status = "failed"
                run.error_detail = str(exc)
                run.finished_at = datetime.utcnow()
                db.commit()
            db.close()
            log.error("Agent 4 failed", error=str(exc))
            raise
