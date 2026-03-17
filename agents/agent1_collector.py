"""
agents/agent1_collector.py — Agent 1: Multi-Source Inventory Collector.
Runs all source collectors in parallel, normalises output, stores to PostgreSQL.
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, List

import structlog
from sqlalchemy.orm import Session

from collectors.hpe_openview import HPEOpenViewCollector
from collectors.aria_vrops import AriaVROPSCollector
from collectors.openstack import OpenStackCollector
from collectors.openshift import OpenShiftCollector
from collectors.hpe_3par import HPE3PARCollector
from core.canonical import InventoryBundle
from core.database import (
    SessionLocal, InventorySnapshot, AgentRun,
    SourceSystem, InventoryObjectType
)
from core.utils import checksum, log as root_log

log = structlog.get_logger(__name__)


def _run_collector(name: str, fn: Callable) -> tuple[str, Any, Exception | None]:
    """Thread wrapper — catches exceptions so one failure doesn't kill others."""
    try:
        result = fn()
        return name, result, None
    except Exception as exc:
        log.error("Collector failed", collector=name, error=str(exc))
        return name, None, exc


def _upsert_snapshot(
    db: Session,
    source: SourceSystem,
    obj_type: InventoryObjectType,
    source_id: str,
    name: str,
    canonical: dict,
    raw: dict,
) -> InventorySnapshot:
    cs = checksum(canonical)
    snap = (
        db.query(InventorySnapshot)
        .filter_by(source_system=source, source_id=source_id)
        .first()
    )
    if snap:
        if snap.checksum != cs:
            snap.canonical_data = canonical
            snap.raw_data       = raw
            snap.checksum       = cs
            snap.name           = name
            snap.last_seen      = datetime.utcnow()
        else:
            snap.last_seen = datetime.utcnow()
    else:
        snap = InventorySnapshot(
            source_system=source,
            object_type=obj_type,
            source_id=source_id,
            name=name,
            canonical_data=canonical,
            raw_data=raw,
            checksum=cs,
        )
        db.add(snap)
    return snap


class Agent1Collector:
    """Orchestrates all source collectors and persists results."""

    AGENT_NAME = "agent1_collector"

    def run(self, run_type: str = "full") -> InventoryBundle:
        log.info("Agent 1 starting", run_type=run_type)
        run_record = AgentRun(agent_name=self.AGENT_NAME, run_type=run_type, status="running")
        db = SessionLocal()
        db.add(run_record)
        db.commit()

        bundle = InventoryBundle()
        processed = 0
        failed = 0

        try:
            # ── Launch collectors in parallel threads ─────────────────────────
            collectors = {
                "hpe_openview": lambda: HPEOpenViewCollector().collect(),
                "aria_vrops":   lambda: AriaVROPSCollector().collect(),
                "openstack":    lambda: OpenStackCollector().collect(),
                "openshift":    lambda: OpenShiftCollector().collect(),
                "hpe_3par":     lambda: HPE3PARCollector().collect(),
            }

            results = {}
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {
                    pool.submit(_run_collector, name, fn): name
                    for name, fn in collectors.items()
                }
                for future in as_completed(futures):
                    name, data, exc = future.result()
                    if exc:
                        failed += 1
                    else:
                        results[name] = data

            # ── HPE OpenView → devices ────────────────────────────────────────
            if "hpe_openview" in results:
                for dev in results["hpe_openview"]:
                    bundle.devices.append(dev)
                    _upsert_snapshot(
                        db, SourceSystem.HPE_OPENVIEW,
                        InventoryObjectType.DEVICE,
                        dev.source_id, dev.name,
                        dev.model_dump(mode="json"),
                        dev.custom_fields,
                    )
                    processed += 1

            # ── vROPS → clusters + VMs ────────────────────────────────────────
            if "aria_vrops" in results:
                clusters, vms = results["aria_vrops"]
                for cl in clusters:
                    bundle.clusters.append(cl)
                    _upsert_snapshot(
                        db, SourceSystem.ARIA_VROPS,
                        InventoryObjectType.CLUSTER,
                        cl.source_id, cl.name,
                        cl.model_dump(mode="json"), {},
                    )
                    processed += 1
                for vm in vms:
                    bundle.virtual_machines.append(vm)
                    _upsert_snapshot(
                        db, SourceSystem.ARIA_VROPS,
                        InventoryObjectType.VIRTUAL_MACHINE,
                        vm.source_id, vm.name,
                        vm.model_dump(mode="json"), {},
                    )
                    processed += 1

            # ── OpenStack → clusters + VMs + networks + volumes ───────────────
            if "openstack" in results:
                clusters, vms, nets, vols = results["openstack"]
                for cl in clusters:
                    bundle.clusters.append(cl)
                    _upsert_snapshot(db, SourceSystem.OPENSTACK, InventoryObjectType.CLUSTER,
                                     cl.source_id, cl.name, cl.model_dump(mode="json"), {})
                    processed += 1
                for vm in vms:
                    bundle.virtual_machines.append(vm)
                    _upsert_snapshot(db, SourceSystem.OPENSTACK, InventoryObjectType.VIRTUAL_MACHINE,
                                     vm.source_id, vm.name, vm.model_dump(mode="json"), {})
                    processed += 1
                for net in nets:
                    bundle.networks.append(net)
                    _upsert_snapshot(db, SourceSystem.OPENSTACK, InventoryObjectType.NETWORK,
                                     net.source_id, net.name, net.model_dump(mode="json"), {})
                    processed += 1
                for vol in vols:
                    bundle.volumes.append(vol)
                    _upsert_snapshot(db, SourceSystem.OPENSTACK, InventoryObjectType.VOLUME,
                                     vol.source_id, vol.name, vol.model_dump(mode="json"), {})
                    processed += 1

            # ── OpenShift → clusters + nodes + namespaces ─────────────────────
            if "openshift" in results:
                clusters, nodes, namespaces = results["openshift"]
                for cl in clusters:
                    bundle.clusters.append(cl)
                    _upsert_snapshot(db, SourceSystem.OPENSHIFT, InventoryObjectType.CLUSTER,
                                     cl.source_id, cl.name, cl.model_dump(mode="json"), {})
                    processed += 1
                for node in nodes:
                    bundle.k8s_nodes.append(node)
                    _upsert_snapshot(db, SourceSystem.OPENSHIFT, InventoryObjectType.K8S_NODE,
                                     node.source_id, node.name, node.model_dump(mode="json"), {})
                    processed += 1
                for ns in namespaces:
                    bundle.k8s_namespaces.append(ns)
                    _upsert_snapshot(db, SourceSystem.OPENSHIFT, InventoryObjectType.K8S_NAMESPACE,
                                     ns.source_id, ns.name, ns.model_dump(mode="json"), {})
                    processed += 1

            # ── HPE 3PAR → storage systems + volumes ──────────────────────────
            if "hpe_3par" in results:
                storage_systems, volumes = results["hpe_3par"]
                for ss in storage_systems:
                    bundle.storage_systems.append(ss)
                    _upsert_snapshot(db, SourceSystem.HPE_3PAR, InventoryObjectType.STORAGE_SYSTEM,
                                     ss.source_id, ss.name, ss.model_dump(mode="json"), {})
                    processed += 1
                for vol in volumes:
                    bundle.volumes.append(vol)
                    _upsert_snapshot(db, SourceSystem.HPE_3PAR, InventoryObjectType.VOLUME,
                                     vol.source_id, vol.name, vol.model_dump(mode="json"), {})
                    processed += 1

            db.commit()
            run_record.status = "success"
            run_record.objects_processed = processed
            run_record.objects_failed = failed
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.info("Agent 1 complete", processed=processed, failed=failed, total=bundle.total())
            return bundle

        except Exception as exc:
            db.rollback()
            run_record.status = "failed"
            run_record.error_detail = str(exc)
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.error("Agent 1 failed", error=str(exc))
            raise
        finally:
            db.close()
