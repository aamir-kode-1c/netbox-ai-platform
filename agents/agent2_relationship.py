"""
agents/agent2_relationship.py — Agent 2: End-to-End Relationship Mapper.
Builds cross-system dependency graph using IP/hostname/WWN matching + LLM assist.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import networkx as nx
import structlog
from sqlalchemy.orm import Session

from core.canonical import InventoryBundle
from core.database import (
    SessionLocal, InventorySnapshot, RelationshipEdge, AgentRun,
    SourceSystem, InventoryObjectType
)
from core.utils import OllamaClient

log = structlog.get_logger(__name__)


class RelationshipMapper:
    """Builds a NetworkX directed graph of cross-system inventory relationships."""

    AGENT_NAME = "agent2_relationship"

    def __init__(self) -> None:
        self.llm   = OllamaClient()
        self.graph = nx.DiGraph()

    # ── Index builders ────────────────────────────────────────────────────────

    def _build_ip_index(self, bundle: InventoryBundle) -> Dict[str, Tuple[str, str]]:
        """Map IP address → (source_id, object_type)."""
        idx: Dict[str, Tuple[str, str]] = {}

        def add(ips, sid, otype):
            for ip in (ips or []):
                clean = ip.split("/")[0].strip()
                if clean:
                    idx[clean] = (sid, otype)

        for d in bundle.devices:
            add([d.primary_ip4, d.primary_ip6], d.source_id, "device")
            for iface in d.interfaces:
                add(iface.ip_addresses, d.source_id, "device")

        for vm in bundle.virtual_machines:
            add([vm.primary_ip4, vm.primary_ip6], vm.source_id, "virtual_machine")
            for iface in vm.interfaces:
                add(iface.ip_addresses, vm.source_id, "virtual_machine")

        for node in bundle.k8s_nodes:
            add([node.primary_ip4], node.source_id, "k8s_node")

        return idx

    def _build_name_index(self, bundle: InventoryBundle) -> Dict[str, Tuple[str, str]]:
        """Map normalised hostname → (source_id, object_type)."""
        idx: Dict[str, Tuple[str, str]] = {}

        def norm(name: Optional[str]) -> str:
            if not name:
                return ""
            return re.sub(r"[^a-z0-9]", "", name.lower().split(".")[0])

        for d in bundle.devices:
            k = norm(d.name)
            if k:
                idx[k] = (d.source_id, "device")
            if d.fqdn:
                idx[norm(d.fqdn)] = (d.source_id, "device")

        for vm in bundle.virtual_machines:
            k = norm(vm.name)
            if k:
                idx[k] = (vm.source_id, "virtual_machine")

        for node in bundle.k8s_nodes:
            idx[norm(node.name)] = (node.source_id, "k8s_node")

        for ss in bundle.storage_systems:
            idx[norm(ss.name)] = (ss.source_id, "storage_system")

        return idx

    # ── Relationship detection ─────────────────────────────────────────────────

    def _match_vm_to_host(
        self,
        bundle: InventoryBundle,
        ip_idx: Dict,
        name_idx: Dict,
        db: Session,
    ) -> List[RelationshipEdge]:
        edges = []
        for vm in bundle.virtual_machines:
            host_src_id = None
            resolved_by = None

            # Method 1: explicit host_device_name field
            if vm.host_device_name:
                key = re.sub(r"[^a-z0-9]", "", vm.host_device_name.lower().split(".")[0])
                if key in name_idx:
                    host_src_id, _ = name_idx[key]
                    resolved_by = "name_match"

            # Method 2: LLM-assisted fuzzy match
            if not host_src_id and vm.host_device_name:
                host_src_id = self._llm_resolve_host(vm.host_device_name, bundle)
                if host_src_id:
                    resolved_by = "llm"

            if host_src_id:
                edge = RelationshipEdge(
                    from_source=vm.source_system,
                    from_source_id=host_src_id,
                    from_object_type=InventoryObjectType.DEVICE,
                    to_source=vm.source_system,
                    to_source_id=vm.source_id,
                    to_object_type=InventoryObjectType.VIRTUAL_MACHINE,
                    relationship_type="hosts_vm",
                    resolved_by=resolved_by,
                )
                edges.append(edge)
                self.graph.add_edge(host_src_id, vm.source_id, rel="hosts_vm")
        return edges

    def _match_vm_to_cluster(
        self, bundle: InventoryBundle, db: Session
    ) -> List[RelationshipEdge]:
        cluster_map = {
            re.sub(r"[^a-z0-9]", "", cl.name.lower()): cl.source_id
            for cl in bundle.clusters
        }
        edges = []
        for vm in bundle.virtual_machines:
            if not vm.cluster_name:
                continue
            key = re.sub(r"[^a-z0-9]", "", vm.cluster_name.lower())
            cl_id = cluster_map.get(key)
            if cl_id:
                edges.append(RelationshipEdge(
                    from_source=SourceSystem(vm.source_system),
                    from_source_id=cl_id,
                    from_object_type=InventoryObjectType.CLUSTER,
                    to_source=SourceSystem(vm.source_system),
                    to_source_id=vm.source_id,
                    to_object_type=InventoryObjectType.VIRTUAL_MACHINE,
                    relationship_type="cluster_contains_vm",
                    resolved_by="name_match",
                ))
                self.graph.add_edge(cl_id, vm.source_id, rel="cluster_contains_vm")
        return edges

    def _match_volume_to_host(
        self, bundle: InventoryBundle, name_idx: Dict, db: Session
    ) -> List[RelationshipEdge]:
        edges = []
        for vol in bundle.volumes:
            if not vol.attached_to_host:
                continue
            key = re.sub(r"[^a-z0-9]", "", vol.attached_to_host.lower())
            if key in name_idx:
                host_id, host_type = name_idx[key]
                obj_type = (
                    InventoryObjectType.DEVICE
                    if host_type == "device"
                    else InventoryObjectType.VIRTUAL_MACHINE
                )
                edges.append(RelationshipEdge(
                    from_source=SourceSystem(vol.source_system),
                    from_source_id=host_id,
                    from_object_type=obj_type,
                    to_source=SourceSystem(vol.source_system),
                    to_source_id=vol.source_id,
                    to_object_type=InventoryObjectType.VOLUME,
                    relationship_type="attached_volume",
                    resolved_by="name_match",
                ))
                self.graph.add_edge(host_id, vol.source_id, rel="attached_volume")
        return edges

    def _match_k8s_node_to_vm(
        self, bundle: InventoryBundle, ip_idx: Dict, name_idx: Dict
    ) -> List[RelationshipEdge]:
        edges = []
        for node in bundle.k8s_nodes:
            vm_id = None
            resolved_by = None

            if node.primary_ip4 and node.primary_ip4 in ip_idx:
                vm_id, _ = ip_idx[node.primary_ip4]
                resolved_by = "ip_match"
            elif node.name:
                key = re.sub(r"[^a-z0-9]", "", node.name.lower().split(".")[0])
                if key in name_idx and name_idx[key][1] == "virtual_machine":
                    vm_id, _ = name_idx[key]
                    resolved_by = "name_match"

            if vm_id:
                edges.append(RelationshipEdge(
                    from_source=SourceSystem(node.source_system),
                    from_source_id=vm_id,
                    from_object_type=InventoryObjectType.VIRTUAL_MACHINE,
                    to_source=SourceSystem(node.source_system),
                    to_source_id=node.source_id,
                    to_object_type=InventoryObjectType.K8S_NODE,
                    relationship_type="vm_runs_k8s_node",
                    resolved_by=resolved_by,
                ))
                self.graph.add_edge(vm_id, node.source_id, rel="vm_runs_k8s_node")
        return edges

    # ── LLM assist ────────────────────────────────────────────────────────────

    def _llm_resolve_host(self, host_hint: str, bundle: InventoryBundle) -> Optional[str]:
        """Ask LLM to match a host hint against known device names."""
        device_names = [d.name for d in bundle.devices[:50]]  # limit context
        if not device_names:
            return None
        prompt = (
            f"Given the host hint: '{host_hint}'\n"
            f"And this list of known physical server names: {device_names}\n"
            f"Which server name best matches? Reply with ONLY the exact server name from the list, "
            f"or 'NONE' if no match."
        )
        try:
            response = self.llm.chat(prompt, temperature=0.0, max_tokens=64)
            response = response.strip().strip('"')
            for dev in bundle.devices:
                if dev.name.lower() == response.lower():
                    return dev.source_id
        except Exception as exc:
            log.warning("LLM host resolution failed", error=str(exc))
        return None

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_edges(self, edges: List[RelationshipEdge], db: Session) -> None:
        for edge in edges:
            existing = (
                db.query(RelationshipEdge)
                .filter_by(
                    from_source_id=edge.from_source_id,
                    to_source_id=edge.to_source_id,
                    relationship_type=edge.relationship_type,
                )
                .first()
            )
            if not existing:
                db.add(edge)
        db.commit()

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self, bundle: InventoryBundle) -> nx.DiGraph:
        log.info("Agent 2 starting relationship mapping")
        run_record = AgentRun(agent_name=self.AGENT_NAME, run_type="mapping", status="running")
        db = SessionLocal()
        db.add(run_record)
        db.commit()

        try:
            ip_idx   = self._build_ip_index(bundle)
            name_idx = self._build_name_index(bundle)

            all_edges: List[RelationshipEdge] = []
            all_edges.extend(self._match_vm_to_host(bundle, ip_idx, name_idx, db))
            all_edges.extend(self._match_vm_to_cluster(bundle, db))
            all_edges.extend(self._match_volume_to_host(bundle, name_idx, db))
            all_edges.extend(self._match_k8s_node_to_vm(bundle, ip_idx, name_idx))

            self._save_edges(all_edges, db)

            run_record.status = "success"
            run_record.objects_processed = len(all_edges)
            run_record.finished_at = datetime.utcnow()
            db.commit()

            log.info("Agent 2 complete",
                     relationships=len(all_edges),
                     graph_nodes=self.graph.number_of_nodes(),
                     graph_edges=self.graph.number_of_edges())
            return self.graph

        except Exception as exc:
            db.rollback()
            run_record.status = "failed"
            run_record.error_detail = str(exc)
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.error("Agent 2 failed", error=str(exc))
            raise
        finally:
            db.close()
