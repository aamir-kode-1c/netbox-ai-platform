"""
collectors/openshift.py — OpenShift / Kubernetes collector via kubernetes-client.
Collects nodes, namespaces, and pods.
"""
from __future__ import annotations

from typing import List

import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from core.canonical import CanonicalCluster, CanonicalK8sNode, CanonicalK8sNamespace
from core.settings import settings

log = structlog.get_logger(__name__)


class OpenShiftCollector:
    SOURCE = "openshift"

    def __init__(self) -> None:
        self._loaded = False

    def _load_config(self) -> None:
        if not self._loaded:
            try:
                config.load_kube_config(config_file=settings.kubeconfig)
            except Exception:
                try:
                    config.load_incluster_config()
                except Exception as exc:
                    log.error("Cannot load K8s config", error=str(exc))
                    raise
            if not settings.k8s_verify_ssl:
                client.Configuration._default.verify_ssl = False
            self._loaded = True
            log.info("Kubernetes config loaded")

    # ── Cluster name from context ─────────────────────────────────────────────

    def _cluster_name(self) -> str:
        try:
            contexts, active = config.list_kube_config_contexts(config_file=settings.kubeconfig)
            return active.get("context", {}).get("cluster", "openshift-cluster")
        except Exception:
            return "openshift-cluster"

    # ── Nodes ─────────────────────────────────────────────────────────────────

    def _collect_nodes(self, cluster_name: str) -> List[CanonicalK8sNode]:
        v1 = client.CoreV1Api()
        nodes = []
        try:
            for node in v1.list_node().items:
                meta  = node.metadata
                spec  = node.spec
                info  = node.status.node_info
                addrs = node.status.addresses or []

                # Determine primary IP
                primary_ip = None
                for addr in addrs:
                    if addr.type == "InternalIP":
                        primary_ip = addr.address
                        break

                # Determine role
                labels = meta.labels or {}
                if labels.get("node-role.kubernetes.io/master") is not None or \
                   labels.get("node-role.kubernetes.io/control-plane") is not None:
                    role = "master"
                elif labels.get("node-role.kubernetes.io/infra") is not None:
                    role = "infra"
                else:
                    role = "worker"

                # Node ready status
                ready = False
                for cond in (node.status.conditions or []):
                    if cond.type == "Ready":
                        ready = cond.status == "True"
                        break
                status = "active" if ready else "offline"

                # Capacity
                capacity = node.status.capacity or {}
                cpu_cap = None
                mem_mb  = None
                try:
                    cpu_cap = int(capacity.get("cpu", 0))
                except Exception:
                    pass
                try:
                    mem_ki = capacity.get("memory", "0Ki").replace("Ki", "")
                    mem_mb = int(mem_ki) // 1024
                except Exception:
                    pass

                nodes.append(CanonicalK8sNode(
                    source_system=self.SOURCE,
                    source_id=meta.uid,
                    name=meta.name,
                    role=role,
                    status=status,
                    os_image=info.os_image if info else None,
                    kernel_version=info.kernel_version if info else None,
                    container_runtime=info.container_runtime_version if info else None,
                    cpu_capacity=cpu_cap,
                    memory_capacity_mb=mem_mb,
                    primary_ip4=primary_ip,
                    cluster_name=cluster_name,
                    labels=labels,
                    custom_fields={
                        "k8s_node_uid": meta.uid,
                        "kubelet_version": info.kubelet_version if info else "",
                        "architecture": info.architecture if info else "",
                    },
                ))
        except ApiException as exc:
            log.error("K8s node collection failed", error=str(exc))
        return nodes

    # ── Namespaces ────────────────────────────────────────────────────────────

    def _collect_namespaces(self, cluster_name: str) -> List[CanonicalK8sNamespace]:
        v1 = client.CoreV1Api()
        namespaces = []
        ns_filter = settings.k8s_namespace_filter
        try:
            for ns in v1.list_namespace().items:
                name = ns.metadata.name
                if ns_filter and ns_filter not in name:
                    continue
                phase = (ns.status.phase or "Active").lower()
                namespaces.append(CanonicalK8sNamespace(
                    source_system=self.SOURCE,
                    source_id=ns.metadata.uid,
                    name=name,
                    cluster_name=cluster_name,
                    status="active" if phase == "active" else "offline",
                    labels=ns.metadata.labels or {},
                ))
        except ApiException as exc:
            log.error("K8s namespace collection failed", error=str(exc))
        return namespaces

    # ── Cluster object ────────────────────────────────────────────────────────

    def _collect_cluster(self, cluster_name: str) -> CanonicalCluster:
        return CanonicalCluster(
            source_system=self.SOURCE,
            source_id=cluster_name,
            name=cluster_name,
            cluster_type="openshift",
            custom_fields={"k8s_cluster_name": cluster_name},
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def collect(self) -> tuple[
        List[CanonicalCluster],
        List[CanonicalK8sNode],
        List[CanonicalK8sNamespace],
    ]:
        log.info("Starting OpenShift collection")
        self._load_config()
        cluster_name = self._cluster_name()
        cluster      = [self._collect_cluster(cluster_name)]
        nodes        = self._collect_nodes(cluster_name)
        namespaces   = self._collect_namespaces(cluster_name)
        log.info("OpenShift collection complete",
                 nodes=len(nodes), namespaces=len(namespaces))
        return cluster, nodes, namespaces
