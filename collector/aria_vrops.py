"""
collectors/aria_vrops.py — VMware Aria vROPS (vRealize Operations) collector.
Pulls clusters, hosts, and VMs via the vROPS REST API.
"""
from __future__ import annotations

import urllib3
from typing import Any, Dict, List, Optional

import requests
import structlog

from core.canonical import CanonicalCluster, CanonicalVirtualMachine, CanonicalInterface
from core.settings import settings

urllib3.disable_warnings()
log = structlog.get_logger(__name__)

# vROPS resource kind identifiers
KIND_CLUSTER = "ClusterComputeResource"
KIND_HOST    = "HostSystem"
KIND_VM      = "VirtualMachine"


class AriaVROPSCollector:
    SOURCE = "aria_vrops"

    def __init__(self) -> None:
        self.host    = settings.vrops_host.rstrip("/")
        self.username = settings.vrops_username
        self.password = settings.vrops_password
        self.verify  = settings.vrops_ssl_verify
        self.session = requests.Session()
        self.session.verify = self.verify
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._token: Optional[str] = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        resp = self.session.post(
            f"{self.host}/suite-api/api/auth/token/acquire",
            json={"username": self.username, "password": self.password,
                  "authSource": "LOCAL"},
            timeout=15,
        )
        resp.raise_for_status()
        self._token = resp.json().get("token")
        self.session.headers.update({"Authorization": f"vRealizeOpsToken {self._token}"})
        log.info("vROPS authenticated")

    def _release_token(self) -> None:
        if self._token:
            try:
                self.session.post(
                    f"{self.host}/suite-api/api/auth/token/release",
                    timeout=10,
                )
            except Exception:
                pass

    # ── REST helpers ──────────────────────────────────────────────────────────

    def _get(self, path: str, params: Dict | None = None) -> Any:
        resp = self.session.get(
            f"{self.host}/suite-api{path}",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_resources(self, resource_kind: str) -> List[Dict]:
        """Fetch all resources of a given resource kind."""
        resources = []
        page = 0
        page_size = 200
        while True:
            data = self._get(
                "/api/resources",
                params={
                    "resourceKind": resource_kind,
                    "pageSize": page_size,
                    "page": page,
                },
            )
            batch = data.get("resourceList", [])
            resources.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return resources

    def _get_properties(self, resource_id: str) -> Dict[str, str]:
        """Fetch all property key/value for a resource."""
        try:
            data = self._get(f"/api/resources/{resource_id}/properties")
            props = data.get("property", [])
            return {p["name"]: p.get("value", "") for p in props}
        except Exception:
            return {}

    def _get_latest_metrics(self, resource_id: str, stat_keys: List[str]) -> Dict[str, float]:
        """Get latest metric values for given stat keys."""
        try:
            resp = self.session.post(
                f"{self.host}/suite-api/api/resources/{resource_id}/stats/latestquery",
                json={"statKey": [{"key": k} for k in stat_keys]},
                timeout=20,
            )
            resp.raise_for_status()
            result = {}
            for stat in resp.json().get("values", []):
                key = stat.get("statKey", {}).get("key")
                vals = stat.get("data", [])
                if key and vals:
                    result[key] = vals[-1]
            return result
        except Exception:
            return {}

    # ── Mapping ───────────────────────────────────────────────────────────────

    def _map_cluster(self, resource: Dict) -> CanonicalCluster:
        rid = resource["identifier"]
        props = self._get_properties(rid)
        return CanonicalCluster(
            source_system=self.SOURCE,
            source_id=rid,
            name=resource.get("resourceName", rid),
            cluster_type="vmware-esxi",
            site=props.get("summary|parentDatacenter", None),
            custom_fields={
                "vrops_cluster_id": rid,
                "datacenter": props.get("summary|parentDatacenter", ""),
            },
        )

    def _map_vm(self, resource: Dict) -> CanonicalVirtualMachine:
        rid = resource["identifier"]
        props = self._get_properties(rid)
        metrics = self._get_latest_metrics(rid, [
            "cpu|demandmhz", "mem|guest_usage", "config|hardware|num_cpu",
            "config|hardware|memory_kilobytes",
        ])

        power_state = props.get("runtime|powerState", "poweredOn").lower()
        status = "active" if "on" in power_state else "offline"

        vcpus = None
        try:
            vcpus = int(float(props.get("config|hardware|numCpu", 0) or metrics.get("config|hardware|num_cpu", 0)))
        except Exception:
            pass

        memory_mb = None
        try:
            memory_mb = int(float(props.get("config|hardware|memoryKB", 0) or 0)) // 1024
        except Exception:
            pass

        ip = props.get("net|ip_addresses") or props.get("summary|guest|ipAddress")

        return CanonicalVirtualMachine(
            source_system=self.SOURCE,
            source_id=rid,
            name=resource.get("resourceName", rid),
            cluster_name=props.get("summary|parentCluster"),
            host_device_name=props.get("summary|parentHost"),
            status=status,
            vcpus=vcpus,
            memory_mb=memory_mb,
            primary_ip4=ip.split(",")[0].strip() if ip else None,
            os_type=props.get("config|guestFullName"),
            custom_fields={
                "vrops_vm_id": rid,
                "vmware_tools": props.get("guest|toolsRunningStatus", ""),
                "power_state": power_state,
            },
            tags=["vrops", "vmware"],
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def collect(self) -> tuple[List[CanonicalCluster], List[CanonicalVirtualMachine]]:
        log.info("Starting Aria vROPS collection")
        self._authenticate()
        try:
            clusters = []
            for r in self._get_resources(KIND_CLUSTER):
                try:
                    clusters.append(self._map_cluster(r))
                except Exception as exc:
                    log.error("Failed vROPS cluster map", id=r.get("identifier"), error=str(exc))

            vms = []
            for r in self._get_resources(KIND_VM):
                try:
                    vms.append(self._map_vm(r))
                except Exception as exc:
                    log.error("Failed vROPS VM map", id=r.get("identifier"), error=str(exc))

            log.info("vROPS collection complete", clusters=len(clusters), vms=len(vms))
            return clusters, vms
        finally:
            self._release_token()
