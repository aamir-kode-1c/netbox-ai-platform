"""
collectors/hpe_openview.py — HPE OpenView / Operations Manager collector.
Pulls nodes, topology, and network inventory via REST API.
Falls back to SOAP (zeep) if REST is unavailable.
"""
from __future__ import annotations

import urllib3
from typing import Any, Dict, List

import requests
import structlog

from core.canonical import CanonicalDevice, CanonicalInterface
from core.settings import settings

urllib3.disable_warnings()
log = structlog.get_logger(__name__)


class HPEOpenViewCollector:
    """Collects inventory from HPE OpenView / Operations Manager."""

    SOURCE = "hpe_openview"

    def __init__(self) -> None:
        self.base_url = settings.ov_base_url.rstrip("/")
        self.username = settings.ov_username
        self.password = settings.ov_password
        self.verify   = settings.ov_ssl_verify
        self.session  = requests.Session()
        self.session.verify = self.verify
        self._token: str | None = None

    # ── Authentication ────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        """Obtain a session token from OpenView."""
        url = f"{self.base_url}/api/{settings.ov_api_version}/auth/token"
        try:
            resp = self.session.post(
                url,
                json={"login": self.username, "password": self.password},
                timeout=15,
            )
            resp.raise_for_status()
            self._token = resp.json().get("token") or resp.headers.get("X-Auth-Token")
            self.session.headers.update({"X-Auth-Token": self._token})
            log.info("OpenView authenticated")
        except Exception as exc:
            # Fallback: Basic auth header
            log.warning("OpenView token auth failed, using Basic auth", error=str(exc))
            self.session.auth = (self.username, self.password)

    def _get(self, path: str, params: Dict | None = None) -> Any:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ── Data Collection ───────────────────────────────────────────────────────

    def _fetch_nodes(self) -> List[Dict]:
        """Fetch all managed nodes."""
        try:
            data = self._get(f"/api/{settings.ov_api_version}/nodes")
            return data.get("nodes", data) if isinstance(data, dict) else data
        except Exception as exc:
            log.error("Failed to fetch OV nodes", error=str(exc))
            return []

    def _fetch_node_details(self, node_id: str) -> Dict:
        try:
            return self._get(f"/api/{settings.ov_api_version}/nodes/{node_id}")
        except Exception:
            return {}

    def _fetch_interfaces(self, node_id: str) -> List[Dict]:
        try:
            data = self._get(f"/api/{settings.ov_api_version}/nodes/{node_id}/interfaces")
            return data.get("interfaces", data) if isinstance(data, dict) else data
        except Exception:
            return []

    def _map_node_to_device(self, node: Dict) -> CanonicalDevice:
        node_id = str(node.get("id") or node.get("nodeId") or node.get("name"))
        details = self._fetch_node_details(node_id)

        raw_ifaces = self._fetch_interfaces(node_id)
        interfaces = []
        for iface in raw_ifaces:
            interfaces.append(CanonicalInterface(
                name=iface.get("name", "eth0"),
                mac_address=iface.get("macAddress") or iface.get("mac"),
                ip_addresses=[iface["ipAddress"]] if iface.get("ipAddress") else [],
                speed_mbps=iface.get("speed"),
                enabled=iface.get("status", "up") == "up",
            ))

        # Determine status from OV management state
        ov_status = node.get("managementState", node.get("status", "MANAGED")).upper()
        nb_status = "active" if ov_status in ("MANAGED", "ACTIVE", "UP") else "offline"

        return CanonicalDevice(
            source_system=self.SOURCE,
            source_id=node_id,
            name=node.get("name") or node.get("hostname", node_id),
            fqdn=node.get("fqdn") or details.get("fqdn"),
            manufacturer=details.get("manufacturer", "HPE"),
            model=details.get("model") or details.get("productName"),
            serial_number=details.get("serialNumber"),
            site=details.get("location") or details.get("site"),
            device_role=self._infer_role(node, details),
            platform=details.get("osType") or details.get("operatingSystem"),
            os_version=details.get("osVersion"),
            status=nb_status,
            primary_ip4=node.get("primaryIPAddress") or node.get("ipAddress"),
            cpu_count=details.get("cpuCount"),
            ram_gb=int(details.get("ramMb", 0) or 0) // 1024 or None,
            interfaces=interfaces,
            custom_fields={
                "openview_node_id": node_id,
                "ov_management_state": ov_status,
                "ov_node_type": node.get("type", ""),
            },
            tags=["openview"],
        )

    @staticmethod
    def _infer_role(node: Dict, details: Dict) -> str:
        node_type = (node.get("type") or details.get("type") or "").lower()
        if "switch" in node_type or "router" in node_type:
            return "network-device"
        if "storage" in node_type:
            return "storage"
        return "server"

    # ── Public API ────────────────────────────────────────────────────────────

    def collect(self) -> List[CanonicalDevice]:
        """Main entry point — returns list of canonical devices."""
        log.info("Starting HPE OpenView collection")
        self._authenticate()
        nodes = self._fetch_nodes()
        devices = []
        for node in nodes:
            try:
                devices.append(self._map_node_to_device(node))
            except Exception as exc:
                log.error("Failed to map OV node", node=str(node.get("name")), error=str(exc))
        log.info("HPE OpenView collection complete", count=len(devices))
        return devices
