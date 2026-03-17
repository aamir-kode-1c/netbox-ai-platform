"""
collectors/hpe_3par.py — HPE 3PAR / Primera WSAPI collector.
Pulls storage systems, CPGs, virtual volumes, and host mappings.
"""
from __future__ import annotations

import urllib3
from typing import Any, Dict, List, Optional

import requests
import structlog

from core.canonical import CanonicalStorageSystem, CanonicalVolume
from core.settings import settings

urllib3.disable_warnings()
log = structlog.get_logger(__name__)


class HPE3PARCollector:
    SOURCE = "hpe_3par"

    def __init__(self) -> None:
        self.host    = settings.tpar_host.rstrip("/")
        self.username = settings.tpar_username
        self.password = settings.tpar_password
        self.verify  = settings.tpar_ssl_verify
        self.session = requests.Session()
        self.session.verify = self.verify
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._session_key: Optional[str] = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        resp = self.session.post(
            f"{self.host}/api/{settings.tpar_wsapi_version}/credentials",
            json={"user": self.username, "password": self.password},
            timeout=15,
        )
        resp.raise_for_status()
        self._session_key = resp.json().get("key")
        self.session.headers.update({"X-HP3PAR-WSAPI-SessionKey": self._session_key})
        log.info("3PAR WSAPI authenticated")

    def _logout(self) -> None:
        if self._session_key:
            try:
                self.session.delete(
                    f"{self.host}/api/{settings.tpar_wsapi_version}/credentials/{self._session_key}",
                    timeout=10,
                )
            except Exception:
                pass

    def _get(self, path: str, params: Dict | None = None) -> Any:
        resp = self.session.get(
            f"{self.host}/api/{settings.tpar_wsapi_version}{path}",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    # ── System info ───────────────────────────────────────────────────────────

    def _fetch_system(self) -> Dict:
        try:
            return self._get("/system")
        except Exception as exc:
            log.error("3PAR system fetch failed", error=str(exc))
            return {}

    # ── CPGs ──────────────────────────────────────────────────────────────────

    def _fetch_cpgs(self) -> List[Dict]:
        try:
            data = self._get("/cpgs")
            return data.get("members", [])
        except Exception as exc:
            log.error("3PAR CPG fetch failed", error=str(exc))
            return []

    # ── Volumes ───────────────────────────────────────────────────────────────

    def _fetch_volumes(self) -> List[Dict]:
        try:
            data = self._get("/volumes")
            return data.get("members", [])
        except Exception as exc:
            log.error("3PAR volume fetch failed", error=str(exc))
            return []

    # ── Hosts ─────────────────────────────────────────────────────────────────

    def _fetch_hosts(self) -> List[Dict]:
        try:
            data = self._get("/hosts")
            return data.get("members", [])
        except Exception as exc:
            log.error("3PAR host fetch failed", error=str(exc))
            return []

    def _fetch_vluns(self) -> List[Dict]:
        """VLUNs = volume–LUN–host mappings."""
        try:
            data = self._get("/vluns")
            return data.get("members", [])
        except Exception as exc:
            log.error("3PAR VLUN fetch failed", error=str(exc))
            return []

    # ── Mapping ───────────────────────────────────────────────────────────────

    def _map_system(self, sys_info: Dict) -> CanonicalStorageSystem:
        total_mb = sys_info.get("totalCapacityMiB", 0) or 0
        free_mb  = sys_info.get("freeCapacityMiB", 0) or 0
        return CanonicalStorageSystem(
            source_system=self.SOURCE,
            source_id=str(sys_info.get("id", "3par-system")),
            name=sys_info.get("name", "3PAR System"),
            model=sys_info.get("model"),
            serial_number=sys_info.get("serialNumber"),
            firmware=sys_info.get("systemVersion"),
            total_capacity_gb=total_mb // 1024 if total_mb else None,
            free_capacity_gb=free_mb // 1024 if free_mb else None,
            custom_fields={
                "3par_id": sys_info.get("id"),
                "3par_name": sys_info.get("name"),
                "3par_ipv4": sys_info.get("IPv4Addr"),
                "3par_nodes": sys_info.get("numberOfNodes"),
            },
        )

    def _map_volume(self, vol: Dict, host_map: Dict[str, str], sys_name: str) -> CanonicalVolume:
        size_mib = vol.get("sizeMiB", 0) or 0
        vol_id   = str(vol.get("id", vol.get("name")))
        return CanonicalVolume(
            source_system=self.SOURCE,
            source_id=vol_id,
            name=vol.get("name", vol_id),
            storage_system_id=sys_name,
            cpg=vol.get("userCPG") or vol.get("snapCPG"),
            size_gb=size_mib // 1024 if size_mib else None,
            wwn=vol.get("wwn"),
            attached_to_host=host_map.get(vol.get("name", "")),
            status="active" if vol.get("state", 1) == 1 else "offline",
            custom_fields={
                "3par_volume_id": vol_id,
                "3par_volume_type": vol.get("type"),
                "3par_cpg": vol.get("userCPG", ""),
                "3par_wwn": vol.get("wwn", ""),
                "dedup": vol.get("dedupCapable", False),
            },
        )

    # ── Public ────────────────────────────────────────────────────────────────

    def collect(self) -> tuple[List[CanonicalStorageSystem], List[CanonicalVolume]]:
        log.info("Starting HPE 3PAR collection")
        self._authenticate()
        try:
            sys_info = self._fetch_system()
            storage_system = self._map_system(sys_info)
            sys_name = storage_system.name

            # Build volume→host map via VLUNs
            vluns = self._fetch_vluns()
            host_map: dict[str, str] = {}
            for vlun in vluns:
                vol_name = vlun.get("volumeName")
                host_name = vlun.get("hostname")
                if vol_name and host_name:
                    host_map[vol_name] = host_name

            raw_volumes = self._fetch_volumes()
            volumes = []
            for vol in raw_volumes:
                try:
                    volumes.append(self._map_volume(vol, host_map, sys_name))
                except Exception as exc:
                    log.error("3PAR volume map failed", vol=vol.get("name"), error=str(exc))

            log.info("3PAR collection complete", volumes=len(volumes))
            return [storage_system], volumes
        finally:
            self._logout()
