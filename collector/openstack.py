"""
collectors/openstack.py — OpenStack collector via openstacksdk.
Collects instances, tenants, networks, subnets, and volumes.
"""
from __future__ import annotations

import os
from typing import List

import structlog
import openstack
from openstack.connection import Connection

from core.canonical import (
    CanonicalCluster, CanonicalVirtualMachine, CanonicalNetwork,
    CanonicalVolume, CanonicalInterface
)
from core.settings import settings

log = structlog.get_logger(__name__)


def _build_conn() -> Connection:
    """Create an OpenStack SDK connection from environment settings."""
    # Allow OS_CACERT to override SSL verification
    if settings.os_cacert:
        os.environ["OS_CACERT"] = settings.os_cacert

    return openstack.connect(
        auth_url=settings.os_auth_url,
        project_name=settings.os_project_name,
        username=settings.os_username,
        password=settings.os_password,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
        region_name=settings.os_region_name,
        verify=bool(settings.os_cacert),
    )


class OpenStackCollector:
    SOURCE = "openstack"

    def __init__(self) -> None:
        self.conn: Connection | None = None

    def _connect(self) -> None:
        self.conn = _build_conn()
        log.info("OpenStack connected", auth_url=settings.os_auth_url)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _status_map(self, os_status: str) -> str:
        mapping = {
            "ACTIVE":    "active",
            "SHUTOFF":   "offline",
            "SUSPENDED": "offline",
            "ERROR":     "offline",
            "BUILD":     "staged",
            "DELETED":   "retired",
        }
        return mapping.get(os_status.upper(), "active")

    # ── Availability Zones → Clusters ─────────────────────────────────────────

    def _collect_clusters(self) -> List[CanonicalCluster]:
        clusters = []
        try:
            for az in self.conn.compute.availability_zones():
                clusters.append(CanonicalCluster(
                    source_system=self.SOURCE,
                    source_id=az.name,
                    name=az.name,
                    cluster_type="openstack",
                    custom_fields={"os_az_state": az.state.get("available", True)},
                ))
        except Exception as exc:
            log.error("OS cluster collection failed", error=str(exc))
        return clusters

    # ── Instances → VMs ───────────────────────────────────────────────────────

    def _collect_vms(self) -> List[CanonicalVirtualMachine]:
        vms = []
        project_map: dict[str, str] = {}
        try:
            for proj in self.conn.identity.projects():
                project_map[proj.id] = proj.name
        except Exception:
            pass

        try:
            for server in self.conn.compute.servers(all_projects=True):
                try:
                    flavor = {}
                    if server.flavor:
                        try:
                            fl = self.conn.compute.get_flavor(server.flavor["id"])
                            flavor = {"vcpus": fl.vcpus, "ram": fl.ram, "disk": fl.disk}
                        except Exception:
                            flavor = {
                                "vcpus": server.flavor.get("vcpus"),
                                "ram": server.flavor.get("ram"),
                                "disk": server.flavor.get("disk"),
                            }

                    # Collect network interfaces
                    interfaces = []
                    primary_ip4 = None
                    for net_name, addrs in (server.addresses or {}).items():
                        for addr in addrs:
                            if not primary_ip4 and addr.get("version") == 4:
                                primary_ip4 = addr["addr"]
                            interfaces.append(CanonicalInterface(
                                name=net_name,
                                ip_addresses=[addr["addr"]],
                                mac_address=addr.get("OS-EXT-IPS-MAC:mac_addr"),
                            ))

                    tenant_name = project_map.get(server.project_id, server.project_id)

                    vms.append(CanonicalVirtualMachine(
                        source_system=self.SOURCE,
                        source_id=server.id,
                        name=server.name,
                        cluster_name=server.availability_zone or "nova",
                        tenant=tenant_name,
                        status=self._status_map(server.status),
                        vcpus=flavor.get("vcpus"),
                        memory_mb=flavor.get("ram"),
                        disk_gb=flavor.get("disk"),
                        primary_ip4=primary_ip4,
                        interfaces=interfaces,
                        os_type=server.metadata.get("os_type") if server.metadata else None,
                        custom_fields={
                            "os_instance_id": server.id,
                            "os_project_id": server.project_id,
                            "os_flavor": server.flavor.get("original_name", ""),
                            "os_image_id": (server.image or {}).get("id", ""),
                            "os_host": getattr(server, "hypervisor_hostname", ""),
                        },
                        tags=["openstack"],
                    ))
                except Exception as exc:
                    log.error("OS VM map failed", server_id=server.id, error=str(exc))
        except Exception as exc:
            log.error("OS instance collection failed", error=str(exc))
        return vms

    # ── Networks ──────────────────────────────────────────────────────────────

    def _collect_networks(self) -> List[CanonicalNetwork]:
        networks = []
        try:
            subnet_map: dict[str, str] = {}
            for subnet in self.conn.network.subnets():
                subnet_map.setdefault(subnet.network_id, []).append(subnet.cidr)

            for net in self.conn.network.networks():
                networks.append(CanonicalNetwork(
                    source_system=self.SOURCE,
                    source_id=net.id,
                    name=net.name,
                    vlan_id=net.provider_segmentation_id if hasattr(net, "provider_segmentation_id") else None,
                    tenant=net.project_id,
                    status="active" if net.is_admin_state_up else "offline",
                    subnets=subnet_map.get(net.id, []),
                    custom_fields={
                        "os_network_id": net.id,
                        "os_network_type": getattr(net, "provider_network_type", ""),
                        "shared": net.is_shared,
                    },
                ))
        except Exception as exc:
            log.error("OS network collection failed", error=str(exc))
        return networks

    # ── Cinder Volumes ────────────────────────────────────────────────────────

    def _collect_volumes(self) -> List[CanonicalVolume]:
        volumes = []
        try:
            for vol in self.conn.block_storage.volumes(all_projects=True):
                attached_to = None
                if vol.attachments:
                    attached_to = vol.attachments[0].get("server_id")
                volumes.append(CanonicalVolume(
                    source_system=self.SOURCE,
                    source_id=vol.id,
                    name=vol.name or vol.id,
                    size_gb=vol.size,
                    status="active" if vol.status in ("in-use", "available") else "offline",
                    attached_to_host=attached_to,
                    tenant=vol.project_id,
                    custom_fields={
                        "os_volume_id": vol.id,
                        "os_volume_type": vol.volume_type,
                        "os_volume_status": vol.status,
                    },
                ))
        except Exception as exc:
            log.error("OS volume collection failed", error=str(exc))
        return volumes

    # ── Public ────────────────────────────────────────────────────────────────

    def collect(self) -> tuple[
        List[CanonicalCluster],
        List[CanonicalVirtualMachine],
        List[CanonicalNetwork],
        List[CanonicalVolume],
    ]:
        log.info("Starting OpenStack collection")
        self._connect()
        clusters  = self._collect_clusters()
        vms       = self._collect_vms()
        networks  = self._collect_networks()
        volumes   = self._collect_volumes()
        log.info("OpenStack collection complete",
                 clusters=len(clusters), vms=len(vms),
                 networks=len(networks), volumes=len(volumes))
        return clusters, vms, networks, volumes
