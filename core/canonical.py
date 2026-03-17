"""
core/canonical.py — Canonical inventory schema (Pydantic) that all collectors output.
Every source system maps its raw data into these models before storage.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CanonicalInterface(BaseModel):
    name: str
    mac_address: Optional[str] = None
    ip_addresses: List[str] = Field(default_factory=list)   # CIDR notation
    speed_mbps: Optional[int] = None
    mtu: Optional[int] = None
    enabled: bool = True
    description: Optional[str] = None
    vlan_ids: List[int] = Field(default_factory=list)


class CanonicalDevice(BaseModel):
    """Physical server / network device / appliance."""
    source_system: str
    source_id: str
    name: str
    fqdn: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    asset_tag: Optional[str] = None
    site: Optional[str] = None
    rack: Optional[str] = None
    rack_unit: Optional[int] = None
    device_role: str = "server"
    platform: Optional[str] = None        # OS name
    os_version: Optional[str] = None
    status: str = "active"                # active | offline | planned
    primary_ip4: Optional[str] = None
    primary_ip6: Optional[str] = None
    interfaces: List[CanonicalInterface] = Field(default_factory=list)
    cpu_count: Optional[int] = None
    ram_gb: Optional[int] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    last_updated: Optional[datetime] = None


class CanonicalVirtualMachine(BaseModel):
    """VM from vROPS or OpenStack."""
    source_system: str
    source_id: str
    name: str
    cluster_name: Optional[str] = None
    host_device_name: Optional[str] = None   # physical host
    tenant: Optional[str] = None
    status: str = "active"
    vcpus: Optional[int] = None
    memory_mb: Optional[int] = None
    disk_gb: Optional[int] = None
    primary_ip4: Optional[str] = None
    primary_ip6: Optional[str] = None
    interfaces: List[CanonicalInterface] = Field(default_factory=list)
    os_type: Optional[str] = None
    platform: Optional[str] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    last_updated: Optional[datetime] = None


class CanonicalCluster(BaseModel):
    """vROPS cluster / OpenStack availability zone."""
    source_system: str
    source_id: str
    name: str
    cluster_type: str = "vmware-esxi"   # vmware-esxi | openstack | openshift
    site: Optional[str] = None
    tenant: Optional[str] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)


class CanonicalNetwork(BaseModel):
    """OpenStack Neutron network / VLAN."""
    source_system: str
    source_id: str
    name: str
    vlan_id: Optional[int] = None
    tenant: Optional[str] = None
    status: str = "active"
    subnets: List[str] = Field(default_factory=list)   # CIDR strings
    custom_fields: Dict[str, Any] = Field(default_factory=dict)


class CanonicalStorageSystem(BaseModel):
    """HPE 3PAR / Primera system."""
    source_system: str
    source_id: str
    name: str
    model: Optional[str] = None
    serial_number: Optional[str] = None
    firmware: Optional[str] = None
    site: Optional[str] = None
    total_capacity_gb: Optional[int] = None
    free_capacity_gb: Optional[int] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)


class CanonicalVolume(BaseModel):
    """3PAR virtual volume / OpenStack Cinder volume."""
    source_system: str
    source_id: str
    name: str
    storage_system_id: Optional[str] = None
    cpg: Optional[str] = None               # 3PAR CPG
    size_gb: Optional[int] = None
    wwn: Optional[str] = None
    attached_to_host: Optional[str] = None  # device source_id
    tenant: Optional[str] = None
    status: str = "active"
    custom_fields: Dict[str, Any] = Field(default_factory=dict)


class CanonicalK8sNode(BaseModel):
    """OpenShift / Kubernetes node."""
    source_system: str
    source_id: str
    name: str
    role: str = "worker"                    # master | worker | infra
    status: str = "active"
    os_image: Optional[str] = None
    kernel_version: Optional[str] = None
    container_runtime: Optional[str] = None
    cpu_capacity: Optional[int] = None
    memory_capacity_mb: Optional[int] = None
    primary_ip4: Optional[str] = None
    cluster_name: Optional[str] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    labels: Dict[str, str] = Field(default_factory=dict)


class CanonicalK8sNamespace(BaseModel):
    """OpenShift project / K8s namespace."""
    source_system: str
    source_id: str
    name: str
    cluster_name: Optional[str] = None
    status: str = "active"
    labels: Dict[str, str] = Field(default_factory=dict)
    tenant: Optional[str] = None


class InventoryBundle(BaseModel):
    """Full inventory bundle returned by Agent 1 after a collection run."""
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    devices:          List[CanonicalDevice]         = Field(default_factory=list)
    virtual_machines: List[CanonicalVirtualMachine] = Field(default_factory=list)
    clusters:         List[CanonicalCluster]        = Field(default_factory=list)
    networks:         List[CanonicalNetwork]        = Field(default_factory=list)
    storage_systems:  List[CanonicalStorageSystem]  = Field(default_factory=list)
    volumes:          List[CanonicalVolume]         = Field(default_factory=list)
    k8s_nodes:        List[CanonicalK8sNode]        = Field(default_factory=list)
    k8s_namespaces:   List[CanonicalK8sNamespace]   = Field(default_factory=list)

    def total(self) -> int:
        return sum([
            len(self.devices), len(self.virtual_machines), len(self.clusters),
            len(self.networks), len(self.storage_systems), len(self.volumes),
            len(self.k8s_nodes), len(self.k8s_namespaces),
        ])
