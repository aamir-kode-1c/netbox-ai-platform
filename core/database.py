"""
core/database.py — SQLAlchemy models for agent state, inventory snapshots, change queue
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Column, String, Integer, DateTime, Text, Boolean, JSON,
    ForeignKey, Enum as SAEnum, create_engine, event, text
)
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker, Session
from sqlalchemy.pool import NullPool
import enum

from core.settings import settings


engine = create_engine(
    settings.postgres_dsn,
    poolclass=NullPool,
    echo=False,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


# ─── Enums ────────────────────────────────────────────────────────────────────

class SourceSystem(str, enum.Enum):
    HPE_OPENVIEW = "hpe_openview"
    ARIA_VROPS   = "aria_vrops"
    OPENSTACK    = "openstack"
    OPENSHIFT    = "openshift"
    HPE_3PAR     = "hpe_3par"


class InventoryObjectType(str, enum.Enum):
    DEVICE        = "device"
    VIRTUAL_MACHINE = "virtual_machine"
    CLUSTER       = "cluster"
    NETWORK       = "network"
    SUBNET        = "subnet"
    STORAGE_SYSTEM = "storage_system"
    VOLUME        = "volume"
    K8S_NODE      = "k8s_node"
    K8S_POD       = "k8s_pod"
    K8S_NAMESPACE = "k8s_namespace"
    INTERFACE     = "interface"
    IP_ADDRESS    = "ip_address"


class ChangeOperation(str, enum.Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class LifecycleStatus(str, enum.Enum):
    PLANNED         = "planned"
    STAGED          = "staged"
    ACTIVE          = "active"
    OFFLINE         = "offline"
    DECOMMISSIONING = "decommissioning"
    RETIRED         = "retired"


# ─── Models ───────────────────────────────────────────────────────────────────

class InventorySnapshot(Base):
    """Latest known state of each inventory object from source systems."""
    __tablename__ = "inventory_snapshots"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    source_system = Column(SAEnum(SourceSystem), nullable=False, index=True)
    object_type   = Column(SAEnum(InventoryObjectType), nullable=False, index=True)
    source_id     = Column(String(512), nullable=False, index=True)
    name          = Column(String(512))
    canonical_data = Column(JSON, nullable=False)          # normalised canonical JSON
    raw_data      = Column(JSON)                           # original source payload
    checksum      = Column(String(64))                     # SHA256 of canonical_data
    last_seen     = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    first_seen    = Column(DateTime, default=datetime.utcnow)
    netbox_id     = Column(Integer)                        # NetBox object ID after population
    netbox_type   = Column(String(128))                    # e.g. dcim.device
    lifecycle_status = Column(SAEnum(LifecycleStatus), default=LifecycleStatus.ACTIVE)

    changes = relationship("ChangeEvent", back_populates="snapshot")


class ChangeEvent(Base):
    """Delta queue — changes detected by Agent 5 awaiting processing."""
    __tablename__ = "change_events"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id   = Column(Integer, ForeignKey("inventory_snapshots.id"))
    source_system = Column(SAEnum(SourceSystem), nullable=False)
    object_type   = Column(SAEnum(InventoryObjectType), nullable=False)
    source_id     = Column(String(512), nullable=False)
    operation     = Column(SAEnum(ChangeOperation), nullable=False)
    diff          = Column(JSON)                           # DeepDiff result
    processed     = Column(Boolean, default=False)
    created_at    = Column(DateTime, default=datetime.utcnow)
    processed_at  = Column(DateTime)

    snapshot = relationship("InventorySnapshot", back_populates="changes")


class RelationshipEdge(Base):
    """Cross-system relationships built by Agent 2."""
    __tablename__ = "relationship_edges"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    from_source      = Column(SAEnum(SourceSystem), nullable=False)
    from_source_id   = Column(String(512), nullable=False)
    from_object_type = Column(SAEnum(InventoryObjectType), nullable=False)
    to_source        = Column(SAEnum(SourceSystem), nullable=False)
    to_source_id     = Column(String(512), nullable=False)
    to_object_type   = Column(SAEnum(InventoryObjectType), nullable=False)
    relationship_type = Column(String(128), nullable=False)  # e.g. "hosts_vm", "attached_volume"
    confidence       = Column(Integer, default=100)          # 0–100
    resolved_by      = Column(String(64))                    # "ip_match", "name_match", "llm"
    created_at       = Column(DateTime, default=datetime.utcnow)


class AgentRun(Base):
    """Audit log of every agent execution."""
    __tablename__ = "agent_runs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    agent_name  = Column(String(64), nullable=False, index=True)
    run_type    = Column(String(32))          # full, incremental
    started_at  = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    status      = Column(String(32))          # running, success, failed
    objects_processed = Column(Integer, default=0)
    objects_failed    = Column(Integer, default=0)
    error_detail = Column(Text)
    meta        = Column(JSON)


class NetBoxYAMLBatch(Base):
    """Transformed YAML batches ready for NetBox population."""
    __tablename__ = "netbox_yaml_batches"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    source_system = Column(SAEnum(SourceSystem))
    object_type = Column(SAEnum(InventoryObjectType))
    yaml_content = Column(Text, nullable=False)
    netbox_app  = Column(String(64))           # dcim, virtualization, ipam …
    netbox_model = Column(String(64))
    populated   = Column(Boolean, default=False)
    created_at  = Column(DateTime, default=datetime.utcnow)
    populated_at = Column(DateTime)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create all tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
    # Verify connectivity
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[DB] Tables initialised successfully.")
