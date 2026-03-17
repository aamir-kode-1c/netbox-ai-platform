"""
agents/agent3_transformer.py — Agent 3: NetBox 4.4.1 Format Transformer.
Uses RAG (ChromaDB + Ollama embeddings) to teach the LLM the NetBox schema,
then transforms canonical inventory objects into validated NetBox YAML.
"""
from __future__ import annotations

import json
import textwrap
from datetime import datetime
from typing import Any, Dict, List, Optional

import chromadb
import structlog
import yaml
from pydantic import BaseModel, ValidationError

from core.canonical import (
    CanonicalDevice, CanonicalVirtualMachine, CanonicalCluster,
    CanonicalNetwork, CanonicalStorageSystem, CanonicalVolume,
    CanonicalK8sNode, CanonicalK8sNamespace, InventoryBundle,
)
from core.database import SessionLocal, AgentRun, NetBoxYAMLBatch, InventoryObjectType, SourceSystem
from core.settings import settings
from core.utils import OllamaClient

log = structlog.get_logger(__name__)

# ── NetBox schema knowledge base ──────────────────────────────────────────────
NETBOX_SCHEMA_DOCS = [
    ("dcim.site", "A physical location. Fields: name(str,req), slug(str,req), status(str: active|planned|retired), description(str), latitude(float), longitude(float), tenant(nested:name), tags(list:str)."),
    ("dcim.device", "A physical device. Fields: name(str,req), device_type(nested:model,manufacturer.name,req), role(nested:name,req), site(nested:name,req), rack(nested:name), position(int), status(str: active|offline|planned|staged|decommissioning|inventory), primary_ip4(str), platform(nested:name), serial(str), asset_tag(str), tenant(nested:name), custom_fields(dict), tags(list:str), comments(str)."),
    ("dcim.manufacturer", "Hardware manufacturer. Fields: name(str,req), slug(str,req), description(str)."),
    ("dcim.device_type", "Device model/type. Fields: manufacturer(nested:name,req), model(str,req), slug(str,req), u_height(int), is_full_depth(bool), subdevice_role(str)."),
    ("dcim.device_role", "Role for a device. Fields: name(str,req), slug(str,req), color(str:hex6), vm_role(bool)."),
    ("dcim.interface", "Network interface on a device. Fields: device(nested:name,req), name(str,req), type(str: 1000base-t|10gbase-x-sfpp|virtual|other), enabled(bool), mac_address(str), mtu(int), description(str), mode(str: access|tagged|tagged-all)."),
    ("dcim.rack", "Server rack. Fields: name(str,req), site(nested:name,req), status(str: active|planned|reserved|available|deprecated), u_height(int,default:42), tenant(nested:name)."),
    ("virtualization.cluster_type", "Type of virtualisation cluster. Fields: name(str,req), slug(str,req), description(str)."),
    ("virtualization.cluster", "A virtualisation cluster (vSphere cluster, OpenStack AZ, OShift cluster). Fields: name(str,req), type(nested:name,req), group(nested:name), site(nested:name), status(str: active|planned|staging|decommissioning|offline), tenant(nested:name), custom_fields(dict), tags(list:str)."),
    ("virtualization.virtual_machine", "A virtual machine. Fields: name(str,req), cluster(nested:name,req), status(str: active|offline|staged|decommissioning), vcpus(float), memory(int:MB), disk(int:GB), primary_ip4(str), platform(nested:name), tenant(nested:name), role(nested:name), custom_fields(dict), tags(list:str)."),
    ("virtualization.vminterface", "Network interface on a VM. Fields: virtual_machine(nested:name,req), name(str,req), enabled(bool), mac_address(str), mtu(int), description(str)."),
    ("ipam.vrf", "VPN Routing and Forwarding table. Fields: name(str,req), rd(str), tenant(nested:name), description(str)."),
    ("ipam.prefix", "IP prefix/subnet. Fields: prefix(str:CIDR,req), status(str: active|container|deprecated|reserved), vrf(nested:name), tenant(nested:name), site(nested:name), vlan(nested:vid), description(str)."),
    ("ipam.ipaddress", "Individual IP address. Fields: address(str:CIDR,req), status(str: active|reserved|deprecated|dhcp|slaac), vrf(nested:name), tenant(nested:name), dns_name(str), description(str), assigned_object_type(str: dcim.interface|virtualization.vminterface), assigned_object_id(int)."),
    ("ipam.vlan", "VLAN definition. Fields: vid(int:1-4094,req), name(str,req), site(nested:name), group(nested:name), status(str: active|reserved|deprecated), tenant(nested:name)."),
    ("tenancy.tenant", "An organisation or project. Fields: name(str,req), slug(str,req), group(nested:name), description(str)."),
    ("extras.customfield", "Custom field definition. Fields: name(str,req), type(str: text|integer|boolean|date|url|json), object_types(list:str), label(str), default(any)."),
    ("extras.tag", "A tag for labelling objects. Fields: name(str,req), slug(str,req), color(str:hex6)."),
]


class NetBoxYAMLObject(BaseModel):
    """Validated NetBox YAML object before writing to DB."""
    netbox_app: str
    netbox_model: str
    fields: Dict[str, Any]


class Agent3Transformer:
    AGENT_NAME = "agent3_transformer"

    def __init__(self) -> None:
        self.llm    = OllamaClient()
        self._chroma: Optional[chromadb.ClientAPI] = None
        self._collection = None

    # ── ChromaDB / RAG Setup ──────────────────────────────────────────────────

    def _get_chroma(self) -> chromadb.ClientAPI:
        if not self._chroma:
            self._chroma = chromadb.HttpClient(
                host=settings.chroma_host,
                port=settings.chroma_port,
            )
        return self._chroma

    def build_schema_index(self) -> None:
        """Embed and store NetBox schema documentation into ChromaDB."""
        log.info("Building NetBox schema RAG index")
        client = self._get_chroma()
        try:
            client.delete_collection(settings.chroma_collection_netbox)
        except Exception:
            pass

        self._collection = client.create_collection(
            name=settings.chroma_collection_netbox,
            metadata={"hnsw:space": "cosine"},
        )

        docs, ids, embeddings = [], [], []
        for i, (model, doc) in enumerate(NETBOX_SCHEMA_DOCS):
            emb = self.llm.embed(f"{model}: {doc}")
            docs.append(doc)
            ids.append(f"schema_{i}")
            embeddings.append(emb)

        self._collection.add(documents=docs, ids=ids, embeddings=embeddings)
        log.info("Schema index built", count=len(docs))

    def _get_collection(self):
        if not self._collection:
            client = self._get_chroma()
            try:
                self._collection = client.get_collection(settings.chroma_collection_netbox)
            except Exception:
                self.build_schema_index()
        return self._collection

    def _retrieve_schema_context(self, object_description: str, n_results: int = 4) -> str:
        """Retrieve relevant NetBox schema docs for a given object."""
        try:
            collection = self._get_collection()
            emb = self.llm.embed(object_description)
            results = collection.query(
                query_embeddings=[emb],
                n_results=min(n_results, len(NETBOX_SCHEMA_DOCS)),
            )
            return "\n\n".join(results["documents"][0])
        except Exception as exc:
            log.warning("RAG retrieval failed, using full schema", error=str(exc))
            return "\n".join(doc for _, doc in NETBOX_SCHEMA_DOCS[:6])

    # ── LLM Transformation ────────────────────────────────────────────────────

    def _transform_with_llm(
        self,
        canonical_obj: Dict,
        object_hint: str,
        schema_context: str,
    ) -> Optional[Dict]:
        system_prompt = textwrap.dedent(f"""
            You are a NetBox 4.4.1 data transformation expert.
            You transform raw infrastructure inventory data into valid NetBox REST API objects.
            
            NetBox Schema Reference:
            {schema_context}
            
            Rules:
            1. Output ONLY valid JSON — no explanation, no markdown, no code blocks.
            2. Use exactly the field names from the schema reference.
            3. For nested objects use {{"name": "value"}} format.
            4. For status fields use NetBox valid values only.
            5. Omit fields with null/unknown values.
            6. Include netbox_app and netbox_model at the top level.
            
            Output format:
            {{"netbox_app": "dcim", "netbox_model": "device", "fields": {{...}}}}
        """).strip()

        user_prompt = (
            f"Transform this {object_hint} inventory object into NetBox format:\n"
            f"{json.dumps(canonical_obj, default=str, indent=2)}"
        )

        try:
            raw = self.llm.chat(user_prompt, system=system_prompt, temperature=0.05, max_tokens=2048)
            # Strip any accidental markdown
            raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            parsed = json.loads(raw)
            return parsed
        except json.JSONDecodeError as exc:
            log.warning("LLM output not valid JSON", hint=object_hint, error=str(exc))
            return None
        except Exception as exc:
            log.error("LLM transform failed", hint=object_hint, error=str(exc))
            return None

    # ── Per-type transforms ───────────────────────────────────────────────────

    def _transform_device(self, dev: CanonicalDevice) -> Optional[NetBoxYAMLObject]:
        schema_ctx = self._retrieve_schema_context(f"physical server device {dev.device_role}")
        result = self._transform_with_llm(
            dev.model_dump(mode="json"),
            f"physical device ({dev.device_role})",
            schema_ctx,
        )
        if not result:
            # Deterministic fallback
            result = {
                "netbox_app": "dcim",
                "netbox_model": "device",
                "fields": {
                    "name": dev.name,
                    "site": {"name": dev.site or "default"},
                    "device_type": {"model": dev.model or "Unknown", "manufacturer": {"name": dev.manufacturer or "Unknown"}},
                    "role": {"name": dev.device_role},
                    "status": dev.status,
                    "serial": dev.serial_number,
                    "custom_fields": dev.custom_fields,
                    "tags": [{"name": t} for t in dev.tags],
                },
            }
        return NetBoxYAMLObject(**result)

    def _transform_vm(self, vm: CanonicalVirtualMachine) -> Optional[NetBoxYAMLObject]:
        schema_ctx = self._retrieve_schema_context("virtual machine VM vcpu memory cluster")
        result = self._transform_with_llm(
            vm.model_dump(mode="json"), "virtual machine", schema_ctx
        )
        if not result:
            result = {
                "netbox_app": "virtualization",
                "netbox_model": "virtual_machine",
                "fields": {
                    "name": vm.name,
                    "cluster": {"name": vm.cluster_name or "default"},
                    "status": vm.status,
                    "vcpus": vm.vcpus,
                    "memory": vm.memory_mb,
                    "disk": vm.disk_gb,
                    "tenant": {"name": vm.tenant} if vm.tenant else None,
                    "custom_fields": vm.custom_fields,
                    "tags": [{"name": t} for t in vm.tags],
                },
            }
        return NetBoxYAMLObject(**result)

    def _transform_cluster(self, cl: CanonicalCluster) -> Optional[NetBoxYAMLObject]:
        schema_ctx = self._retrieve_schema_context("virtualisation cluster type site")
        result = self._transform_with_llm(
            cl.model_dump(mode="json"), "virtualisation cluster", schema_ctx
        )
        if not result:
            result = {
                "netbox_app": "virtualization",
                "netbox_model": "cluster",
                "fields": {
                    "name": cl.name,
                    "type": {"name": cl.cluster_type},
                    "site": {"name": cl.site} if cl.site else None,
                    "custom_fields": cl.custom_fields,
                },
            }
        return NetBoxYAMLObject(**result)

    def _transform_network(self, net: CanonicalNetwork) -> Optional[NetBoxYAMLObject]:
        schema_ctx = self._retrieve_schema_context("VLAN prefix network ipam subnet")
        result = self._transform_with_llm(
            net.model_dump(mode="json"), "network / VLAN", schema_ctx
        )
        if not result:
            fields: Dict = {"name": net.name, "status": net.status, "custom_fields": net.custom_fields}
            if net.vlan_id:
                fields["vid"] = net.vlan_id
            result = {"netbox_app": "ipam", "netbox_model": "vlan", "fields": fields}
        return NetBoxYAMLObject(**result)

    def _transform_storage(self, ss: CanonicalStorageSystem) -> Optional[NetBoxYAMLObject]:
        schema_ctx = self._retrieve_schema_context("storage system physical device rack site")
        result = self._transform_with_llm(
            ss.model_dump(mode="json"), "storage system", schema_ctx
        )
        if not result:
            result = {
                "netbox_app": "dcim",
                "netbox_model": "device",
                "fields": {
                    "name": ss.name,
                    "site": {"name": ss.site or "default"},
                    "device_type": {"model": ss.model or "3PAR", "manufacturer": {"name": "HPE"}},
                    "role": {"name": "storage"},
                    "serial": ss.serial_number,
                    "custom_fields": ss.custom_fields,
                    "tags": [{"name": "3par"}, {"name": "storage"}],
                },
            }
        return NetBoxYAMLObject(**result)

    # ── Persist to DB ─────────────────────────────────────────────────────────

    def _save_yaml_batch(
        self,
        obj: NetBoxYAMLObject,
        source_system: str,
        object_type: str,
        db,
    ) -> None:
        batch = NetBoxYAMLBatch(
            source_system=source_system,
            object_type=object_type,
            yaml_content=yaml.dump(obj.fields, default_flow_style=False, allow_unicode=True),
            netbox_app=obj.netbox_app,
            netbox_model=obj.netbox_model,
        )
        db.add(batch)

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self, bundle: InventoryBundle) -> List[NetBoxYAMLObject]:
        log.info("Agent 3 starting format transformation")
        run_record = AgentRun(agent_name=self.AGENT_NAME, run_type="transform", status="running")
        db = SessionLocal()
        db.add(run_record)
        db.commit()

        results: List[NetBoxYAMLObject] = []
        processed = 0
        failed = 0

        try:
            for dev in bundle.devices:
                try:
                    obj = self._transform_device(dev)
                    if obj:
                        results.append(obj)
                        self._save_yaml_batch(obj, dev.source_system, "device", db)
                        processed += 1
                except Exception as exc:
                    log.error("Device transform failed", name=dev.name, error=str(exc))
                    failed += 1

            for vm in bundle.virtual_machines:
                try:
                    obj = self._transform_vm(vm)
                    if obj:
                        results.append(obj)
                        self._save_yaml_batch(obj, vm.source_system, "virtual_machine", db)
                        processed += 1
                except Exception as exc:
                    log.error("VM transform failed", name=vm.name, error=str(exc))
                    failed += 1

            for cl in bundle.clusters:
                try:
                    obj = self._transform_cluster(cl)
                    if obj:
                        results.append(obj)
                        self._save_yaml_batch(obj, cl.source_system, "cluster", db)
                        processed += 1
                except Exception as exc:
                    log.error("Cluster transform failed", name=cl.name, error=str(exc))
                    failed += 1

            for net in bundle.networks:
                try:
                    obj = self._transform_network(net)
                    if obj:
                        results.append(obj)
                        self._save_yaml_batch(obj, net.source_system, "network", db)
                        processed += 1
                except Exception as exc:
                    log.error("Network transform failed", name=net.name, error=str(exc))
                    failed += 1

            for ss in bundle.storage_systems:
                try:
                    obj = self._transform_storage(ss)
                    if obj:
                        results.append(obj)
                        self._save_yaml_batch(obj, ss.source_system, "storage_system", db)
                        processed += 1
                except Exception as exc:
                    log.error("Storage transform failed", name=ss.name, error=str(exc))
                    failed += 1

            db.commit()
            run_record.status = "success"
            run_record.objects_processed = processed
            run_record.objects_failed = failed
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.info("Agent 3 complete", transformed=processed, failed=failed)
            return results

        except Exception as exc:
            db.rollback()
            run_record.status = "failed"
            run_record.error_detail = str(exc)
            run_record.finished_at = datetime.utcnow()
            db.commit()
            log.error("Agent 3 failed", error=str(exc))
            raise
        finally:
            db.close()
