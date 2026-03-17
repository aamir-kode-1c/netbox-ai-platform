#!/usr/bin/env python3
"""
scripts/health_check.py — Verify all platform components are healthy.
Run anytime to check platform status.
"""
import sys
from typing import List, Tuple

import structlog

from core.settings import settings
from core.utils import setup_logging, OllamaClient, get_redis, NetBoxClient

setup_logging()
log = structlog.get_logger(__name__)


def check_ollama() -> Tuple[bool, str]:
    client = OllamaClient()
    try:
        if client.is_available():
            resp = client.chat("Say OK", temperature=0.0, max_tokens=5)
            return True, f"LLM responding: '{resp.strip()}'"
        return False, "Ollama API not reachable"
    except Exception as exc:
        return False, f"Ollama error: {exc}"


def check_postgres() -> Tuple[bool, str]:
    try:
        from core.database import engine
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "PostgreSQL connected"
    except Exception as exc:
        return False, f"PostgreSQL error: {exc}"


def check_redis() -> Tuple[bool, str]:
    try:
        r = get_redis()
        r.ping()
        info = r.info("server")
        return True, f"Redis {info.get('redis_version', '?')} connected"
    except Exception as exc:
        return False, f"Redis error: {exc}"


def check_chroma() -> Tuple[bool, str]:
    try:
        import chromadb
        client = chromadb.HttpClient(host=settings.chroma_host, port=settings.chroma_port)
        client.heartbeat()
        collections = [c.name for c in client.list_collections()]
        return True, f"ChromaDB connected. Collections: {collections or ['none']}"
    except Exception as exc:
        return False, f"ChromaDB error: {exc}"


def check_netbox() -> Tuple[bool, str]:
    try:
        import pynetbox, urllib3
        urllib3.disable_warnings()
        nb = pynetbox.api(settings.netbox_url, token=settings.netbox_token)
        if not settings.netbox_ssl_verify:
            nb.http_session.verify = False
        # Try to get device count
        count = nb.dcim.devices.count()
        return True, f"NetBox connected. Devices: {count}"
    except Exception as exc:
        return False, f"NetBox error: {exc}"


def check_db_tables() -> Tuple[bool, str]:
    try:
        from core.database import engine, AgentRun, InventorySnapshot
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        required = {"inventory_snapshots", "change_events", "relationship_edges",
                    "agent_runs", "netbox_yaml_batches"}
        missing = required - tables
        if missing:
            return False, f"Missing tables: {missing}"
        return True, f"All {len(required)} required tables present"
    except Exception as exc:
        return False, f"DB table check error: {exc}"


def check_recent_runs() -> Tuple[bool, str]:
    try:
        from core.database import SessionLocal, AgentRun
        db = SessionLocal()
        recent = (
            db.query(AgentRun)
            .order_by(AgentRun.started_at.desc())
            .limit(5)
            .all()
        )
        db.close()
        if not recent:
            return True, "No agent runs yet (first run pending)"
        summary = [f"{r.agent_name}:{r.status}" for r in recent]
        return True, f"Recent runs: {', '.join(summary)}"
    except Exception as exc:
        return False, f"Agent run check error: {exc}"


def main():
    print("\n" + "═" * 60)
    print("  NetBox AI Platform — Health Check")
    print("═" * 60 + "\n")

    checks = [
        ("PostgreSQL",      check_postgres),
        ("Redis",           check_redis),
        ("ChromaDB",        check_chroma),
        ("DB Tables",       check_db_tables),
        ("NetBox",          check_netbox),
        ("Ollama LLM",      check_ollama),
        ("Agent Runs",      check_recent_runs),
    ]

    all_ok = True
    for name, fn in checks:
        try:
            ok, msg = fn()
        except Exception as exc:
            ok, msg = False, f"Unexpected error: {exc}"

        status = "✅" if ok else "❌"
        print(f"  {status}  {name:<18} {msg}")
        if not ok:
            all_ok = False

    print("\n" + "═" * 60)
    if all_ok:
        print("  ✅  All systems healthy")
    else:
        print("  ❌  Some checks failed — review above")
    print("═" * 60 + "\n")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
