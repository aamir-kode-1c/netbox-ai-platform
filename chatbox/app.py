"""
chatbox/app.py — Streamlit Chatbox Agent: Natural Language NetBox Query Interface.
Run with: streamlit run chatbox/app.py --server.port 8501
"""
from __future__ import annotations

import json
import re
import textwrap
from typing import Any, Dict, List, Optional, Tuple

import pynetbox
import streamlit as st
import urllib3

from core.settings import settings
from core.utils import OllamaClient

urllib3.disable_warnings()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title=settings.chatbox_title,
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── NetBox & LLM clients (cached) ─────────────────────────────────────────────
@st.cache_resource
def get_netbox():
    nb = pynetbox.api(settings.netbox_url, token=settings.netbox_token)
    if not settings.netbox_ssl_verify:
        nb.http_session.verify = False
    return nb

@st.cache_resource
def get_llm():
    return OllamaClient()

nb  = get_netbox()
llm = get_llm()

# ── NetBox API capability map ─────────────────────────────────────────────────
NETBOX_ENDPOINTS = {
    "devices":          ("dcim",            "devices"),
    "virtual_machines": ("virtualization",  "virtual_machines"),
    "clusters":         ("virtualization",  "clusters"),
    "interfaces":       ("dcim",            "interfaces"),
    "ip_addresses":     ("ipam",            "ip_addresses"),
    "prefixes":         ("ipam",            "prefixes"),
    "vlans":            ("ipam",            "vlans"),
    "sites":            ("dcim",            "sites"),
    "racks":            ("dcim",            "racks"),
    "cables":           ("dcim",            "cables"),
    "tenants":          ("tenancy",         "tenants"),
    "platforms":        ("dcim",            "platforms"),
    "device_types":     ("dcim",            "device_types"),
    "vminterfaces":     ("virtualization",  "interfaces"),
}

SYSTEM_PROMPT = textwrap.dedent(f"""
    You are a NetBox 4.4.1 inventory assistant. You help operators query their IT infrastructure.
    
    Available NetBox endpoints (app.model): {', '.join(NETBOX_ENDPOINTS.keys())}
    
    When a user asks about inventory, respond with a JSON action:
    {{
      "action": "query",
      "endpoint": "<endpoint_name_from_list>",
      "filters": {{"field": "value"}},
      "limit": 50,
      "summary": "Brief description of what you're looking for"
    }}
    
    For count queries, add "count_only": true.
    For free-text answers (greetings, explanations), use:
    {{"action": "answer", "text": "your response"}}
    
    Common filters:
    - status: active, offline, staged, decommissioning, retired
    - site: site name
    - tenant: tenant name
    - role: device role name
    - cluster: cluster name
    - name__ic: case-insensitive contains (e.g. "name__ic": "srv")
    
    Always output valid JSON only. No markdown, no explanation outside JSON.
""").strip()


# ── Intent parsing ────────────────────────────────────────────────────────────

def parse_intent(user_message: str, history: List[Dict]) -> Dict:
    """Ask LLM to convert user question into a NetBox query action."""
    context = ""
    if history:
        recent = history[-4:]
        context = "\nConversation so far:\n" + "\n".join(
            f"{m['role'].upper()}: {m['content'][:200]}" for m in recent
        )

    prompt = f"{context}\n\nUser: {user_message}"
    raw = llm.chat(prompt, system=SYSTEM_PROMPT, temperature=0.0, max_tokens=512)
    raw = re.sub(r"```json|```", "", raw).strip()

    try:
        return json.loads(raw)
    except Exception:
        # Fallback: treat as free-text
        return {"action": "answer", "text": raw}


# ── NetBox query execution ────────────────────────────────────────────────────

def execute_query(action: Dict) -> Tuple[List[Dict], str]:
    """Run a NetBox API query from the parsed action."""
    endpoint_key = action.get("endpoint", "devices")
    filters      = action.get("filters", {})
    limit        = min(int(action.get("limit", 50)), 200)
    count_only   = action.get("count_only", False)

    if endpoint_key not in NETBOX_ENDPOINTS:
        return [], f"Unknown endpoint: {endpoint_key}"

    app, model = NETBOX_ENDPOINTS[endpoint_key]
    endpoint   = getattr(getattr(nb, app), model)

    try:
        if count_only:
            count = endpoint.count(**filters)
            return [{"count": count}], f"Total count: {count}"

        results = list(endpoint.filter(**filters, limit=limit))
        items = []
        for obj in results:
            item = {"id": obj.id, "name": getattr(obj, "name", str(obj))}
            for field in ("status", "site", "tenant", "cluster", "role", "vcpus", "memory", "disk"):
                val = getattr(obj, field, None)
                if val is not None:
                    item[field] = str(val)
            items.append(item)
        return items, ""
    except Exception as exc:
        return [], f"Query error: {exc}"


# ── Response formatting ───────────────────────────────────────────────────────

def format_response(action: Dict, results: List[Dict], query_error: str, user_q: str) -> str:
    """Ask LLM to turn raw API results into a human-friendly answer."""
    if action.get("action") == "answer":
        return action.get("text", "I couldn't understand that query.")

    if query_error:
        return f"⚠️ Query failed: {query_error}"

    if not results:
        return f"No results found for: *{action.get('summary', user_q)}*"

    if len(results) == 1 and "count" in results[0]:
        return f"**{results[0]['count']}** objects found."

    # Ask LLM to summarise
    summary_prompt = (
        f"The user asked: '{user_q}'\n"
        f"NetBox returned {len(results)} results (sample of first 10):\n"
        f"{json.dumps(results[:10], indent=2, default=str)}\n\n"
        f"Write a clear, concise summary in plain English. Use a markdown table if helpful. "
        f"Do not repeat the raw JSON."
    )
    try:
        return llm.chat(summary_prompt, temperature=0.1, max_tokens=1024)
    except Exception:
        # Fallback: simple table
        if results:
            headers = list(results[0].keys())
            rows = [" | ".join(str(r.get(h, "")) for h in headers) for r in results[:20]]
            header_row = " | ".join(headers)
            sep = " | ".join(["---"] * len(headers))
            return f"**{len(results)} results:**\n\n{header_row}\n{sep}\n" + "\n".join(rows)
        return "No data returned."


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("⚙️ Configuration")
    st.markdown(f"**NetBox:** `{settings.netbox_url}`")
    st.markdown(f"**LLM Model:** `{settings.ollama_model}`")

    # Quick status check
    try:
        device_count = nb.dcim.devices.count()
        vm_count     = nb.virtualization.virtual_machines.count()
        st.success(f"✅ NetBox connected")
        st.metric("Devices", device_count)
        st.metric("VMs", vm_count)
    except Exception as exc:
        st.error(f"❌ NetBox: {exc}")

    try:
        if llm.is_available():
            st.success(f"✅ LLM ready ({settings.ollama_model})")
        else:
            st.warning("⚠️ LLM not reachable")
    except Exception:
        st.warning("⚠️ LLM check failed")

    st.divider()
    st.markdown("**Example queries:**")
    examples = [
        "List all active servers",
        "How many VMs are in the Finance tenant?",
        "Show offline devices",
        "Which clusters have VMs?",
        "List decommissioning devices",
        "What storage volumes are attached to srv-db-01?",
        "Show all OpenShift nodes",
    ]
    for ex in examples:
        if st.button(f"💬 {ex}", key=ex, use_container_width=True):
            st.session_state["prefill"] = ex

    st.divider()
    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()


# ── Main chat UI ──────────────────────────────────────────────────────────────

st.title(f"🤖 {settings.chatbox_title}")
st.caption("Ask questions about your infrastructure in plain English. Powered by local LLM + NetBox.")
st.divider()

# Session state
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Display history
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Handle prefill from sidebar buttons
prefill = st.session_state.pop("prefill", None)

# Chat input
user_input = st.chat_input("Ask about your inventory…") or prefill

if user_input:
    # Add user message
    st.session_state["messages"].append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Querying NetBox…"):
            try:
                # Keep context window bounded
                history = st.session_state["messages"][-settings.chatbox_max_history:]
                action  = parse_intent(user_input, history[:-1])

                results, query_error = [], ""
                if action.get("action") == "query":
                    results, query_error = execute_query(action)

                response = format_response(action, results, query_error, user_input)

                # Show debug details in expander
                with st.expander("🔍 Query details", expanded=False):
                    st.json(action)
                    if results:
                        st.json(results[:5])

            except Exception as exc:
                response = f"❌ Error: {exc}"

        st.markdown(response)

    st.session_state["messages"].append({"role": "assistant", "content": response})
