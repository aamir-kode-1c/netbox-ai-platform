# NetBox AI Platform — Multi-Agent Inventory Management

A locally-hosted, air-gapped AI platform that automates IT infrastructure inventory
discovery, relationship mapping, NetBox 4.4.1 population, and lifecycle management.

## Architecture

```
Source Systems                    Agent Layer                     NetBox 4.4.1
──────────────          ───────────────────────────────         ──────────────
HPE OpenView    ─┐
Aria vROPS      ─┤──► Agent 1: Collector ──► Agent 2: Mapper
OpenStack       ─┤      │                                         NetBox REST
OpenShift       ─┤      ▼                                              │
HPE 3PAR        ─┘  Agent 3: Transformer (LLM+RAG)                   │
                         │                                            │
                         ▼                                            │
                    Agent 4: Populator ─────────────────────────────►│
                         │
                         ├──► Agent 5: Change Watcher (looping)
                         └──► Agent 6: Lifecycle Manager (scheduled)

                    Chatbox Agent ────────────────────────────────────►│ (read-only)
                    (Streamlit UI)
```

## Quick Start

1. Install prerequisites:
```bash
sudo bash deploy/install_rhel.sh
```

2. Configure credentials:
```bash
cp config/.env.template config/.env
vi config/.env    # Fill in all credentials
```

3. Start infrastructure services:
```bash
systemctl start netbox-ai-postgres netbox-ai-redis netbox-ai-chroma ollama
```

4. (Air-gapped) Load Ollama model:
```bash
ollama load < /path/to/llama31-8b.tar
```

5. Initialise database:
```bash
python scripts/init_db.py
```

6. Setup LLM and RAG index:
```bash
python scripts/setup_ollama.py
```

7. Check health:
```bash
python scripts/health_check.py
```

8. Run first pipeline:
```bash
python scripts/run_pipeline.py --agent full
```

9. Start services:
```bash
systemctl start netbox-ai-scheduler netbox-ai-chatbox
```

10. Access Chatbox UI: `http://your-server:8501`

## Running Individual Agents

```bash
# Full pipeline
python scripts/run_pipeline.py

# Single agents
python scripts/run_pipeline.py --agent collect
python scripts/run_pipeline.py --agent relate
python scripts/run_pipeline.py --agent transform
python scripts/run_pipeline.py --agent populate
python scripts/run_pipeline.py --agent watch
python scripts/run_pipeline.py --agent lifecycle

# Rebuild NetBox schema RAG index
python scripts/run_pipeline.py --agent index
```

## Service Status

```bash
systemctl status netbox-ai-scheduler
systemctl status netbox-ai-chatbox
journalctl -u netbox-ai-scheduler -f
journalctl -u netbox-ai-chatbox -f
```

## Project Structure

```
netbox-ai-platform/
├── agents/
│   ├── agent1_collector.py      # Multi-source inventory collector
│   ├── agent2_relationship.py   # Cross-system relationship mapper
│   ├── agent3_transformer.py    # LLM-powered NetBox format transformer
│   ├── agent4_populator.py      # NetBox hierarchical populator
│   ├── agent5_change_watcher.py # Change detection & incremental sync
│   ├── agent6_lifecycle.py      # Lifecycle state machine manager
│   └── orchestrator.py          # LangGraph pipeline orchestrator
├── collectors/
│   ├── hpe_openview.py          # HPE OpenView REST/SOAP collector
│   ├── aria_vrops.py            # VMware Aria vROPS collector
│   ├── openstack.py             # OpenStack SDK collector
│   ├── openshift.py             # Kubernetes/OpenShift collector
│   └── hpe_3par.py              # HPE 3PAR WSAPI collector
├── chatbox/
│   └── app.py                   # Streamlit NL query chatbox
├── core/
│   ├── canonical.py             # Pydantic canonical inventory schema
│   ├── database.py              # SQLAlchemy models & session management
│   ├── settings.py              # Centralised configuration
│   └── utils.py                 # LLM client, Redis client, logging
├── scripts/
│   ├── scheduler.py             # APScheduler long-running process
│   ├── init_db.py               # Database initialisation
│   ├── setup_ollama.py          # Ollama model setup + RAG index build
│   ├── health_check.py          # Platform health verification
│   └── run_pipeline.py          # Manual pipeline trigger
├── deploy/
│   ├── install_rhel.sh          # RHEL automated installer
│   └── podman-compose.yml       # Infrastructure services compose file
├── config/
│   └── .env.template            # Environment configuration template
└── requirements.txt             # Python dependencies
```
