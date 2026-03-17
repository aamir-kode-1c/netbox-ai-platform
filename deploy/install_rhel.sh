#!/usr/bin/env bash
# =============================================================================
# deploy/install_rhel.sh — Full platform installation on RHEL 8/9
# Run as root or a sudo-capable user.
# =============================================================================
set -euo pipefail

PLATFORM_USER="netbox-ai"
PLATFORM_DIR="/opt/netbox-ai"
LOG_DIR="/var/log/netbox-ai"
VENV_DIR="${PLATFORM_DIR}/venv"
REPO_DIR="${PLATFORM_DIR}/app"
SERVICE_PREFIX="netbox-ai"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 1. System packages ────────────────────────────────────────────────────────
log_info "Installing system packages"
if command -v dnf &>/dev/null; then
    dnf install -y python3.11 python3.11-pip python3.11-devel \
        gcc gcc-c++ make git curl openssl-devel \
        postgresql-devel redis podman podman-compose \
        firewalld policycoreutils-python-utils 2>/dev/null || true
else
    log_warn "dnf not found — skipping system package install"
fi

# ── 2. Create platform user ───────────────────────────────────────────────────
log_info "Creating platform user: ${PLATFORM_USER}"
if ! id "${PLATFORM_USER}" &>/dev/null; then
    useradd -r -m -d "${PLATFORM_DIR}" -s /bin/bash "${PLATFORM_USER}"
fi

# ── 3. Directory structure ────────────────────────────────────────────────────
log_info "Creating directories"
mkdir -p "${PLATFORM_DIR}"/{app,venv,data,chroma,config,logs}
mkdir -p "${LOG_DIR}"
chown -R "${PLATFORM_USER}:${PLATFORM_USER}" "${PLATFORM_DIR}" "${LOG_DIR}"

# ── 4. Copy application code ──────────────────────────────────────────────────
log_info "Copying application to ${REPO_DIR}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_SRC="$(dirname "${SCRIPT_DIR}")"

if [[ -d "${APP_SRC}/agents" ]]; then
    cp -r "${APP_SRC}/." "${REPO_DIR}/"
    chown -R "${PLATFORM_USER}:${PLATFORM_USER}" "${REPO_DIR}"
else
    log_warn "Source not found at ${APP_SRC} — skipping copy"
fi

# ── 5. Python virtual environment ─────────────────────────────────────────────
log_info "Creating Python virtual environment"
PYTHON_BIN=$(command -v python3.11 || command -v python3 || echo "python3")
sudo -u "${PLATFORM_USER}" "${PYTHON_BIN}" -m venv "${VENV_DIR}"
sudo -u "${PLATFORM_USER}" "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel

log_info "Installing Python requirements"
if [[ -f "${REPO_DIR}/requirements.txt" ]]; then
    sudo -u "${PLATFORM_USER}" "${VENV_DIR}/bin/pip" install -r "${REPO_DIR}/requirements.txt"
fi

# ── 6. Environment file ───────────────────────────────────────────────────────
log_info "Setting up environment file"
ENV_FILE="${REPO_DIR}/config/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
    cp "${REPO_DIR}/config/.env.template" "${ENV_FILE}"
    chmod 600 "${ENV_FILE}"
    chown "${PLATFORM_USER}:${PLATFORM_USER}" "${ENV_FILE}"
    log_warn "Created ${ENV_FILE} — fill in credentials before starting services"
else
    log_info "Environment file already exists, skipping"
fi

# ── 7. Ollama installation ────────────────────────────────────────────────────
log_info "Installing Ollama"
if ! command -v ollama &>/dev/null; then
    if [[ -f "${PLATFORM_DIR}/ollama-linux-amd64" ]]; then
        # Air-gapped install from pre-downloaded binary
        install -o root -g root -m 755 "${PLATFORM_DIR}/ollama-linux-amd64" /usr/local/bin/ollama
        log_info "Ollama installed from local binary"
    else
        log_warn "Ollama binary not found at ${PLATFORM_DIR}/ollama-linux-amd64"
        log_warn "For air-gapped install: download from https://ollama.ai/download/ollama-linux-amd64"
        log_warn "Place at ${PLATFORM_DIR}/ollama-linux-amd64 and re-run this script"
    fi
else
    log_info "Ollama already installed: $(ollama --version 2>/dev/null || echo 'unknown version')"
fi

# Create ollama user and service
if ! id ollama &>/dev/null; then
    useradd -r -s /bin/false -d /var/lib/ollama ollama
    mkdir -p /var/lib/ollama
    chown ollama:ollama /var/lib/ollama
fi

cat > /etc/systemd/system/ollama.service << 'EOF'
[Unit]
Description=Ollama LLM Service
After=network-online.target
Wants=network-online.target

[Service]
User=ollama
Group=ollama
ExecStart=/usr/local/bin/ollama serve
Environment=OLLAMA_HOST=127.0.0.1:11434
Environment=OLLAMA_MODELS=/var/lib/ollama/models
Restart=always
RestartSec=3
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
EOF

# ── 8. PostgreSQL via Podman ──────────────────────────────────────────────────
log_info "Setting up PostgreSQL container"
cat > /etc/systemd/system/netbox-ai-postgres.service << 'EOF'
[Unit]
Description=NetBox AI PostgreSQL
After=network.target

[Service]
User=netbox-ai
Restart=always
ExecStartPre=-/usr/bin/podman stop netbox-ai-postgres
ExecStartPre=-/usr/bin/podman rm netbox-ai-postgres
ExecStart=/usr/bin/podman run \
  --name netbox-ai-postgres \
  --env-file /opt/netbox-ai/app/config/.env \
  -e POSTGRES_DB=netbox_ai \
  -e POSTGRES_USER=netbox_ai \
  -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
  -p 127.0.0.1:5432:5432 \
  -v /opt/netbox-ai/data/postgres:/var/lib/postgresql/data:Z \
  postgres:15-alpine
ExecStop=/usr/bin/podman stop netbox-ai-postgres

[Install]
WantedBy=multi-user.target
EOF

# ── 9. Redis via Podman ───────────────────────────────────────────────────────
log_info "Setting up Redis container"
cat > /etc/systemd/system/netbox-ai-redis.service << 'EOF'
[Unit]
Description=NetBox AI Redis
After=network.target

[Service]
User=netbox-ai
Restart=always
ExecStartPre=-/usr/bin/podman stop netbox-ai-redis
ExecStartPre=-/usr/bin/podman rm netbox-ai-redis
ExecStart=/usr/bin/podman run \
  --name netbox-ai-redis \
  -p 127.0.0.1:6379:6379 \
  -v /opt/netbox-ai/data/redis:/data:Z \
  redis:7-alpine redis-server --requirepass ${REDIS_PASSWORD}
ExecStop=/usr/bin/podman stop netbox-ai-redis

[Install]
WantedBy=multi-user.target
EOF

# ── 10. ChromaDB via Podman ───────────────────────────────────────────────────
log_info "Setting up ChromaDB container"
cat > /etc/systemd/system/netbox-ai-chroma.service << 'EOF'
[Unit]
Description=NetBox AI ChromaDB
After=network.target

[Service]
User=netbox-ai
Restart=always
ExecStartPre=-/usr/bin/podman stop netbox-ai-chroma
ExecStartPre=-/usr/bin/podman rm netbox-ai-chroma
ExecStart=/usr/bin/podman run \
  --name netbox-ai-chroma \
  -p 127.0.0.1:8080:8000 \
  -v /opt/netbox-ai/chroma:/chroma/chroma:Z \
  chromadb/chroma:latest
ExecStop=/usr/bin/podman stop netbox-ai-chroma

[Install]
WantedBy=multi-user.target
EOF

# ── 11. Agent Scheduler service ───────────────────────────────────────────────
log_info "Creating agent scheduler service"
cat > /etc/systemd/system/netbox-ai-scheduler.service << EOF
[Unit]
Description=NetBox AI Agent Scheduler
After=network-online.target ollama.service netbox-ai-postgres.service netbox-ai-redis.service
Requires=netbox-ai-postgres.service netbox-ai-redis.service

[Service]
User=${PLATFORM_USER}
WorkingDirectory=${REPO_DIR}
ExecStart=${VENV_DIR}/bin/python scripts/scheduler.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=netbox-ai-scheduler

[Install]
WantedBy=multi-user.target
EOF

# ── 12. Chatbox service ───────────────────────────────────────────────────────
log_info "Creating chatbox service"
cat > /etc/systemd/system/netbox-ai-chatbox.service << EOF
[Unit]
Description=NetBox AI Chatbox UI
After=network-online.target ollama.service

[Service]
User=${PLATFORM_USER}
WorkingDirectory=${REPO_DIR}
ExecStart=${VENV_DIR}/bin/streamlit run chatbox/app.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=netbox-ai-chatbox

[Install]
WantedBy=multi-user.target
EOF

# ── 13. Reload systemd & enable services ──────────────────────────────────────
log_info "Enabling systemd services"
systemctl daemon-reload
systemctl enable ollama netbox-ai-postgres netbox-ai-redis netbox-ai-chroma \
    netbox-ai-scheduler netbox-ai-chatbox 2>/dev/null || true

# ── 14. Firewall rules ────────────────────────────────────────────────────────
log_info "Configuring firewall (internal access only)"
if command -v firewall-cmd &>/dev/null; then
    firewall-cmd --zone=internal --add-port=8501/tcp --permanent 2>/dev/null || true
    firewall-cmd --reload 2>/dev/null || true
fi

# ── 15. SELinux ───────────────────────────────────────────────────────────────
log_info "Applying SELinux contexts"
if command -v restorecon &>/dev/null; then
    restorecon -Rv "${PLATFORM_DIR}" 2>/dev/null || true
fi

# ── Done ──────────────────────────────────────────────────────────────────────
log_info ""
log_info "══════════════════════════════════════════════════════════"
log_info "  Installation complete!"
log_info "══════════════════════════════════════════════════════════"
log_info ""
log_info "Next steps:"
log_info "  1. Edit ${ENV_FILE} with your credentials"
log_info "  2. If air-gapped: copy Ollama model tar and run:"
log_info "     ollama load < /path/to/llama31.tar"
log_info "  3. Start services:"
log_info "     systemctl start ollama netbox-ai-postgres netbox-ai-redis netbox-ai-chroma"
log_info "  4. Run initial setup:"
log_info "     sudo -u ${PLATFORM_USER} ${VENV_DIR}/bin/python ${REPO_DIR}/scripts/setup_ollama.py"
log_info "  5. Start agents:"
log_info "     systemctl start netbox-ai-scheduler netbox-ai-chatbox"
log_info ""
log_info "  Chatbox UI: http://$(hostname -f):8501"
log_info ""
