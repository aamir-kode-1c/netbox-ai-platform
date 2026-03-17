"""
core/settings.py — Centralised configuration loaded from environment / .env
"""
import os
from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings

_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _ROOT / "config" / ".env"


class Settings(BaseSettings):
    # LLM
    ollama_base_url: str = "http://127.0.0.1:11434"
    ollama_model: str = "llama3.1:8b"
    ollama_embed_model: str = "nomic-embed-text"
    ollama_timeout: int = 120
    ollama_num_ctx: int = 8192

    # NetBox
    netbox_url: str = "http://netbox.internal:8000"
    netbox_token: str = ""
    netbox_ssl_verify: bool = False
    netbox_timeout: int = 30

    # PostgreSQL
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_db: str = "netbox_ai"
    postgres_user: str = "netbox_ai"
    postgres_password: str = ""

    # Redis
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0

    # ChromaDB
    chroma_host: str = "127.0.0.1"
    chroma_port: int = 8080
    chroma_collection_netbox: str = "netbox_schema"
    chroma_persist_dir: str = "/opt/netbox-ai/chroma"

    # Source Systems
    ov_base_url: str = "https://openview.internal:8443"
    ov_username: str = "admin"
    ov_password: str = ""
    ov_ssl_verify: bool = False
    ov_api_version: str = "v1"

    vrops_host: str = "https://vrops.internal"
    vrops_username: str = "admin"
    vrops_password: str = ""
    vrops_ssl_verify: bool = False

    os_auth_url: str = "https://keystone.internal:5000/v3"
    os_username: str = "admin"
    os_password: str = ""
    os_project_name: str = "admin"
    os_user_domain_name: str = "Default"
    os_project_domain_name: str = "Default"
    os_region_name: str = "RegionOne"
    os_cacert: str = ""

    kubeconfig: str = "/opt/netbox-ai/config/kubeconfig"
    k8s_namespace_filter: str = ""
    k8s_verify_ssl: bool = False

    tpar_host: str = "https://3par.internal:8080"
    tpar_username: str = "3paradm"
    tpar_password: str = ""
    tpar_ssl_verify: bool = False
    tpar_wsapi_version: str = "1.7"

    # Scheduling
    agent1_full_interval_min: int = 30
    agent1_incremental_interval_min: int = 5
    agent5_poll_interval_min: int = 5
    agent6_scan_interval_min: int = 60

    # Lifecycle
    lifecycle_vm_grace_days: int = 7
    lifecycle_server_grace_days: int = 30
    lifecycle_pod_grace_days: int = 1

    # Chatbox
    chatbox_port: int = 8501
    chatbox_title: str = "NetBox AI Assistant"
    chatbox_max_history: int = 10

    # Platform
    log_level: str = "INFO"
    log_dir: str = "/var/log/netbox-ai"
    data_dir: str = "/opt/netbox-ai/data"
    platform_env: str = "production"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    model_config = {"env_file": str(_ENV_FILE), "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
