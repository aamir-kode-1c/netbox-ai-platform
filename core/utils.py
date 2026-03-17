"""
core/utils.py — Shared utilities: logging, LLM client, Redis client, checksums
"""
from __future__ import annotations

import hashlib
import json
import logging
import sys
from typing import Any, Optional

import redis
import requests
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from core.settings import settings


# ─── Structured Logging ───────────────────────────────────────────────────────

def setup_logging(level: str = settings.log_level) -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if settings.platform_env != "production"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
    )


log = structlog.get_logger()


# ─── Checksum ─────────────────────────────────────────────────────────────────

def checksum(data: Any) -> str:
    """Deterministic SHA-256 of any JSON-serialisable object."""
    serialised = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


# ─── Redis Client ─────────────────────────────────────────────────────────────

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password or None,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=5,
    )


# ─── Ollama LLM Client ────────────────────────────────────────────────────────

class OllamaClient:
    """Thin wrapper around the Ollama REST API for chat and embeddings."""

    def __init__(self) -> None:
        self.base_url = settings.ollama_base_url.rstrip("/")
        self.model    = settings.ollama_model
        self.embed_model = settings.ollama_embed_model
        self.timeout  = settings.ollama_timeout
        self.session  = requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def chat(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 4096,
    ) -> str:
        """Send a chat prompt and return the text response."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
                "num_ctx": settings.ollama_num_ctx,
            },
        }
        resp = self.session.post(
            f"{self.base_url}/api/chat",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"].strip()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def embed(self, text: str) -> list[float]:
        """Generate an embedding vector for the given text."""
        resp = self.session.post(
            f"{self.base_url}/api/embeddings",
            json={"model": self.embed_model, "prompt": text},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()["embedding"]

    def is_available(self) -> bool:
        try:
            resp = self.session.get(f"{self.base_url}/api/tags", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False


# ─── NetBox REST helper ───────────────────────────────────────────────────────

class NetBoxClient:
    """Minimal NetBox REST client wrapping pynetbox with retry logic."""

    def __init__(self) -> None:
        import pynetbox
        self.nb = pynetbox.api(
            settings.netbox_url,
            token=settings.netbox_token,
        )
        if not settings.netbox_ssl_verify:
            import urllib3
            urllib3.disable_warnings()
            self.nb.http_session.verify = False

    def get_or_create(self, app: str, model: str, lookup: dict, data: dict) -> tuple[Any, bool]:
        """
        Returns (obj, created).
        Looks up by `lookup` fields; creates with `data` if not found.
        """
        endpoint = getattr(getattr(self.nb, app), model)
        existing = endpoint.get(**lookup)
        if existing:
            return existing, False
        created = endpoint.create(**data)
        return created, True

    def update_status(self, app: str, model: str, nb_id: int, status: str) -> bool:
        endpoint = getattr(getattr(self.nb, app), model)
        obj = endpoint.get(nb_id)
        if obj:
            obj.status = status
            return obj.save()
        return False
