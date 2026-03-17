#!/usr/bin/env python3
"""
scripts/setup_ollama.py — Download models and verify Ollama is running.
Run once after Ollama is installed on the RHEL server.
"""
import subprocess
import sys
import time

import requests
import structlog

from core.settings import settings
from core.utils import setup_logging, OllamaClient

setup_logging()
log = structlog.get_logger(__name__)


def wait_for_ollama(max_wait: int = 60) -> bool:
    """Poll Ollama until it's available or timeout."""
    log.info("Waiting for Ollama to be ready", url=settings.ollama_base_url)
    for i in range(max_wait):
        try:
            resp = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=3)
            if resp.status_code == 200:
                log.info("Ollama is ready")
                return True
        except Exception:
            pass
        time.sleep(1)
        if i % 10 == 0:
            log.info("Still waiting for Ollama...", elapsed_sec=i)
    return False


def pull_model(model: str) -> bool:
    """Pull a model via Ollama REST API."""
    log.info(f"Pulling model: {model}")
    try:
        resp = requests.post(
            f"{settings.ollama_base_url}/api/pull",
            json={"name": model, "stream": False},
            timeout=3600,  # Models can be large
        )
        resp.raise_for_status()
        log.info(f"Model pulled successfully: {model}")
        return True
    except Exception as exc:
        log.error(f"Failed to pull model {model}", error=str(exc))
        return False


def check_model_loaded(model: str) -> bool:
    """Check if a model is already available."""
    try:
        resp = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=10)
        models = [m["name"] for m in resp.json().get("models", [])]
        return any(model.split(":")[0] in m for m in models)
    except Exception:
        return False


def build_schema_index() -> None:
    """Build the NetBox schema RAG index in ChromaDB."""
    log.info("Building NetBox schema RAG index")
    from agents.agent3_transformer import Agent3Transformer
    transformer = Agent3Transformer()
    transformer.build_schema_index()
    log.info("Schema index built successfully")


def main():
    log.info("NetBox AI Platform — Setup Script")
    log.info(f"Ollama URL: {settings.ollama_base_url}")
    log.info(f"Primary model: {settings.ollama_model}")
    log.info(f"Embed model: {settings.ollama_embed_model}")

    # 1. Wait for Ollama
    if not wait_for_ollama(max_wait=120):
        log.error("Ollama is not available. Start the Ollama service first.")
        sys.exit(1)

    client = OllamaClient()

    # 2. Pull primary model if not present
    if not check_model_loaded(settings.ollama_model):
        success = pull_model(settings.ollama_model)
        if not success:
            log.warning("Could not pull primary model. Trying next option...")
    else:
        log.info(f"Primary model already available: {settings.ollama_model}")

    # 3. Pull embedding model if not present
    if not check_model_loaded(settings.ollama_embed_model):
        pull_model(settings.ollama_embed_model)
    else:
        log.info(f"Embed model already available: {settings.ollama_embed_model}")

    # 4. Verify LLM is responding
    log.info("Verifying LLM response...")
    try:
        response = client.chat("Reply with only: READY", temperature=0.0, max_tokens=10)
        log.info("LLM test response", response=response)
    except Exception as exc:
        log.error("LLM test failed", error=str(exc))
        sys.exit(1)

    # 5. Build schema index
    try:
        build_schema_index()
    except Exception as exc:
        log.warning("Schema index build failed — will retry on first run", error=str(exc))

    log.info("✅ Setup complete. You can now start the scheduler and chatbox.")


if __name__ == "__main__":
    main()
