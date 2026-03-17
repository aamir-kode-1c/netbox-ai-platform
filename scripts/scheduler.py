"""
scripts/scheduler.py — APScheduler-based agent runner.
Runs as a long-lived process; triggers each agent on its configured schedule.
Start with: python scripts/scheduler.py
"""
from __future__ import annotations

import signal
import sys
import time

import structlog
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from agents.agent1_collector import Agent1Collector
from agents.agent5_change_watcher import Agent5ChangeWatcher
from agents.agent6_lifecycle import Agent6LifecycleManager
from agents.orchestrator import run_full_pipeline
from core.database import init_db
from core.settings import settings
from core.utils import setup_logging

setup_logging()
log = structlog.get_logger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")


# ── Scheduled jobs ────────────────────────────────────────────────────────────

def job_full_pipeline():
    """Full inventory collection + relationship + transform + populate."""
    log.info("Scheduled: full pipeline starting")
    try:
        result = run_full_pipeline(run_type="full")
        log.info("Scheduled: full pipeline complete", **result)
    except Exception as exc:
        log.error("Scheduled: full pipeline failed", error=str(exc))


def job_incremental_collect():
    """Lightweight incremental collection only (no LLM transform)."""
    log.info("Scheduled: incremental collect")
    try:
        agent = Agent1Collector()
        bundle = agent.run(run_type="incremental")
        log.info("Scheduled: incremental collect complete", total=bundle.total())
    except Exception as exc:
        log.error("Scheduled: incremental collect failed", error=str(exc))


def job_change_watch():
    """Change detection poll cycle."""
    try:
        watcher = Agent5ChangeWatcher()
        changes = watcher.run()
        if changes > 0:
            log.info("Change watch: changes detected", count=changes)
    except Exception as exc:
        log.error("Change watch job failed", error=str(exc))


def job_lifecycle():
    """Lifecycle management scan."""
    log.info("Scheduled: lifecycle scan")
    try:
        lm = Agent6LifecycleManager()
        result = lm.run()
        log.info("Scheduled: lifecycle scan complete", **result)
    except Exception as exc:
        log.error("Scheduled: lifecycle scan failed", error=str(exc))


# ── Register schedules ────────────────────────────────────────────────────────

def configure_scheduler() -> None:
    # Full pipeline — every N minutes (default 30)
    scheduler.add_job(
        job_full_pipeline,
        trigger=IntervalTrigger(minutes=settings.agent1_full_interval_min),
        id="full_pipeline",
        name="Full Inventory Pipeline",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Change watcher — every N minutes (default 5)
    scheduler.add_job(
        job_change_watch,
        trigger=IntervalTrigger(minutes=settings.agent5_poll_interval_min),
        id="change_watch",
        name="Change Watch Poll",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # Lifecycle manager — every N minutes (default 60)
    scheduler.add_job(
        job_lifecycle,
        trigger=IntervalTrigger(minutes=settings.agent6_scan_interval_min),
        id="lifecycle",
        name="Lifecycle Manager",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    log.info("Scheduler configured",
             full_interval_min=settings.agent1_full_interval_min,
             change_interval_min=settings.agent5_poll_interval_min,
             lifecycle_interval_min=settings.agent6_scan_interval_min)


# ── Signal handling ───────────────────────────────────────────────────────────

def handle_shutdown(signum, frame):
    log.info("Shutdown signal received, stopping scheduler")
    scheduler.shutdown(wait=False)
    sys.exit(0)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("NetBox AI Platform Scheduler starting")

    # Initialise database tables
    init_db()

    # Configure and start scheduler
    configure_scheduler()
    scheduler.start()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Run initial full pipeline on startup
    log.info("Running initial full pipeline on startup")
    try:
        job_full_pipeline()
    except Exception as exc:
        log.error("Initial pipeline failed — will retry on next scheduled run", error=str(exc))

    # Keep alive
    log.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        time.sleep(30)
        jobs = scheduler.get_jobs()
        for job in jobs:
            log.debug("Job status", job=job.id, next_run=str(job.next_run_time))
