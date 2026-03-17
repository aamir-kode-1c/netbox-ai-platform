#!/usr/bin/env python3
"""
scripts/run_pipeline.py — Manually trigger the full or partial pipeline.
Usage:
    python scripts/run_pipeline.py                  # full pipeline
    python scripts/run_pipeline.py --agent collect  # single agent
    python scripts/run_pipeline.py --agent lifecycle
"""
import argparse
import sys

import structlog

from core.database import init_db
from core.utils import setup_logging

setup_logging()
log = structlog.get_logger(__name__)


def run_full(run_type: str = "full"):
    from agents.orchestrator import run_full_pipeline
    log.info("Running full pipeline", run_type=run_type)
    result = run_full_pipeline(run_type=run_type)
    log.info("Pipeline complete", **{k: v for k, v in result.items() if not isinstance(v, dict)})
    if result.get("errors"):
        log.warning("Errors encountered", errors=result["errors"])
    return result


def run_collect():
    from agents.agent1_collector import Agent1Collector
    bundle = Agent1Collector().run()
    log.info("Collection complete", total=bundle.total())
    return bundle


def run_relate():
    from agents.agent1_collector import Agent1Collector
    from agents.agent2_relationship import RelationshipMapper
    bundle = Agent1Collector().run()
    graph = RelationshipMapper().run(bundle)
    log.info("Relationship mapping complete", edges=graph.number_of_edges())


def run_transform():
    from agents.agent1_collector import Agent1Collector
    from agents.agent3_transformer import Agent3Transformer
    bundle = Agent1Collector().run()
    objs = Agent3Transformer().run(bundle)
    log.info("Transformation complete", yaml_objects=len(objs))


def run_populate():
    from agents.agent4_populator import Agent4Populator
    count = Agent4Populator().run()
    log.info("Population complete", count=count)


def run_change_watch():
    from agents.agent5_change_watcher import Agent5ChangeWatcher
    changes = Agent5ChangeWatcher().run()
    log.info("Change watch complete", changes=changes)


def run_lifecycle():
    from agents.agent6_lifecycle import Agent6LifecycleManager
    result = Agent6LifecycleManager().run()
    log.info("Lifecycle scan complete", **result)


def run_build_index():
    from agents.agent3_transformer import Agent3Transformer
    Agent3Transformer().build_schema_index()
    log.info("Schema index rebuilt")


AGENTS = {
    "full":       run_full,
    "collect":    run_collect,
    "relate":     run_relate,
    "transform":  run_transform,
    "populate":   run_populate,
    "watch":      run_change_watch,
    "lifecycle":  run_lifecycle,
    "index":      run_build_index,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NetBox AI Pipeline Runner")
    parser.add_argument(
        "--agent",
        choices=list(AGENTS.keys()),
        default="full",
        help="Which agent/pipeline to run (default: full)",
    )
    parser.add_argument(
        "--type",
        choices=["full", "incremental"],
        default="full",
        help="Run type for collect/full (default: full)",
    )
    args = parser.parse_args()

    try:
        init_db()
        fn = AGENTS[args.agent]
        if args.agent == "full":
            fn(run_type=args.type)
        else:
            fn()
        sys.exit(0)
    except Exception as exc:
        log.error("Pipeline failed", agent=args.agent, error=str(exc))
        sys.exit(1)
