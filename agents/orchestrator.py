"""
agents/orchestrator.py — LangGraph StateGraph orchestrating all 6 agents.
"""
from __future__ import annotations

import json
from typing import Annotated, Any, Dict, List, Optional, TypedDict

import structlog
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

from agents.agent1_collector import Agent1Collector
from agents.agent2_relationship import RelationshipMapper
from agents.agent3_transformer import Agent3Transformer
from agents.agent4_populator import Agent4Populator
from agents.agent5_change_watcher import Agent5ChangeWatcher
from agents.agent6_lifecycle import Agent6LifecycleManager
from core.canonical import InventoryBundle

log = structlog.get_logger(__name__)


# ── Shared workflow state ──────────────────────────────────────────────────────

class PipelineState(TypedDict):
    run_type: str                    # "full" | "incremental"
    bundle: Optional[InventoryBundle]
    graph: Optional[Any]             # NetworkX DiGraph
    yaml_objects: Optional[List[Any]]
    populated_count: int
    change_count: int
    lifecycle_result: Optional[Dict]
    errors: List[str]
    messages: Annotated[List[Any], add_messages]


# ── Node functions ─────────────────────────────────────────────────────────────

def node_collect(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: collect")
    try:
        agent = Agent1Collector()
        bundle = agent.run(run_type=state.get("run_type", "full"))
        return {**state, "bundle": bundle}
    except Exception as exc:
        log.error("Collect node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"collect: {exc}"]}


def node_relate(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: relate")
    bundle = state.get("bundle")
    if not bundle:
        return {**state, "errors": state["errors"] + ["relate: no bundle"]}
    try:
        mapper = RelationshipMapper()
        graph  = mapper.run(bundle)
        return {**state, "graph": graph}
    except Exception as exc:
        log.error("Relate node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"relate: {exc}"]}


def node_transform(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: transform")
    bundle = state.get("bundle")
    if not bundle:
        return {**state, "errors": state["errors"] + ["transform: no bundle"]}
    try:
        transformer = Agent3Transformer()
        yaml_objects = transformer.run(bundle)
        return {**state, "yaml_objects": yaml_objects}
    except Exception as exc:
        log.error("Transform node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"transform: {exc}"]}


def node_populate(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: populate")
    yaml_objects = state.get("yaml_objects")
    try:
        populator = Agent4Populator()
        count = populator.run(yaml_objects=yaml_objects)
        return {**state, "populated_count": count}
    except Exception as exc:
        log.error("Populate node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"populate: {exc}"]}


def node_watch_changes(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: watch_changes")
    try:
        watcher = Agent5ChangeWatcher()
        count = watcher.run()
        return {**state, "change_count": count}
    except Exception as exc:
        log.error("Change watch node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"change_watch: {exc}"]}


def node_lifecycle(state: PipelineState) -> PipelineState:
    log.info("[Orchestrator] Node: lifecycle")
    try:
        lm = Agent6LifecycleManager()
        result = lm.run()
        return {**state, "lifecycle_result": result}
    except Exception as exc:
        log.error("Lifecycle node failed", error=str(exc))
        return {**state, "errors": state["errors"] + [f"lifecycle: {exc}"]}


# ── Conditional edge: skip remaining steps if collect failed ──────────────────

def should_continue_after_collect(state: PipelineState) -> str:
    if state.get("bundle") and state["bundle"].total() > 0:
        return "relate"
    return END


def should_continue_after_relate(state: PipelineState) -> str:
    if len(state.get("errors", [])) == 0 or state.get("bundle"):
        return "transform"
    return "populate"   # skip transform, go straight to populate from DB queue


# ── Build the graph ────────────────────────────────────────────────────────────

def build_pipeline(include_change_watch: bool = True, include_lifecycle: bool = True) -> Any:
    """
    Build and compile the LangGraph pipeline.
    Returns a compiled graph ready to invoke.
    """
    workflow = StateGraph(PipelineState)

    # Add nodes
    workflow.add_node("collect",       node_collect)
    workflow.add_node("relate",        node_relate)
    workflow.add_node("transform",     node_transform)
    workflow.add_node("populate",      node_populate)
    workflow.add_node("watch_changes", node_watch_changes)
    workflow.add_node("lifecycle",     node_lifecycle)

    # Entry point
    workflow.set_entry_point("collect")

    # Edges
    workflow.add_conditional_edges(
        "collect",
        should_continue_after_collect,
        {"relate": "relate", END: END},
    )
    workflow.add_conditional_edges(
        "relate",
        should_continue_after_relate,
        {"transform": "transform", "populate": "populate"},
    )
    workflow.add_edge("transform", "populate")

    if include_change_watch and include_lifecycle:
        workflow.add_edge("populate", "watch_changes")
        workflow.add_edge("watch_changes", "lifecycle")
        workflow.add_edge("lifecycle", END)
    elif include_change_watch:
        workflow.add_edge("populate", "watch_changes")
        workflow.add_edge("watch_changes", END)
    elif include_lifecycle:
        workflow.add_edge("populate", "lifecycle")
        workflow.add_edge("lifecycle", END)
    else:
        workflow.add_edge("populate", END)

    return workflow.compile()


# ── Convenience runner ────────────────────────────────────────────────────────

def run_full_pipeline(run_type: str = "full") -> Dict:
    """Execute the complete agent pipeline and return final state."""
    pipeline = build_pipeline()
    initial_state: PipelineState = {
        "run_type": run_type,
        "bundle": None,
        "graph": None,
        "yaml_objects": None,
        "populated_count": 0,
        "change_count": 0,
        "lifecycle_result": None,
        "errors": [],
        "messages": [],
    }
    final_state = pipeline.invoke(initial_state)
    return {
        "status": "success" if not final_state["errors"] else "partial",
        "errors": final_state["errors"],
        "populated_count": final_state.get("populated_count", 0),
        "change_count": final_state.get("change_count", 0),
        "lifecycle_result": final_state.get("lifecycle_result"),
        "total_collected": final_state["bundle"].total() if final_state.get("bundle") else 0,
    }
