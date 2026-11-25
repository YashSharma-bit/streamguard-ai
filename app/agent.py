from __future__ import annotations

import json
from typing import Any, Literal, TypedDict
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from app.config import settings
from app.rag import rag_store
from app.telemetry import IncidentScenario, TelemetrySnapshot, collect_telemetry


class RCAReport(BaseModel):
    """Structured Root Cause Analysis report produced by the LangGraph agent."""

    severity: Literal["INFO", "WARNING", "CRITICAL"]
    summary: str
    root_cause: str
    affected_components: list[str]
    evidence: list[str]
    matched_runbooks: list[str]
    remediation_commands: list[str]
    reasoning_mode: Literal["llm", "deterministic_offline"] = Field(default="deterministic_offline")


class AgentState(TypedDict, total=False):
    scenario: IncidentScenario | None
    query: str
    telemetry: TelemetrySnapshot
    runbooks: list[dict[str, str]]
    rca_report: RCAReport


def _node_collect_telemetry(state: AgentState) -> AgentState:
    snapshot = collect_telemetry(state.get("scenario"))
    return {"telemetry": snapshot}


def _build_diagnostic_query(snapshot: TelemetrySnapshot, user_query: str) -> str:
    log_msgs = " ".join(str(entry.get("msg", "")) + " " + str(entry.get("error", "")) for entry in snapshot.kafka.recent_logs)
    max_lag = max((t.total_lag for t in snapshot.kafka.topics), default=0)
    urp = sum(t.under_replicated_partitions for t in snapshot.kafka.topics)
    return (
        f"{user_query} brokers={snapshot.kafka.brokers_online}/{snapshot.kafka.brokers_expected} "
        f"lag={max_lag} under_replicated={urp} cpu={snapshot.host.cpu_percent} "
        f"mem={snapshot.host.memory_percent} disk={snapshot.host.disk_percent} {log_msgs}"
    ).strip()


def _node_retrieve_runbooks(state: AgentState) -> AgentState:
    snapshot = state["telemetry"]
    query = _build_diagnostic_query(snapshot, state.get("query", ""))
    matched = rag_store.retrieve(query, k=2)
    return {"runbooks": matched}


def _synthesize_with_llm(snapshot: TelemetrySnapshot, runbooks: list[dict[str, str]], user_query: str) -> RCAReport | None:
    if not settings.openai_api_key:
        return None

    try:
        from langchain_core.messages import HumanMessage, SystemMessage
        from langchain_openai import ChatOpenAI

        llm = ChatOpenAI(model=settings.openai_model, api_key=settings.openai_api_key, temperature=0)
        structured_llm = llm.with_structured_output(RCAReport)
        context = {
            "telemetry": snapshot.model_dump(),
            "runbooks": runbooks,
            "operator_question": user_query,
        }
        report = structured_llm.invoke(
            [
                SystemMessage(
                    content=(
                        "You are an SRE diagnosing an Apache Kafka & Linux platform incident. "
                        "Use only the provided telemetry and runbooks to produce a concise RCA report."
                    )
                ),
                HumanMessage(content=json.dumps(context)),
            ]
        )
        report.reasoning_mode = "llm"
        return report
    except Exception:
        return None


def _synthesize_offline(snapshot: TelemetrySnapshot, runbooks: list[dict[str, str]]) -> RCAReport:
    """Deterministic rule-assisted RCA synthesis when no external LLM key is configured."""
    max_lag = max((t.total_lag for t in snapshot.kafka.topics), default=0)
    total_urp = sum(t.under_replicated_partitions for t in snapshot.kafka.topics)
    runbook_names = [rb["source"] for rb in runbooks]
    evidence = [f"{log.get('level', 'INFO')}: {log.get('msg', '')}" for log in snapshot.kafka.recent_logs]

    if snapshot.kafka.brokers_online < snapshot.kafka.brokers_expected or total_urp > 0:
        return RCAReport(
            severity="CRITICAL",
            summary="Kafka broker outage and under-replicated partitions detected.",
            root_cause=(
                f"Only {snapshot.kafka.brokers_online}/{snapshot.kafka.brokers_expected} brokers are online "
                f"with {total_urp} under-replicated partitions, breaching min.insync.replicas."
            ),
            affected_components=["kafka-broker-2", "payment-transactions", "producer-gateway"],
            evidence=evidence,
            matched_runbooks=runbook_names,
            remediation_commands=[
                "docker ps -a | grep kafka",
                "docker logs --tail 200 streamguard-kafka-2",
                "kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions",
            ],
        )

    if snapshot.host.disk_percent >= 85.0:
        return RCAReport(
            severity="CRITICAL",
            summary="Host disk capacity approaching exhaustion on Kafka storage volume.",
            root_cause=(
                f"Disk utilization reached {snapshot.host.disk_percent}%, delaying Kafka log segment flushes "
                "and risking broker write stalls."
            ),
            affected_components=["linux-storage", "kafka-broker-1"],
            evidence=evidence,
            matched_runbooks=runbook_names,
            remediation_commands=[
                "df -h && df -i",
                "du -sh /var/lib/kafka/data/* | sort -hr | head -n 10",
                "iostat -xz 1 5",
            ],
        )

    if max_lag >= 5000:
        return RCAReport(
            severity="WARNING",
            summary="Elevated consumer-group lag detected on payment-transactions topic.",
            root_cause=(
                f"Consumer group lag spiked to {max_lag:,} messages due to rebalance loops "
                "(max.poll.interval.ms exceeded) or slow downstream writes."
            ),
            affected_components=["payment-events-consumer", "payment-transactions"],
            evidence=evidence,
            matched_runbooks=runbook_names,
            remediation_commands=[
                "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-events-consumer",
                "kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic payment-transactions",
            ],
        )

    if snapshot.host.cpu_percent >= 85.0 or snapshot.host.memory_percent >= 85.0:
        return RCAReport(
            severity="WARNING",
            summary="Host CPU and memory saturation impacting streaming throughput.",
            root_cause=(
                f"Linux host CPU ({snapshot.host.cpu_percent}%) and memory ({snapshot.host.memory_percent}%) "
                "are saturated, increasing GC pauses and TCP socket contention."
            ),
            affected_components=["linux-host", "kafka-clients"],
            evidence=evidence,
            matched_runbooks=runbook_names,
            remediation_commands=[
                "top -b -n 1 | head -n 20",
                "free -m && vmstat 1 5",
                "ss -tanp | grep 9092",
            ],
        )

    return RCAReport(
        severity="INFO",
        summary="Kafka streaming cluster and Linux host metrics are healthy.",
        root_cause="No active anomalies detected; all brokers online and consumer lag is within baseline.",
        affected_components=[],
        evidence=evidence or ["All monitored metrics within normal thresholds."],
        matched_runbooks=runbook_names,
        remediation_commands=["kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list"],
    )


def _node_diagnose_and_remediate(state: AgentState) -> AgentState:
    snapshot = state["telemetry"]
    runbooks = state.get("runbooks", [])
    report = _synthesize_with_llm(snapshot, runbooks, state.get("query", "")) or _synthesize_offline(snapshot, runbooks)
    return {"rca_report": report}


def build_diagnostic_graph() -> Any:
    """Compile the stateful LangGraph workflow for SRE incident diagnosis."""
    graph = StateGraph(AgentState)
    graph.add_node("collect_telemetry", _node_collect_telemetry)
    graph.add_node("retrieve_runbooks", _node_retrieve_runbooks)
    graph.add_node("diagnose_and_remediate", _node_diagnose_and_remediate)

    graph.set_entry_point("collect_telemetry")
    graph.add_edge("collect_telemetry", "retrieve_runbooks")
    graph.add_edge("retrieve_runbooks", "diagnose_and_remediate")
    graph.add_edge("diagnose_and_remediate", END)
    return graph.compile()


diagnostic_graph = build_diagnostic_graph()


def run_diagnostic_workflow(
    scenario: IncidentScenario | None = None,
    query: str = "Diagnose current Kafka cluster and Linux host health",
) -> dict[str, Any]:
    """Execute the full LangGraph telemetry -> RAG -> RCA pipeline."""
    result = diagnostic_graph.invoke({"scenario": scenario, "query": query})
    return {
        "telemetry": result["telemetry"].model_dump(),
        "matched_runbooks": [{"source": r["source"], "title": r["title"]} for r in result["runbooks"]],
        "rca_report": result["rca_report"].model_dump(),
    }
