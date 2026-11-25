from fastapi.testclient import TestClient

from app.main import app
from app.rag import rag_store
from app.telemetry import set_simulated_scenario

client = TestClient(app)


def test_health_endpoint() -> None:
    set_simulated_scenario("healthy")
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_rag_retrieves_relevant_kafka_runbook() -> None:
    rag_store.build_index()
    results = rag_store.retrieve("consumer group lag rebalance max.poll.interval.ms", k=2)
    assert len(results) == 2
    sources = {r["source"] for r in results}
    assert "consumer_lag.md" in sources


def test_diagnose_consumer_lag_scenario() -> None:
    res = client.post(
        "/api/diagnose",
        json={"scenario": "consumer_lag", "query": "Why is consumer lag high?"},
    )
    assert res.status_code == 200
    body = res.json()
    rca = body["rca_report"]
    assert rca["severity"] == "WARNING"
    assert "lag" in rca["summary"].lower()
    assert len(rca["remediation_commands"]) >= 1


def test_diagnose_broker_offline_scenario() -> None:
    res = client.post(
        "/api/diagnose",
        json={"scenario": "broker_offline", "query": "Check under-replicated partitions"},
    )
    assert res.status_code == 200
    body = res.json()
    rca = body["rca_report"]
    assert rca["severity"] == "CRITICAL"
    assert body["telemetry"]["kafka"]["brokers_online"] == 1
    assert any("under-replicated" in cmd for cmd in rca["remediation_commands"])


def test_simulate_incident_and_metrics_endpoint() -> None:
    sim_res = client.post("/api/simulate-incident", json={"scenario": "disk_pressure"})
    assert sim_res.status_code == 200
    assert sim_res.json()["active_scenario"] == "disk_pressure"

    metrics_res = client.get("/api/metrics")
    assert metrics_res.status_code == 200
    assert metrics_res.json()["host"]["disk_percent"] >= 90.0

    # Reset back to healthy
    client.post("/api/simulate-incident", json={"scenario": "healthy"})
