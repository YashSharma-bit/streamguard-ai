from __future__ import annotations

from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.agent import run_diagnostic_workflow
from app.telemetry import IncidentScenario, collect_telemetry, get_simulated_scenario, set_simulated_scenario

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(
    title="StreamGuard AI",
    description="Apache Kafka Reliability Monitor & LangGraph Agentic RCA Assistant",
    version="1.0.0",
)


class DiagnoseRequest(BaseModel):
    query: str = "Diagnose current Kafka cluster and Linux host health"
    scenario: IncidentScenario | None = None


class SimulateRequest(BaseModel):
    scenario: IncidentScenario


@app.get("/", include_in_schema=False)
def serve_dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok", "active_scenario": get_simulated_scenario()}


@app.get("/api/metrics")
def get_metrics() -> dict:
    snapshot = collect_telemetry()
    return snapshot.model_dump()


@app.post("/api/simulate-incident")
def simulate_incident(payload: SimulateRequest) -> dict[str, str]:
    active = set_simulated_scenario(payload.scenario)
    return {"status": "updated", "active_scenario": active}


@app.post("/api/diagnose")
def diagnose(payload: DiagnoseRequest) -> dict:
    return run_diagnostic_workflow(scenario=payload.scenario, query=payload.query)
