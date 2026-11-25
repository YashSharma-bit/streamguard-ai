from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
import psutil
from pydantic import BaseModel, Field

from app.config import settings

IncidentScenario = Literal["healthy", "consumer_lag", "broker_offline", "disk_pressure", "cpu_spike"]


class HostMetrics(BaseModel):
    """Linux/OS host resource utilization metrics."""

    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_percent: float
    disk_read_mb: float
    disk_write_mb: float
    net_bytes_sent_mb: float
    net_bytes_recv_mb: float
    active_connections: int


class KafkaTopicLag(BaseModel):
    """Consumer-group lag metrics for a Kafka topic."""

    topic: str
    consumer_group: str
    partitions: int
    under_replicated_partitions: int
    total_lag: int


class KafkaClusterSnapshot(BaseModel):
    """Point-in-time snapshot of Apache Kafka cluster health."""

    source: Literal["live_kafka", "simulated"]
    bootstrap_servers: str
    brokers_online: int
    brokers_expected: int
    topics: list[KafkaTopicLag]
    recent_logs: list[dict[str, Any]] = Field(default_factory=list)


class TelemetrySnapshot(BaseModel):
    """Combined host + Kafka telemetry payload."""

    timestamp: str
    scenario: IncidentScenario
    host: HostMetrics
    kafka: KafkaClusterSnapshot


_active_scenario: IncidentScenario = "healthy"


def set_simulated_scenario(scenario: IncidentScenario) -> IncidentScenario:
    """Set the active simulated incident scenario for demos and tests."""
    global _active_scenario
    _active_scenario = scenario
    return _active_scenario


def get_simulated_scenario() -> IncidentScenario:
    """Return the currently configured scenario."""
    return _active_scenario


def collect_host_metrics(scenario: IncidentScenario | None = None) -> HostMetrics:
    """Collect real OS CPU, memory, disk I/O, and network stats via psutil."""
    disk = psutil.disk_usage("/")
    mem = psutil.virtual_memory()
    disk_io = psutil.disk_io_counters()
    net_io = psutil.net_io_counters()

    try:
        conn_count = len(psutil.net_connections(kind="inet"))
    except (psutil.AccessDenied, PermissionError):
        conn_count = 0

    metrics = HostMetrics(
        cpu_percent=round(psutil.cpu_percent(interval=0.05), 1),
        memory_percent=round(mem.percent, 1),
        memory_available_mb=round(mem.available / (1024 * 1024), 1),
        disk_percent=round(disk.percent, 1),
        disk_read_mb=round((disk_io.read_bytes if disk_io else 0) / (1024 * 1024), 2),
        disk_write_mb=round((disk_io.write_bytes if disk_io else 0) / (1024 * 1024), 2),
        net_bytes_sent_mb=round((net_io.bytes_sent if net_io else 0) / (1024 * 1024), 2),
        net_bytes_recv_mb=round((net_io.bytes_recv if net_io else 0) / (1024 * 1024), 2),
        active_connections=conn_count,
    )

    if scenario == "disk_pressure":
        metrics.disk_percent = 94.2
    elif scenario == "cpu_spike":
        metrics.cpu_percent = 96.4
        metrics.memory_percent = 91.8
    return metrics


def _probe_live_kafka() -> KafkaClusterSnapshot | None:
    """Attempt to query live Kafka cluster metadata via confluent-kafka."""
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        return None

    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        md = admin.list_topics(timeout=settings.kafka_timeout_sec)
        topic_entries = [
            KafkaTopicLag(
                topic=name,
                consumer_group="payment-events-consumer",
                partitions=len(topic_md.partitions),
                under_replicated_partitions=sum(
                    1 for p in topic_md.partitions.values() if len(p.isrs) < len(p.replicas)
                ),
                total_lag=0,
            )
            for name, topic_md in md.topics.items()
            if not name.startswith("__")
        ]
        return KafkaClusterSnapshot(
            source="live_kafka",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            brokers_online=len(md.brokers),
            brokers_expected=max(len(md.brokers), 2),
            topics=topic_entries or [
                KafkaTopicLag(
                    topic="payment-transactions",
                    consumer_group="payment-events-consumer",
                    partitions=6,
                    under_replicated_partitions=0,
                    total_lag=0,
                )
            ],
            recent_logs=[{"level": "INFO", "service": "kafka-monitor", "msg": "Live cluster metadata healthy"}],
        )
    except Exception:
        return None


def _build_simulated_kafka(scenario: IncidentScenario) -> KafkaClusterSnapshot:
    """Generate realistic Kafka cluster telemetry and JSON logs for a scenario."""
    now = datetime.now(timezone.utc).isoformat()
    scenarios: dict[IncidentScenario, tuple[int, int, int, list[dict[str, Any]]]] = {
        "healthy": (
            2,
            0,
            18,
            [{"ts": now, "level": "INFO", "component": "kafka-consumer", "msg": "Offsets committed normally"}],
        ),
        "consumer_lag": (
            2,
            0,
            14820,
            [
                {
                    "ts": now,
                    "level": "ERROR",
                    "component": "payment-events-consumer",
                    "error": "CommitFailedException",
                    "msg": "Consumer group rebalance triggered; max.poll.interval.ms exceeded on partition 2",
                },
                {
                    "ts": now,
                    "level": "WARN",
                    "component": "lag-exporter",
                    "msg": "Consumer lag on payment-transactions crossed 10,000 threshold (current: 14,820)",
                },
            ],
        ),
        "broker_offline": (
            1,
            4,
            3250,
            [
                {
                    "ts": now,
                    "level": "CRITICAL",
                    "component": "kafka-controller",
                    "error": "BrokerNotAvailableException",
                    "msg": "Broker 2 unreachable on internal:29094; 4 under-replicated partitions detected",
                },
                {
                    "ts": now,
                    "level": "ERROR",
                    "component": "producer-gateway",
                    "error": "NOT_ENOUGH_REPLICAS",
                    "msg": "In-sync replica count below min.insync.replicas for payment-transactions",
                },
            ],
        ),
        "disk_pressure": (
            2,
            0,
            1950,
            [
                {
                    "ts": now,
                    "level": "ERROR",
                    "component": "kafka-broker-1",
                    "error": "IOException",
                    "msg": "Disk utilization at 94.2% on /var/lib/kafka/data; log segment flush delayed",
                }
            ],
        ),
        "cpu_spike": (
            2,
            0,
            4100,
            [
                {
                    "ts": now,
                    "level": "WARN",
                    "component": "linux-host-probe",
                    "msg": "Host CPU at 96.4% and RAM at 91.8%; elevated TCP TIME_WAIT sockets on port 9092",
                }
            ],
        ),
    }

    brokers_online, urp, lag, logs = scenarios[scenario]
    return KafkaClusterSnapshot(
        source="simulated",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        brokers_online=brokers_online,
        brokers_expected=2,
        topics=[
            KafkaTopicLag(
                topic="payment-transactions",
                consumer_group="payment-events-consumer",
                partitions=6,
                under_replicated_partitions=urp,
                total_lag=lag,
            ),
            KafkaTopicLag(
                topic="settlement-events",
                consumer_group="settlement-sink-group",
                partitions=3,
                under_replicated_partitions=0 if urp == 0 else 1,
                total_lag=max(4, lag // 8),
            ),
        ],
        recent_logs=logs,
    )


def collect_telemetry(scenario: IncidentScenario | None = None) -> TelemetrySnapshot:
    """Collect unified host + Kafka telemetry (live if healthy/available, or simulated)."""
    chosen = scenario or _active_scenario
    host = collect_host_metrics(chosen)

    if chosen == "healthy":
        live = _probe_live_kafka()
        if live is not None:
            return TelemetrySnapshot(
                timestamp=datetime.now(timezone.utc).isoformat(),
                scenario=chosen,
                host=host,
                kafka=live,
            )

    kafka = _build_simulated_kafka(chosen)
    return TelemetrySnapshot(
        timestamp=datetime.now(timezone.utc).isoformat(),
        scenario=chosen,
        host=host,
        kafka=kafka,
    )
