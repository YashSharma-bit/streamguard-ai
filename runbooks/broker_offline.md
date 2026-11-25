# Runbook: Kafka Broker Offline or Under-Replicated Partitions

## Symptoms
- `UnderReplicatedPartitions > 0` or `OfflinePartitionsCount > 0`.
- Producer errors: `NOT_ENOUGH_REPLICAS`, `LEADER_NOT_AVAILABLE`, or `BrokerNotAvailableException`.
- Broker unreachable on port 9092/29092.

## Root Cause Checklist
1. Broker JVM crashed due to OutOfMemoryError (OOM) or GC pause exceeding session timeout.
2. Host disk full on Kafka `log.dirs` volume causing broker shutdown.
3. Network partition or controller quorum loss between brokers.

## Remediation Commands
- Verify broker process and container health:
  `docker ps -a | grep kafka` and `ss -tulpn | grep 9092`
- Inspect under-replicated partitions:
  `kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions`
- Check broker logs for OOM or IO errors:
  `docker logs --tail 200 streamguard-kafka-1`
- Check disk space on log directory: `df -h /var/lib/kafka/data`
