# Runbook: High Kafka Consumer Group Lag

## Symptoms
- Consumer group lag exceeds threshold (e.g., > 5,000 messages across topic partitions).
- End-to-end event processing latency spikes; downstream databases or APIs show stale data.
- Error logs show `CommitFailedException`, `rebalance`, or `max.poll.interval.ms` timeouts.

## Root Cause Checklist
1. Consumer processing time exceeds `max.poll.interval.ms` causing frequent group rebalances.
2. Uneven partition key distribution (hot partition) overloading a single consumer instance.
3. Downstream database or external REST API latency throttling consumer throughput.
4. Fewer active consumer instances than topic partitions.

## Remediation Commands
- Inspect consumer group lag and partition assignment:
  `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-events-consumer`
- Check topic partition count and ISR status:
  `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic payment-transactions`
- Scale consumer pods/workers up to match partition count or tune `max.poll.records` / `max.poll.interval.ms`.
