# Runbook: Linux Host Disk Pressure & High Disk I/O

## Symptoms
- Host disk utilization exceeds 85% or disk I/O wait (`%iowait`) spikes.
- Kafka brokers stall on log segment flushes or fail with `No space left on device`.

## Root Cause Checklist
1. Kafka `log.retention.hours` or `log.retention.bytes` misconfigured for high-throughput topics.
2. Unrotated application logs filling `/var/log`.
3. Slow EBS/storage throughput saturating IOPS during compaction or replication.

## Remediation Commands
- Check filesystem usage and inode exhaustion:
  `df -h` and `df -i`
- Identify largest directories in Kafka data path:
  `du -sh /var/lib/kafka/data/* | sort -hr | head -n 10`
- Monitor real-time disk I/O latency:
  `iostat -xz 1 5`
- Enforce topic retention cleanup or expand EBS storage volume.
