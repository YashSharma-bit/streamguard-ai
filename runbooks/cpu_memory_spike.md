# Runbook: Linux Host CPU, Memory & Network Saturation

## Symptoms
- Host CPU utilization > 85% or available RAM < 10%, triggering swap thrashing or OOM killer.
- Elevated TCP retransmits or socket TIME_WAIT exhaustion between Kafka producers/consumers.

## Root Cause Checklist
1. Unbounded consumer memory buffers or batch decompression spiking JVM/Python heap.
2. Network interface saturation during cross-AZ broker replication.
3. Runaway worker process consuming CPU cores.

## Remediation Commands
- Inspect top CPU/memory processes and virtual memory stats:
  `top -b -n 1 | head -n 20` and `free -m` and `vmstat 1 5`
- Inspect active TCP socket states and port connections:
  `ss -s` and `ss -tanp | grep 9092`
- Check kernel OOM events:
  `dmesg -T | grep -i -E "oom|killed process"`
