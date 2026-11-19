#!/usr/bin/env bash
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
K=(docker compose exec -T kafka /opt/kafka/bin)
echo "================================================================"
echo " Fault-tolerance demo"
echo "  1) kill one fast-plane instance (simulated crash, no clean shutdown)"
echo "  2) Kafka rebalances its partitions to the surviving instance"
echo "  3) the survivor restores window state from the checkpoint and"
echo "     resumes from the checkpointed offset (no data loss, no double count)"
echo "  4) the crashed instance is restarted and partitions are shared again"
echo "================================================================"
echo
echo "--- before ---"
"${K[@]}"/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
echo
echo "--- docker kill tessera-fastplane-1 ---"
docker kill tessera-fastplane-1
echo "waiting 25 s for the session timeout and rebalance..."
sleep 25
echo
echo "--- after the crash: all partitions owned by the survivor ---"
"${K[@]}"/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
echo
echo "--- survivor log (look for RECOVERED partition ... from checkpoint) ---"
docker compose logs --tail 25 fastplane
echo
echo "--- restarting the crashed instance ---"
docker start tessera-fastplane-1
sleep 20
"${K[@]}"/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
echo
echo "Other things to try:"
echo "  docker restart tessera-timescaledb-1   (sink rewinds and retries the batch; nothing lost)"
echo "  docker restart tessera-kafka-1         (every service reconnects; idempotent producers)"
echo "  docker kill tessera-slowplane-1        (fast plane keeps running at full speed: P1)"
