@echo off
cd /d "%~dp0"
echo ===== containers =====
docker compose ps
echo.
echo ===== consumer groups (partition assignment + lag) =====
for %%G in (tessera-fastplane tessera-gate tessera-slowplane tessera-sink) do (
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group %%G 2>nul
)
