@echo off
cd /d "%~dp0"
rem usage: scale.bat 3   -> run 3 fast-plane consumers in group tessera-fastplane (max useful = 6 partitions)
set N=%~1
if "%N%"=="" set N=3
echo Scaling the fast plane to %N% instance(s). Kafka rebalances the 6 partitions of gps.raw;
echo each instance restores the state of the partitions it receives from its checkpoint.
docker compose up -d --no-recreate --scale fastplane=%N% fastplane
timeout /t 15 /nobreak >nul
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
