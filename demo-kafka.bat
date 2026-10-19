@echo off
cd /d "%~dp0"
set K=docker compose exec -T kafka /opt/kafka/bin
echo ===== 1. Topics (Kafka is the backbone between every component) =====
%K%/kafka-topics.sh --bootstrap-server localhost:9092 --list
echo.
echo ===== 2. Partitioning of gps.raw (key = coarse H3 cell, so partition == region) =====
%K%/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic gps.raw
echo.
echo ===== 3. Consumer group tessera-fastplane (partitions shared between instances) =====
%K%/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
echo.
echo ===== 4. Five raw GPS events (key = H3 cell : value = JSON) =====
%K%/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gps.raw --max-messages 5 --property print.key=true --property print.partition=true --timeout-ms 15000
echo.
echo ===== 5. Five windowed aggregates =====
%K%/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tile.aggregates --max-messages 5 --property print.partition=true --timeout-ms 20000
echo.
echo ===== 6. Latest alerts =====
%K%/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning --max-messages 5 --timeout-ms 15000
pause
