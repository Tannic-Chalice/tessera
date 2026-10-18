@echo off
cd /d "%~dp0"
echo Stopping TESSERA (data and Kafka topics are kept; run start.bat to resume)...
docker compose stop
