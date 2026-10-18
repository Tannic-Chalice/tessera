@echo off
cd /d "%~dp0"
echo This removes all containers, Kafka topics and the TimescaleDB volume.
set /p ok=Type YES to continue: 
if /i not "%ok%"=="YES" exit /b 0
docker compose down -v --remove-orphans
echo Done. Run start.bat for a fresh pipeline.
