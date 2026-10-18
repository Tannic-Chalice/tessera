@echo off
cd /d "%~dp0"
echo Running unit tests inside Docker (no local Python needed)...
docker compose build init
docker compose --profile test run --rm tests
pause
