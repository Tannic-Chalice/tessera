@echo off
REM Real BMTC buses currently in TESSERA (from Kafka gps.raw). Usage: bmtc-now.bat [minutes]
cd /d "%~dp0"
set MIN=%1
if "%MIN%"=="" set MIN=3
powershell -NoProfile -Command "Get-Content scripts\bmtc_now.py | docker compose exec -T producer python - %MIN%"
pause
