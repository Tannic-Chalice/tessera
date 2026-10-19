@echo off
setlocal
title TESSERA - start
cd /d "%~dp0"
echo.
echo  ==============================================================
echo   TESSERA  -  Geo-Agentic Real-Time Spatial Stream Analytics
echo   Data Source ^> Kafka ^> Stream Processing ^> Storage ^> Dashboard
echo  ==============================================================
echo.

where docker >nul 2>nul
if errorlevel 1 (
  echo [X] Docker is not installed. Install Docker Desktop for Windows ^(free^):
  echo     https://www.docker.com/products/docker-desktop/
  pause
  exit /b 1
)
docker info >nul 2>nul
if errorlevel 1 (
  echo [X] Docker Desktop is installed but not running. Start Docker Desktop,
  echo     wait until it says "Engine running", then run start.bat again.
  pause
  exit /b 1
)

echo [1/3] Building the TESSERA image ^(first run downloads ~1.5 GB, be patient^)...
docker compose build init
if errorlevel 1 (
  echo [X] Image build failed - see the messages above.
  pause
  exit /b 1
)
echo       Starting containers...
docker compose up -d
if errorlevel 1 (
  echo [X] docker compose failed - see the messages above.
  pause
  exit /b 1
)

echo [2/3] Waiting for the dashboard on http://localhost:8000 ...
set /a tries=0
:wait
set /a tries+=1
curl -s -o nul http://localhost:8000/ >nul 2>nul
if not errorlevel 1 goto ready
if %tries% GEQ 90 goto slow
timeout /t 2 /nobreak >nul
goto wait

:slow
echo [!] Dashboard not answering yet. Check:  docker compose ps   and   logs.bat
goto open

:ready
echo [3/3] Pipeline is up.

:open
echo.
echo   Dashboard ........ http://localhost:8000
echo   Kafka UI ......... http://localhost:8085
echo   BLR GTFS-RT .... http://localhost:8090/VehiclePositions.pb  (india mode: simulated Bengaluru feed)
echo   TimescaleDB ...... localhost:5433  (db/user/password: tessera)
echo.
echo   The first windows close about 70 s after start; the first incident is injected after ~45 s.
echo   Useful: status.bat  logs.bat  demo-kafka.bat  demo-fault-tolerance.bat  scale.bat 3  stop.bat
echo.
start "" http://localhost:8000
endlocal
