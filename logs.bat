@echo off
cd /d "%~dp0"
rem usage: logs.bat            (all pipeline services)
rem        logs.bat fastplane  (one service: producer fastplane gate slowplane sink dashboard kafka)
if "%~1"=="" (
  docker compose logs -f --tail 40 producer fastplane gate slowplane sink
) else (
  docker compose logs -f --tail 80 %*
)
