@echo off
rem REAL live data, Delhi only. The key is read from .env (OTD_API_KEY=...).
rem Run reset.bat once when switching modes.
cd /d "%~dp0"
findstr /b /c:"OTD_API_KEY=" .env >/dev/null 2>/dev/null || set /p OTD_API_KEY=Paste your Delhi OTD API key and press Enter: 
set SOURCE_MODE=delhi
call start.bat
