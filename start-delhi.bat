@echo off
rem Delhi only. Uses OTD_API_KEY from .env or prompts when blank; otherwise simulated GPS is used.
rem Run reset.bat once when switching modes.
cd /d "%~dp0"
for /f "tokens=1,* delims==" %%A in ('findstr /b /c:"OTD_API_KEY=" .env') do if "%%B"=="" set /p OTD_API_KEY=Paste your Delhi OTD API key and press Enter: 
set SOURCE_MODE=delhi
call start.bat
