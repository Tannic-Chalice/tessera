@echo off
rem Bengaluru only (simulated mixed traffic on real roads, no Delhi feed). Run reset.bat once when switching modes.
cd /d "%~dp0"
set SOURCE_MODE=synthetic
call start.bat
