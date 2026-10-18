@echo off
rem Benchmark mode: replay the Microsoft T-Drive taxi traces (Beijing) instead of the live Bengaluru feed.
rem 1. Download T-Drive from Microsoft Research and copy the .txt files into data\tdrive\
rem 2. Run reset.bat once when switching between Bengaluru and T-Drive (clears data from the previous city)
rem 3. Run this file. To go back to Bengaluru: reset.bat, then start.bat
cd /d "%~dp0"
set SOURCE_MODE=tdrive
set SPEEDUP=60
call start.bat
