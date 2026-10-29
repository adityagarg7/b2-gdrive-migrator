@echo off
setlocal
cd /d "%~dp0"

echo ===============================================================
echo  FourVectors B2 -^> Google Drive Migrator (Setup ^& Launch)
echo ===============================================================

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python 3.10+ is not installed or not on PATH.
    echo Install Python from https://www.python.org/downloads/ and check "Add python.exe to PATH".
    pause
    exit /b 1
)

echo [1/2] Ensuring Python dependencies (requests, tabulate) are installed...
python -m pip install -q -r requirements.txt

echo [2/2] Starting Migration Studio Dashboard (auto-downloads rclone if needed)...
python migrate.py dashboard
pause
