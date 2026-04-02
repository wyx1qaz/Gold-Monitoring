@echo off
setlocal

cd /d "%~dp0"

echo [INFO] Checking existing process on port 8000...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":8000 .*LISTENING"') do (
  echo [INFO] Stopping PID %%P
  taskkill /PID %%P /F >nul 2>nul
)

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Python not found: .venv\Scripts\python.exe
  echo [HINT] Please create venv and install dependencies first.
  pause
  exit /b 1
)

echo [INFO] Starting server at http://127.0.0.1:8000
echo [INFO] Keep this window open while using the app.

".venv\Scripts\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000

endlocal
