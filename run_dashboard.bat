@echo off
setlocal
set "PYTHON_DIR=C:\Users\LENOVO\AppData\Local\Programs\Python\Python313"
set "PATH=%PYTHON_DIR%;%PYTHON_DIR%\Scripts;%PATH%"
set "PYTHONUTF8=1"

echo ============================================================================
echo  Erken Risk Tespiti ve XAI Karar Destek Sistemi Dashboardu Baslatiliyor...
echo  Tarayici Adresi: http://localhost:8501
echo ============================================================================
echo.

start "" cmd /c "timeout /t 3 /nobreak >nul & start http://localhost:8501"

if exist "%PYTHON_DIR%\python.exe" (
    "%PYTHON_DIR%\python.exe" -m streamlit run "%~dp0dashboard.py" --server.headless true --server.port 8501 --browser.gatherUsageStats false
) else (
    py -m streamlit run "%~dp0dashboard.py" --server.headless true --server.port 8501 --browser.gatherUsageStats false
)
pause
