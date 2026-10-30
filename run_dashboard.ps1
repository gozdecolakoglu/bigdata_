$ErrorActionPreference = "Stop"
$PythonDir = "C:\Users\LENOVO\AppData\Local\Programs\Python\Python313"
if (Test-Path $PythonDir) {
    $env:PATH = "$PythonDir;$PythonDir\Scripts;$env:PATH"
}
$env:PYTHONUTF8 = "1"

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host " Erken Risk Tespiti ve XAI Karar Destek Sistemi Dashboardu Baslatiliyor..." -ForegroundColor Green
Write-Host " Tarayici Adresi: http://localhost:8501" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Cyan

Start-Job -ScriptBlock { Start-Sleep -Seconds 3; Start-Process "http://localhost:8501" } | Out-Null

$PythonExe = Join-Path $PythonDir "python.exe"
$DashboardPath = Join-Path $PSScriptRoot "dashboard.py"

if (Test-Path $PythonExe) {
    & $PythonExe -m streamlit run $DashboardPath --server.headless true --server.port 8501 --browser.gatherUsageStats false
} else {
    py -m streamlit run $DashboardPath --server.headless true --server.port 8501 --browser.gatherUsageStats false
}

