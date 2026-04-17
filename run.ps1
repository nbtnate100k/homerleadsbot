# Launches the bot and kills stale copies that still run old code (fixes "Demo bot" ghost).
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

$here = (Get-Item $PSScriptRoot).FullName
$botPy = Join-Path $here "bot.py"
$venvPy = Join-Path $here ".venv\Scripts\python.exe"

Write-Host ""
Write-Host "========== LEADBOT ==========" -ForegroundColor Cyan
Write-Host "Folder: $here"
Write-Host "Script: $botPy"
if (Test-Path $botPy) {
    Write-Host "Modified:" (Get-Item $botPy).LastWriteTime
}
Write-Host ""

if (-not (Test-Path $venvPy)) {
    Write-Host "Creating venv + installing deps..." -ForegroundColor Yellow
    python -m venv .venv
    & $venvPy -m pip install -r (Join-Path $here "requirements.txt")
}

Write-Host "Stopping old bot processes..." -ForegroundColor Yellow
$names = @("python.exe", "python3.exe", "pythonw.exe")
$toStop = foreach ($name in $names) {
    Get-CimInstance Win32_Process -Filter "Name = '$name'" -ErrorAction SilentlyContinue
}
$stoppedAny = $false
foreach ($p in $toStop) {
    $cmd = $p.CommandLine
    if (-not $cmd) { continue }
    if ($cmd -notmatch "bot\.py") { continue }
    $hereLower = $here.ToLowerInvariant()
    $cmdLower = $cmd.ToLowerInvariant()
    $sameFolder = $cmdLower.Contains($hereLower)
    $leadBot = $cmdLower -match "leadbot"
    if ($sameFolder -or $leadBot) {
        Write-Host "  Stop PID $($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        $stoppedAny = $true
    }
}
if (-not $stoppedAny) { Write-Host "  (none matched LEADBOT / this folder)" }

# Telegram needs a moment to release the old long-poll after kill
if ($stoppedAny) {
    Write-Host "Waiting for Telegram to release getUpdates (8s)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 8
} else {
    Start-Sleep -Seconds 2
}

Remove-Item -Recurse -Force (Join-Path $here "__pycache__") -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Starting bot (leave this window OPEN)..." -ForegroundColor Green
Write-Host "Only ONE copy can poll this token. Port 37651 = lock on this PC." -ForegroundColor Gray
Write-Host ""
& $venvPy $botPy
