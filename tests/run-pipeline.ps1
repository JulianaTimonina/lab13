# run-pipeline.ps1 — поднять stack, прогнать pytest (скейлинг + аукцион)
param(
    [string]$Project = "lab13_3",
    [switch]$SkipBuild,
    [switch]$SkipUp
)

$ErrorActionPreference = "Stop"
$TestsDir = $PSScriptRoot
$ProjectRoot = Split-Path $TestsDir -Parent
Set-Location $ProjectRoot

function Fail($msg) { Write-Error $msg; exit 1 }

Write-Host "==> Project root: $ProjectRoot" -ForegroundColor Cyan
Write-Host "==> Compose project: $Project" -ForegroundColor Cyan

if (-not $SkipBuild) {
    Write-Host "==> Build images..." -ForegroundColor Cyan
    docker compose -p $Project build
}

if (-not $SkipUp) {
    Write-Host "==> Start services..." -ForegroundColor Cyan
    docker compose -p $Project up -d
    Start-Sleep -Seconds 8
}

Write-Host "==> Health: orchestrator :8080" -ForegroundColor Cyan
try {
    $r = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/start" `
        -ContentType "application/json" -Body '{"client_id":"pipeline-smoke"}' -TimeoutSec 15
    Write-Host "    OK task_id=$($r.task_id)"
} catch {
    Fail "Orchestrator не отвечает на :8080. Проверьте: docker compose -p $Project ps"
}

Start-Sleep -Seconds 3

Write-Host "==> Auction logs (last lines)..." -ForegroundColor Cyan
$orch = "${Project}-orchestrator-1"
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$auction = @(docker logs $orch 2>&1 | Select-String "Auction" | Select-Object -Last 4)
$ErrorActionPreference = $prevEAP
if ($auction) {
    $auction | ForEach-Object { Write-Host "    $_" }
} else {
    Write-Host "    (строк Auction пока нет — появятся после нагрузки в pytest)" -ForegroundColor DarkGray
}

Write-Host "==> Pytest tests/test_scaling.py..." -ForegroundColor Cyan
$pytestArgs = @("tests/test_scaling.py", "-v")
if (Get-Command pytest -ErrorAction SilentlyContinue) {
    & pytest @pytestArgs
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3 -m pytest @pytestArgs
} else {
    Fail "pytest not found. Install: pip install pytest requests"
}

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "==> Pipeline OK" -ForegroundColor Green
