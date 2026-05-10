# Bootstrap a Python venv, install deps, and prepare the .env file.
$ErrorActionPreference = "Stop"
$Here = Resolve-Path "$PSScriptRoot/.."
Set-Location $Here

Write-Host "[setup] Python venv"
if (-not (Test-Path ".venv")) {
    python -m venv .venv
}
& ".venv/Scripts/Activate.ps1"

Write-Host "[setup] pip install"
python -m pip install --upgrade pip
pip install -r requirements.txt

Write-Host "[setup] .env"
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "  -> created .env (please edit Reddit credentials before live mode)"
}

Write-Host "[setup] Docker check"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Warning "docker not found on PATH. Install Docker Desktop."
}

Write-Host "[setup] OK"
