# End-to-end orchestrator for a demo run.
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot/..")

& "$PSScriptRoot/setup.ps1"
& "$PSScriptRoot/start_infra.ps1"

if (-not (Test-Path "model/artifacts/sentiment.int8.onnx")) {
    Write-Host "[run_all] no model artifact found; train+export+eval"
    python scripts/prepare_dataset.py
    python scripts/train_model.py --epochs 3
    python scripts/export_model.py
    python scripts/eval_model.py
}

$Mode = if ($env:MODE) { $env:MODE } else { "replay" }
if ($Mode -eq "live") {
    Start-Process -NoNewWindow bash -ArgumentList "scripts/start_producers.sh","--mode","live"
} else {
    if (-not $env:FILE) { throw "set `$env:FILE = 'path/to/dump.ndjson' for replay mode" }
    $rate = if ($env:RATE) { $env:RATE } else { "max" }
    Start-Process -NoNewWindow bash -ArgumentList "scripts/start_producers.sh","--mode","replay","--file",$env:FILE,"--rate",$rate
}

& "$PSScriptRoot/submit_flink_job.ps1"
bash scripts/start_dashboard.sh
