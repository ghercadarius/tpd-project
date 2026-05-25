# Bring up infra and create Kafka topics.
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot/..")

Write-Host "[infra] docker compose up -d"
docker compose -f infra/docker-compose.yml up -d

Write-Host "[infra] waiting for Kafka health ..."
for ($i = 0; $i -lt 30; $i++) {
    $ok = docker compose -f infra/docker-compose.yml exec -T kafka `
        kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Kafka up."
        break
    }
    Start-Sleep -Seconds 2
}

Write-Host "[infra] init topics"
python scripts/init_kafka.py

Write-Host "[infra] OK"
Write-Host "  Kafka UI : http://localhost:18080"
Write-Host "  Flink UI : http://localhost:8081"
Write-Host "  Postgres : localhost:5432 (brand/brand)"
