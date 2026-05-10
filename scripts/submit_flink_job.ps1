# Submit the PyFlink job to the local cluster.
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot/..")

$Parallelism = if ($env:FLINK_PARALLELISM) { $env:FLINK_PARALLELISM } else { "4" }
$JobManager  = if ($env:FLINK_JOBMANAGER)  { $env:FLINK_JOBMANAGER }  else { "localhost:8081" }

docker compose -f infra/docker-compose.yml exec -T jobmanager bash -lc @"
flink run -d -p $Parallelism -m $JobManager \
  --pyModule flink_jobs.brand_crisis_job \
  --pyFiles /opt/flink/usrlib/flink_jobs,/opt/flink/usrlib/model_artifacts,/opt/flink/usrlib/config
"@

Write-Host "[flink] job submitted; check $JobManager"
