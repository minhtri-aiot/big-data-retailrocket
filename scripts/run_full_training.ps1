param([string]$RunId=('full-'+(Get-Date -Format 'yyyyMMdd-HHmmss')), [switch]$Resume)
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    docker version --format '{{.Server.Version}}'
    if($LASTEXITCODE -ne 0){throw 'Docker Linux engine is required'}
    docker build -t retailrocket-phase17:1.0 .
    if($LASTEXITCODE -ne 0){throw 'Image build failed'}
    docker compose up -d --wait namenode datanode mongodb redis
    if($LASTEXITCODE -ne 0){throw 'Infrastructure is not healthy'}
    $resumeArguments=@()
    if($Resume){$resumeArguments+= '--resume'}
    docker compose run --rm --no-deps `
      -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 3g --conf spark.driver.maxResultSize=512m --conf spark.local.dir=/tmp/full_spark_scratch pyspark-shell' `
      -e GRAPH_EVAL_MAX_QUERIES=5000 `
      app python -m pipeline.full_train --run-id $RunId @resumeArguments
    if($LASTEXITCODE -ne 0){throw "Training failed; inspect outputs/runs/$RunId/metrics/run_report.json"}
    docker compose up -d --no-deps --force-recreate --wait api
    if($LASTEXITCODE -ne 0){throw 'API startup failed'}
    docker compose run --rm --no-deps app python -m scripts.verify_graph --run-id $RunId --base-url http://api:8000
    if($LASTEXITCODE -ne 0){throw 'Full training/API verification failed'}
    Write-Host "Full source trained. Dashboard http://localhost:8000 | Run $RunId"
} finally {Pop-Location}
