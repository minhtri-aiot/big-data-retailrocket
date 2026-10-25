param([int]$Port=8000,[switch]$SkipBuild)
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    if($Port -lt 1 -or $Port -gt 65535){throw 'Invalid port'}
    $env:APP_PORT=[string]$Port
    if(!$SkipBuild){
        docker build -t retailrocket-graph:unified .
        if($LASTEXITCODE -ne 0){throw 'Image build failed'}
    }
    docker run --rm -v "${PWD}/outputs:/app/outputs" retailrocket-graph:unified python -m scripts.setup_graph_demo
    if($LASTEXITCODE -ne 0){throw 'Graph snapshot validation/install failed. Preserve your old outputs folder before trying again.'}
    docker compose -f compose.graph-demo.yaml up -d --no-build --wait
    if($LASTEXITCODE -ne 0){throw 'Graph API startup failed'}
    Write-Host "Graph demo: http://localhost:$Port | API: http://localhost:$Port/docs"
} finally {Pop-Location}
