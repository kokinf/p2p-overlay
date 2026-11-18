param (
    [int]$N = 15,
    [int]$K = 3,
    [string]$Scheme = "star",
    [string]$Strategy = "random",
    [switch]$TestFailures,
    [switch]$Netem
)

$ErrorActionPreference = "Stop"

Write-Host "Checking Docker daemon..."
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Docker is not running. Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}
Write-Host "Docker is running." -ForegroundColor Green

$runRoot = "run/experiment-" + (Get-Date -Format "yyyyMMdd-HHmmss")
$composeFile = "$runRoot/compose.yaml"
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null
Write-Host "=== Generating network ==="
.\scripts\gen_compose.ps1 -N $N -K $K -Scheme $Scheme -Strategy $Strategy -RunRoot $runRoot -OutputFile "compose-experiment.yaml"

Write-Host "=== Starting Docker Compose ==="


docker compose -p p2p-experiment -f compose-experiment.yaml up -d --build
if ($LASTEXITCODE -ne 0) { throw "Compose startup failed" }

Write-Host "=== Applying netem ==="
if ($Netem) {
    Start-Sleep -Seconds 3
    Write-Host "Applying 15% loss + 100ms delay to node-2 and node-3..."
    docker compose -p p2p-experiment -f compose-experiment.yaml exec -T node-2 tc qdisc add dev eth0 root netem delay 100ms loss 15% 2>&1 | Out-Null
    docker compose -p p2p-experiment -f compose-experiment.yaml exec -T node-3 tc qdisc add dev eth0 root netem delay 100ms loss 15% 2>&1 | Out-Null

    Write-Host "Netem state on node-2:"
    docker compose -p p2p-experiment -f compose-experiment.yaml exec -T node-2 tc qdisc show dev eth0
    Write-Host "Netem state on node-3:"
    docker compose -p p2p-experiment -f compose-experiment.yaml exec -T node-3 tc qdisc show dev eth0
}

Write-Host "=== Waiting for convergence (40s) ==="
Start-Sleep -Seconds 40

Write-Host "=== Checking DHT distribution ==="
python .\scripts\verify_results.py --dir "$runRoot/results" --n $N --k $K

if ($TestFailures) {
    Write-Host "=== Injecting failures ==="

    Write-Host "Stopping seed node-0..."
    docker compose -p p2p-experiment -f compose-experiment.yaml stop node-0
    Start-Sleep -Seconds 10

    Write-Host "Waiting 15s for refresh lookups..."
    Start-Sleep -Seconds 15

    $alive = (docker ps --filter "label=com.docker.compose.project=p2p-experiment" --format "{{.Names}}" | Measure-Object).Count
    Write-Host "Alive nodes: $alive (expected $($N-1))"

    Write-Host "Stopping active relay candidate node-5..."
    docker compose -p p2p-experiment -f compose-experiment.yaml stop node-5
    Start-Sleep -Seconds 5
}

Write-Host "=== 6. Saving Logs ==="
New-Item -ItemType Directory -Force -Path ".\run\logs" | Out-Null
for ($i = 0; $i -lt $N; $i++) {
    # Используем cmd /c для обхода ошибок PowerShell
    docker compose -p p2p-experiment -f compose-experiment.yaml logs "node-$i" 2>&1 | Set-Content "$runRoot/node-$i.log"
}
Write-Host "Logs saved to .\run\logs\"

Write-Host "=== 7. Stopping Network ==="
docker compose -p p2p-experiment -f compose-experiment.yaml down
