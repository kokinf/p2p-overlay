param (
    [int]$N = 15,
    [int]$K = 3,
    [string]$Scheme = "star",
    [switch]$TestFailures
)

$ErrorActionPreference = "Stop"

Write-Host "Checking Docker daemon..."
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Docker is not running. Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}
Write-Host "Docker is running." -ForegroundColor Green

Write-Host " - 1. Generating Network -"
.\scripts\gen_compose.ps1 -N $N -K $K -Scheme $Scheme

Write-Host " - 2. Starting Docker Compose -"

docker-compose down -v
Remove-Item -Recurse -Force ".\run\results\*" -ErrorAction SilentlyContinue

docker-compose up -d --build

Write-Host " - 3. Waiting for Convergence (40s) -"
Start-Sleep -Seconds 40

Write-Host " - 4. Checking Non-degeneracy -"
python .\scripts\verify_results.py --dir .\run\results --n $N --k $K

if ($TestFailures) {
    Write-Host " - 5. Failure Injection -"
    
    Write-Host "Stopping seed node-0..."
    docker stop node-0
    Start-Sleep -Seconds 10
    
    Write-Host "Waiting 15s for refresh lookups..."
    Start-Sleep -Seconds 15

    $alive = (docker ps --filter "name=node-" --format "{{.Names}}" | Measure-Object).Count
    Write-Host "Alive nodes: $alive (expected $($N-1))"

    Write-Host "Stopping active relay candidate node-5..."
    docker stop node-5
    Start-Sleep -Seconds 5
}

Write-Host " - 6. Stopping Network -"
docker-compose down