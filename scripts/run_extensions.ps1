param([string]$OutputDir=("results/extensions-"+(Get-Date -Format "yyyyMMdd-HHmmss")))
$ErrorActionPreference="Stop"
if(Test-Path -LiteralPath $OutputDir){throw "Choose a new results directory"}
New-Item -ItemType Directory -Path $OutputDir | Out-Null
$root=(Resolve-Path -LiteralPath $OutputDir).Path
./scripts/run_file_experiment.ps1 -OutputDir "$root/files"
$previous=$env:P2P_RELAY_RESULTS
try {
 $env:P2P_RELAY_RESULTS="$root/relay"
 go test ./internal/node -v -run '^Test(RelayProfileSnapshot|RelayRatingSelection|SubnetConcentrationExperiment)$' -count=1 *> "$root/relay.txt"
 if($LASTEXITCODE -ne 0){throw "Relay validation failed"}
} finally {$env:P2P_RELAY_RESULTS=$previous}
./scripts/run_network_lab.ps1 -OutputDir "$root/network"
Write-Host "Extensions validated: $root"
