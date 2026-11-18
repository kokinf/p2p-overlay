param(
 [ValidateRange(5,50)][int]$Repeats=5,
 [string]$OutputDir=("results/files-"+(Get-Date -Format "yyyyMMdd-HHmmss"))
)
$ErrorActionPreference="Stop"
if((Test-Path -LiteralPath $OutputDir) -and @(Get-ChildItem -LiteralPath $OutputDir -Force).Count -gt 0){throw "Choose a new results directory"}
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$OutputDir=(Resolve-Path -LiteralPath $OutputDir).Path
$manifest=@{started_utc=[DateTime]::UtcNow.ToString("o");repeats=$Repeats;sizes=@(1024,102400,1048576);sources=@()}
$manifest.sources=@(Get-ChildItem internal,cmd,tests -Recurse -Filter *.go | ForEach-Object {@{path=$_.FullName;sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}})
$manifest | ConvertTo-Json -Depth 6 | Set-Content "$OutputDir/manifest.json" -Encoding utf8
$previous=$env:P2P_FILE_CONFIG
try {
 Copy-Item internal/configs/files-*.yaml -Destination $OutputDir
 go version | Set-Content "$OutputDir/go-version.txt"
 go vet ./... *> "$OutputDir/vet.txt"
 if($LASTEXITCODE -ne 0){throw "vet failed"}
 go test ./... -count=1 -timeout 5m *> "$OutputDir/regression.txt"
 if($LASTEXITCODE -ne 0){throw "Regression failed; see regression.txt"}
 $env:P2P_FILE_CONFIG=(Resolve-Path internal/configs/files-enabled.yaml).Path
 go test ./tests -json -run '^Test(SendFile_|FinalizeFile_|FileServiceDisabled|FileProfiles)' -count $Repeats -timeout 15m > "$OutputDir/raw.jsonl"
 if($LASTEXITCODE -ne 0){throw "File experiment failed; see raw.jsonl"}
 python scripts/summarize_files.py --dir $OutputDir --repeats $Repeats
 if($LASTEXITCODE -ne 0){throw "Result verification failed"}
 Write-Host "File experiment passed: $OutputDir"
} finally {$env:P2P_FILE_CONFIG=$previous}
