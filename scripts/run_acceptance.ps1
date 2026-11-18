param(
 [ValidateSet(0,15,21)][int]$N=0,
 [ValidateSet("all","star","ring","tree","multi-seed")][string]$Scheme="all",
 [ValidateRange(1,50)][int]$Repeats=5,
 [string]$OutputDir=("results/acceptance-"+(Get-Date -Format "yyyyMMdd-HHmmss"))
)
$ErrorActionPreference="Stop"
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$OutputDir=(Resolve-Path -LiteralPath $OutputDir).Path
function Check([string]$Program,[string[]]$Arguments,[string]$Log){
 & $Program @Arguments 2>&1 | Out-File -LiteralPath $Log -Encoding utf8
 if($LASTEXITCODE -ne 0){throw "$Program failed; see $Log"}
}
$manifest=@{started_utc=[DateTime]::UtcNow.ToString("o");go=(& go version);commit=(& git rev-parse HEAD);N=$N;scheme=$Scheme;repeats=$Repeats;ALPHA=3;R=3;mode="independent logical nodes in Go integration harness";sources=@()}
$manifest.sources=@(Get-ChildItem internal,cmd,tests -Recurse -Filter *.go | ForEach-Object {@{path=$_.FullName;sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}})
$manifest | ConvertTo-Json -Depth 6 | Set-Content "$OutputDir/manifest.json" -Encoding utf8
Check "go" @("build","-o","node.exe","./cmd/node") "$OutputDir/build.txt"
Check "go" @("vet","./...") "$OutputDir/vet.txt"
$keys=@("P2P_ACCEPTANCE","P2P_ACCEPTANCE_N","P2P_ACCEPTANCE_SCHEME","P2P_ACCEPTANCE_REPEATS","P2P_ACCEPTANCE_SMOKE","P2P_ACCEPTANCE_OUT")
$saved=@{};foreach($key in $keys){$saved[$key]=[Environment]::GetEnvironmentVariable($key,"Process");[Environment]::SetEnvironmentVariable($key,$null,"Process")}
try {
 Check "go" @("test","./...","-count=1","-timeout","240s") "$OutputDir/tests.txt"
 $env:P2P_ACCEPTANCE="1";$env:P2P_ACCEPTANCE_OUT=$OutputDir;$env:P2P_ACCEPTANCE_REPEATS="$Repeats"
 if($N -ne 0){$env:P2P_ACCEPTANCE_N="$N"};if($Scheme -ne "all"){$env:P2P_ACCEPTANCE_SCHEME=$Scheme}
 Check "go" @("test","./tests","-v","-run","TestAcceptanceMatrix","-count=1","-timeout","25m") "$OutputDir/acceptance.txt"
 Check "python" @("scripts/summarize_acceptance.py","--dir",$OutputDir,"--repeats","$Repeats") "$OutputDir/verification.txt"
 Write-Host "Acceptance passed: $OutputDir"
} finally {foreach($key in $keys){[Environment]::SetEnvironmentVariable($key,$saved[$key],"Process")}}
