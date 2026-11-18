param([string]$OutputDir = ("results/network-lab-" + (Get-Date -Format "yyyyMMdd-HHmmss")))
$ErrorActionPreference = "Stop"
if ((Test-Path -LiteralPath $OutputDir) -and @(Get-ChildItem -LiteralPath $OutputDir -Force).Count -gt 0) { throw "Choose a new results directory" }
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$root = (Resolve-Path $OutputDir).Path
$stateRoot = Join-Path (Get-Location) ("run/network-lab-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $stateRoot | Out-Null
function Check { if ($LASTEXITCODE -ne 0) { throw "External command failed" } }
go build -o node.exe ./cmd/node; Check
docker build -t p2p-network-lab:local . *> "$root/build.txt"; Check
$image = (& docker image inspect --format '{{.Id}}' p2p-network-lab:local); Check
$manifest = @{image = $image; started_utc = [DateTime]::UtcNow.ToString("o"); N = 7; K = 4; ALPHA = 3; R = 3; netem_delay_ms = 80; netem_loss_percent = 2; sources = @() }
$manifest.sources = @(Get-ChildItem internal, cmd -Recurse -Filter *.go | ForEach-Object { @{path = $_.FullName; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash } })
$manifest | ConvertTo-Json -Depth 6 | Set-Content "$root/manifest.json" -Encoding utf8
& ./node.exe gen-identities 7 "$stateRoot/base" | Set-Content "$root/ids.txt"; Check
foreach ($mode in @("baseline-random", "baseline-rating", "degraded-random", "degraded-rating", "restricted-random")) {
    $dir = Join-Path $root $mode
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Copy-Item "$root/ids.txt" "$dir/ids.txt"
    $modeState = Join-Path $stateRoot $mode
    New-Item -ItemType Directory -Force -Path $modeState | Out-Null
    Copy-Item "$stateRoot/base/*" $modeState -Recurse
    $strategy = ($mode -split '-')[1]
    $argsGen = @("scripts/generate_network_lab.py", "--root", $dir, "--strategy", $strategy, "--state-root", $modeState, "--image", $image)
    if ($mode.StartsWith("restricted")) { $argsGen += "--restricted" }
    python @argsGen; Check
    $compose = "$dir/compose.json"; $project = "overlay-lab-" + (Get-Random -Minimum 10000 -Maximum 99999)
    try {
        docker compose -p $project -f $compose up -d *> "$dir/start.txt"; Check
        Start-Sleep -Seconds 8
        if ($mode.StartsWith("degraded")) {
            foreach ($i in @(1, 2)) {
                docker compose -p $project -f $compose exec -T "node-$i" tc qdisc replace dev eth0 root netem delay 80ms loss 2% *> "$dir/netem-$i.txt"; Check
                docker compose -p $project -f $compose exec -T "node-$i" tc -s qdisc show dev eth0 >> "$dir/netem-$i.txt"; Check
            }
        }
        if ($mode.StartsWith("restricted")) {
            docker compose -p $project -f $compose exec -T node-0 /app/node probe 172.29.211.14:9004 *> "$dir/reachable.txt"; Check
            docker compose -p $project -f $compose exec -T node-0 /app/node probe 172.29.212.16:9006 *> "$dir/blocked.txt"
            if ($LASTEXITCODE -eq 0) { throw "Restricted pair unexpectedly reachable" }
            docker compose -p $project -f $compose exec -T node-4 /app/node probe 172.29.212.16:9006 *> "$dir/bridge-reachable.txt"; Check
        }
        $deadline = [DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 5
            $logs = docker compose -p $project -f $compose logs --no-color node-0 2>&1
            $logs | Set-Content "$dir/node-0.log"
            $samples = @($logs | Select-String '\[EXPERIMENT\].*operation=message').Count
            if ([DateTime]::UtcNow -gt $deadline) { throw "Experiment deadline exceeded" }
        } while ($samples -lt 6)
        docker compose -p $project -f $compose logs --no-color *> "$dir/all.log"; Check
        foreach ($i in @(1, 2)) { docker compose -p $project -f $compose exec -T "node-$i" tc -s qdisc show dev eth0 *> "$dir/qdisc-$i.txt"; Check }
    }
    finally { docker compose -p $project -f $compose down *> "$dir/stop.txt" }
}
python scripts/summarize_network_lab.py --dir $root; Check
