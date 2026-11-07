$ErrorActionPreference = "Stop"

$N = 15
$basePort = 9200
$root = ".\run\pilot_adv"
$resultsDir = ".\results"

Get-Process node -ErrorAction SilentlyContinue | Stop-Process -Force
Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "$resultsDir\pilot_adv" -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
New-Item -ItemType Directory -Force -Path "$resultsDir\pilot_adv" | Out-Null

Write-Host "Generating $N identities..."
$nodeIDs = @(& ".\node.exe" gen-identities $N "$root\identities")
if ($nodeIDs.Count -ne $N) {
    throw "Expected $N node IDs, got $($nodeIDs.Count)"
}
$nodeIDs | ForEach-Object { Write-Host "  node: $_" }

for ($i = 0; $i -lt $N; $i++) {
    $port = $basePort + $i
    $stateDir = "$root\identities\node-$i"

    if ($i -eq 0) {
        $peersBlock = "  peers: []"
    }
    else {
        $peersBlock = "  peers:`n  - 127.0.0.1:$basePort"
    }

    $targetsBlock = ""
    for ($j = 0; $j -lt $N; $j++) {
        if ($j -eq $i) { continue }
        $targetsBlock += "  - $($nodeIDs[$j])`n"
    }

    $yaml = @"
node:
  port: $port
  state_dir: $stateDir
  read_timeout: 10s
  write_timeout: 10s
protocol:
  version: 1
  codec: msgpack
dht:
  k: 3
  alpha: 3
  replication: 3
  ttl_seconds: 180
  refresh_interval: 15s
  republish_interval: 60s
bootstrap:
  scheme: star
$peersBlock
experiment:
  enabled: true
  delay: 20s
  output_dir: "$resultsDir\pilot_adv"
  targets:
$targetsBlock
"@
    Set-Content "$root\config-$i.yaml" $yaml -Encoding UTF8
}

$procs = @()
for ($i = 0; $i -lt $N; $i++) {
    $p = Start-Process -FilePath ".\node.exe" `
        -ArgumentList "$root\config-$i.yaml" `
        -RedirectStandardOutput "$root\logs\node-$i.out" `
        -RedirectStandardError  "$root\logs\node-$i.err" `
        -PassThru -NoNewWindow
    $procs += $p
    Start-Sleep -Milliseconds 300
}

Write-Host "Running 60s: bootstrap + convergence + experiment lookups..."
Start-Sleep -Seconds 60

foreach ($p in $procs) {
    if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force }
}

Write-Host "Building summary..."
$rows = @()
Get-ChildItem "$resultsDir\pilot_adv\metrics-*.json" | ForEach-Object {
    $m = Get-Content $_.FullName -Raw | ConvertFrom-Json
    foreach ($lk in $m.lookups) {
        $initialContains = $false
        foreach ($id in $lk.initial_closest) {
            if ($id -eq $lk.target) { $initialContains = $true }
        }
        $rows += [pscustomobject]@{
            Node       = $m.node_id.Substring(0, 8)
            Target     = $lk.target.Substring(0, 8)
            Iterations = $lk.iterations
            RPCs       = $lk.rpcs
            DurationMS = $lk.duration_ms
            InitialHit = $initialContains
            NumBuckets = $m.buckets.Count
        }
    }
}
$rows | Export-Csv "$resultsDir\pilot_adv\summary.csv" -NoTypeInformation

$multiHop = $rows | Where-Object { $_.Iterations -ge 2 -and -not $_.InitialHit }
$multiHop | Measure-Object | ForEach-Object { Write-Host "Multi-hop lookups: $($_.Count)" }
$rows | Measure-Object -Property Iterations -Average -Maximum -Minimum | Format-Table

Write-Host "Done. Logs: $root\logs; metrics: $resultsDir\pilot_adv"