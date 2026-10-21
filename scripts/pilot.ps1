$ErrorActionPreference = "Stop"
$N = 10
$basePort = 9200
$root = ".\run\pilot"
Get-Process node -ErrorAction SilentlyContinue | Stop-Process -Force
Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null

for ($i = 0; $i -lt $N; $i++) {
    $port = $basePort + $i
    $peersBlock = if ($i -eq 0) { "  peers: []" } else { "  peers:`n  - 127.0.0.1:$basePort" }
    $yaml = @"
node:
  port: $port
  state_dir: $root/node-$i
  read_timeout: 10s
  write_timeout: 10s
protocol:
  version: 1
  codec: msgpack
dht:
  k: 3
  alpha: 3
  refresh_interval: 5s
bootstrap:
  scheme: star
$peersBlock
"@
    Set-Content "$root/config-$i.yaml" $yaml -Encoding UTF8
}

$procs = @()
for ($i = 0; $i -lt $N; $i++) {
    $p = Start-Process -FilePath ".\node.exe" `
        -ArgumentList "$root/config-$i.yaml" `
        -RedirectStandardOutput "$root/logs/node-$i.out" `
        -RedirectStandardError  "$root/logs/node-$i.err" `
        -PassThru -NoNewWindow
    $procs += $p
    Start-Sleep -Milliseconds 300
}

Write-Host "Running for 40 seconds to collect refresh lookups..."
Start-Sleep -Seconds 40

foreach ($p in $procs) {
    if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force }
}
Write-Host "Done. Logs in: $root\logs"