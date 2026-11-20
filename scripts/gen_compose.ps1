param (
  [int]$N = 15,
  [int]$K = 3,
  [string]$Scheme = "star",
  [string]$Strategy = "random",
  [string]$OutputFile = "docker-compose.yml",
  [string]$RunRoot = "run"
)

New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null
$yaml = @"
networks:
  p2p-net:
    driver: bridge

services:
"@

Write-Host "Generating $N identities..."
$ids = & ".\node.exe" gen-identities $N ".\$RunRoot\identities"
if ($ids.Count -ne $N) { throw "Failed to generate identities" }

for ($i = 0; $i -lt $N; $i++) {
  $port = 9000 + $i
  $allTargets = ""
  foreach ($offset in @(7, 11)) { $recipient = ($i + $offset) % $N; $allTargets += "        - `"$($ids[$recipient])`"`n" }
  $peers = @()

  if ($Scheme -eq "star") {
    if ($i -ne 0) { $peers += "node-0:9000" }
  }
  elseif ($Scheme -eq "ring") {
    $next = ($i + 1) % $N
    $nextPort = 9000 + $next
    $peers += "node-${next}:${nextPort}"
  }
  elseif ($Scheme -eq "tree") {
    if ($i -gt 0) {
      $parent = [math]::Floor(($i - 1) / 2)
      $parentPort = 9000 + $parent
      $peers += "node-${parent}:${parentPort}"
    }
  }
  elseif ($Scheme -eq "multi-seed") {
    if ($i -gt 0 -and $i -lt 3) { $peers += "node-0:9000" }
    if ($i -ge 3) { foreach ($seed in 0..2) { $seedPort = 9000 + $seed; $peers += "node-${seed}:${seedPort}" } }
  }

  $peersYaml = ""
  foreach ($p in $peers) {
    $peersYaml += "        - `"$p`"`n"
  }
  if ($peers.Count -eq 0) { $peersYaml = "        []" }

  $pinsYaml = ""
  foreach ($p in $peers) {
    $seedIndex = [int]([regex]::Match($p, '^node-(\d+):').Groups[1].Value)
    $pinsYaml += "    `"$p`": `"$($ids[$seedIndex])`"`n"
  }
  if ($peers.Count -eq 0) { $pinsYaml = "    {}" }
  $nodeYaml = @"

  node-${i}:
    build: .
    hostname: node-${i}
    networks:
      - p2p-net
    cap_add:
      - NET_ADMIN
    volumes:
      - ./$RunRoot/identities/node-${i}:/app/state
      - ./$RunRoot/results:/app/results
    command: ["/app/config.yaml"]
    environment:
      - GOTOOLCHAIN=local
    configs:
      - source: config_${i}
        target: /app/config.yaml

"@
  $yaml += $nodeYaml

  $cfg = @"
node:
  port: $port
  advertise_address: node-${i}:${port}
  state_dir: /app/state
  read_timeout: 10s
  write_timeout: 10s
protocol:
  version: 1
  codec: msgpack
dht:
  k: $K
  alpha: 3
  replication: 3
  ttl_seconds: 180
  refresh_interval: 15s
  republish_interval: 60s
  relay_strategy: $Strategy
bootstrap:
  scheme: $Scheme
  peers:
$peersYaml
  pins:
$pinsYaml
experiment:
  enabled: true
  delay: 20s
  output_dir: /app/results
  targets:
$allTargets
"@
  Set-Content -Path ".\$RunRoot\config_$i.yaml" -Value $cfg -Encoding UTF8
}

$yaml += "`nconfigs:`n"
for ($i = 0; $i -lt $N; $i++) {
  $yaml += "  config_${i}:`n    file: ./$RunRoot/config_${i}.yaml`n"
}

New-Item -ItemType Directory -Force -Path ".\$RunRoot\results" | Out-Null
Set-Content -Path $OutputFile -Value $yaml -Encoding UTF8
Write-Host "Generated $OutputFile for N=$N, K=$K, scheme=$Scheme, strategy=$Strategy"
