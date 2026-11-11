param (
    [int]$N = 15,
    [int]$K = 3,
    [string]$Scheme = "star", # star, ring, tree, multi-seed
    [string]$OutputFile = "docker-compose.yml"
)

$yaml = @"
networks:
  p2p-net:
    driver: bridge

services:
"@

Write-Host "Generating $N identities..."
$ids = & ".\node.exe" gen-identities $N ".\run\identities"
if ($ids.Count -ne $N) { throw "Failed to generate identities" }

$allTargets = ""
for ($j = 0; $j -lt $N; $j++) {
    $allTargets += "        - `"$($ids[$j])`"`n"
}

for ($i = 0; $i -lt $N; $i++) {
    $port = 9000 + $i
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
        if ($i -ge 3) {
            $seed = $i % 3
            $seedPort = 9000 + $seed
            $peers += "node-${seed}:${seedPort}"
        }
    }

    $peersYaml = ""
    foreach ($p in $peers) {
        $peersYaml += "        - `"$p`"`n"
    }
    if ($peers.Count -eq 0) { $peersYaml = "        []" }

    $nodeYaml = @"

  node-${i}:
    build: .
    container_name: node-${i}
    hostname: node-${i}
    networks:
      - p2p-net
    volumes:
      - ./run/identities/node-${i}:/app/state
      - ./run/results:/app/results
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
bootstrap:
  scheme: $Scheme
  peers:
$peersYaml
experiment:
  enabled: true
  delay: 20s
  output_dir: /app/results
  targets:
$allTargets
"@
    Set-Content -Path ".\run\config_$i.yaml" -Value $cfg -Encoding UTF8
}

$yaml += "`nconfigs:`n"
for ($i = 0; $i -lt $N; $i++) {
    $yaml += "  config_${i}:`n    file: ./run/config_${i}.yaml`n"
}

New-Item -ItemType Directory -Force -Path ".\run\results" | Out-Null
Set-Content -Path $OutputFile -Value $yaml -Encoding UTF8
Write-Host "Generated $OutputFile for N=$N, K=$K, scheme=$Scheme"