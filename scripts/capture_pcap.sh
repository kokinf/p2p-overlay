#!/usr/bin/env bash
set -euo pipefail

IFACE="${1:-lo}"
PORT="${2:-9000}"
DURATION="${3:-10}"
OUT="${4:-results/capture.pcap}"

mkdir -p "$(dirname "$OUT")"
sudo tcpdump -i "$IFACE" -w "$OUT" "tcp port $PORT" &
TCPDUMP_PID=$!
sleep "$DURATION"
sudo kill -INT "$TCPDUMP_PID" || true
wait "$TCPDUMP_PID" 2>/dev/null || true

echo "Saved to $OUT"
echo "Check for plaintext:"
tshark -r "$OUT" -T fields -e tcp.payload 2>/dev/null | grep -c "hello-secret" || echo "0 occurrences (good)"