#!/usr/bin/env bash
# Stops everything start-cluster.sh started.
set -uo pipefail
cd "$(dirname "$0")"

for pidfile in logs/node*.pid; do
    [[ -f $pidfile ]] || continue
    kill "$(cat "$pidfile")" 2>/dev/null && echo "stopped $(basename "$pidfile" .pid)"
    rm -f "$pidfile"
done

CLI=$(command -v valkey-cli || command -v redis-cli || true)
for port in 6380 6382 6384 6379 6381 6383; do
    [[ -n $CLI ]] && "$CLI" -p "$port" shutdown nosave >/dev/null 2>&1 && echo "stopped store on $port"
done

rm -f node1.env node2.env node3.env
echo "cluster stopped"
