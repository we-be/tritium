#!/usr/bin/env bash
# Local three-node cluster on 8080-8082, each node with its own embedded
# store. RAM-only: nothing touches disk. STORES=valkey runs each node in front
# of a Valkey (or Redis) primary with one replica instead, the way the compose
# file does.
set -euo pipefail
umask 077 # the node env files hold nothing secret here, but the habit is the point
cd "$(dirname "$0")"

mkdir -p logs
go build -trimpath -o bin/tritium ./cmd/tritium

if [[ ${STORES:-} == valkey ]]; then
    SERVER=$(command -v valkey-server || command -v redis-server || true)
    CLI=$(command -v valkey-cli || command -v redis-cli || true)
    if [[ -z $SERVER || -z $CLI ]]; then
        echo "valkey-server and valkey-cli (or redis-*) must be on PATH; try: brew install valkey" >&2
        exit 1
    fi

    wait_ready() {
        local port=$1 n=0
        until "$CLI" -p "$port" ping >/dev/null 2>&1; do
            if (( n++ >= 30 )); then
                echo "store on port $port never answered" >&2
                exit 1
            fi
            sleep 1
        done
    }

    start_store() {
        local port=$1; shift
        "$SERVER" --port "$port" --bind 127.0.0.1 --save "" --appendonly no --daemonize yes \
            --logfile "$PWD/logs/store-$port.log" --pidfile "$PWD/logs/store-$port.pid" "$@"
        wait_ready "$port"
        echo "store 127.0.0.1:$port up"
    }

    for port in 6379 6381 6383; do
        start_store "$port"
        start_store $((port + 1)) --replicaof 127.0.0.1 "$port"
    done
fi

for i in 1 2 3; do
    rpc=$((8079 + i))
    {
        if [[ ${STORES:-} == valkey ]]; then echo "SECURE_STORE_ADDRESS=127.0.0.1:$((6379 + (i - 1) * 2))"; fi
        echo "LISTEN_ADDRESS=127.0.0.1:$rpc"
        if (( i > 1 )); then echo "JOIN_ADDRESS=127.0.0.1:8080"; fi
        echo "PEER_PASSWORD=local-cluster-demo" # every node shares it; node 1 needs it too, to answer the others' AUTH peer
    } > "node$i.env"
    ./bin/tritium -config "node$i.env" > "logs/node$i.log" 2>&1 &
    echo $! > "logs/node$i.pid"
    echo "node$i up: rpc 127.0.0.1:$rpc (pid $!)"
done

echo "monitor: go run ./cmd/tritium-monitor    stop: ./stop-cluster.sh"
