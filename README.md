# Tritium

Tritium is a RAM-only, zero-dependency key-value store: a small Go RPC server in
front of a RESP store (Valkey, Redis, Garnet, anything that speaks the protocol).
Nodes find each other by gossip and replicate every write into each other's
stores, so any node can answer for any key.

The [tritium-wails](https://github.com/we-be/tritium-wails) desktop client
talks to it. The design notes live in
[this gist](https://gist.github.com/hunterjsb/572f8e3b66dde9551e3fa3652f6b40b7).

## Run a cluster

Three nodes, each with its own Valkey primary and replica:

```sh
podman compose up --build        # or: docker compose up --build
go run ./cmd/tritium-monitor     # live dashboard over 8080-8082
```

Same thing on bare metal, with `valkey-server` on PATH (`brew install valkey`):

```sh
make cluster
make cluster-down
```

A single node:

```sh
valkey-server --save "" --appendonly no &
go run ./cmd/tritium             # loads .env if present; environment overrides it
```

## Use it

From the shell:

```sh
go run ./cmd/tritium-cli set -ttl 3600 hello world
go run ./cmd/tritium-cli -addr localhost:8081 get hello   # any node answers
go run ./cmd/tritium-cli nodes
```

From Go:

```go
import "github.com/we-be/tritium/pkg/tritium"

client, err := tritium.NewClient(&tritium.ClientOptions{Address: "localhost:8080"})
if err != nil {
    return err
}
defer client.Close()

err = client.Set("hello", []byte("world"), new(3600)) // TTL in seconds; nil uses the server default
value, err := client.Get("hello")                    // tritium.ErrNotFound when missing or expired
```

## Configuration

Read from `.env` (or the file given by `-config`), then overridden by the environment.

| Variable                 | Default          | Purpose                                                        |
| ------------------------ | ---------------- | -------------------------------------------------------------- |
| `SECURE_STORE_ADDRESS`   | `localhost:6379` | RESP server this node writes through                           |
| `RPC_ADDRESS`            | `localhost:8080` | Listen address for the RPC server                              |
| `ADVERTISE_ADDRESS`      | bound address    | Address peers dial; set it behind NAT or in containers          |
| `JOIN_ADDRESS`           | none             | An existing node to join; unset seeds a new cluster            |
| `MAX_SERVER_CONNECTIONS` | `4`              | Connections pooled per RESP server                             |

## How it works

Each node owns one RESP primary (replicate that however you like; the compose
file gives each one a replica). A write goes to the node's own primary with
`SETEX`, then fans out to every other node's primary. Reads hit the local
primary only. Keys always carry a TTL; the default is 17600 seconds.

Membership is gossip: a joining node pulls the cluster view from any member and
announces itself to everyone in it, then each node swaps views with a random
peer every 5 seconds. A peer silent for 10 s is degraded, for 15 s is down and
dropped from replication, and for 60 s is forgotten.

The wire protocol is Go's `net/rpc` over TCP with gob encoding. Method names
(`Store.Set`, `Store.Get`, `Store.Delete`, `Store.GetClusterNodes`) and the
types in `pkg/storage` are the compatibility surface.

## Security

- **RAM-only.** Run stores with `--save "" --appendonly no`, as the compose file
  and scripts do, and nothing ever touches disk. Every key expires.
- **Zero dependencies.** Standard library only; `go.mod` has no requirements.
- **On the wire, nothing yet.** RPC and RESP are plain TCP without
  authentication or encryption. Bind to loopback or a private network.
  Client-side payload encryption is the next step; `internal/crypto` holds the
  primitives.

## Development

```sh
make test          # unit tests against an in-process RESP fake
make integration   # same tests against a real server on localhost:6379
make lint          # gofmt, go vet, go fix
make image         # container image
```

CI runs all of the above on every push.

## TUI

![tritium-monitor](https://github.com/user-attachments/assets/2a00124f-78f7-4721-bb8b-d70bf6733446)
