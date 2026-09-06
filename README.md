# Tritium

Tritium is a RAM-only, zero-dependency key-value store that speaks the Redis
protocol. Each node is a small Go server in front of a RESP store (Valkey,
Redis, Garnet, anything that speaks RESP). Nodes find each other by gossip and
replicate every write into each other's stores, so any node answers for any
key, and any Redis client can talk to any node.

The [tritium-wails](https://github.com/we-be/tritium-wails) desktop client
talks to it. The design notes live in
[this gist](https://gist.github.com/hunterjsb/572f8e3b66dde9551e3fa3652f6b40b7).

## Run a cluster

Three nodes, each with its own Valkey primary and replica:

```sh
podman compose up --build        # or: docker compose up --build
go run ./cmd/tritium-monitor     # live dashboard over 8080-8082; -config node.env takes addresses and passwords from a node's env file
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

Prebuilt binaries for Linux and macOS, amd64 and arm64, are on the
[releases page](https://github.com/we-be/tritium/releases); each tarball holds
`tritium`, `tritium-cli`, `tritium-monitor` and `tritium-msg`. `make dist` builds
the same set locally.

## Use it

Any Redis client works. From the shell:

```sh
valkey-cli -p 8080 set hello world EX 3600
valkey-cli -p 8081 get hello                  # any node answers
valkey-cli -p 8080 info tritium
```

From Python:

```python
import redis
r = redis.Redis(port=8080, password="change-me")
r.set("hello", b"world", ex=3600)
r.get("hello")
```

From Go, without pulling in a Redis library, and with values encrypted before
they leave the process:

```go
import "github.com/we-be/tritium/pkg/tritium"

client, err := tritium.NewClient(&tritium.ClientOptions{
    Address: "localhost:8080",
    Key:     tritium.KeyFromPassphrase("correct horse battery staple", "my-app"),
})
if err != nil {
    return err
}
defer client.Close()

err = client.Set("hello", []byte("world"), new(3600)) // TTL in seconds; nil uses the server default
value, err := client.Get("hello")                    // tritium.ErrNotFound when missing or expired
opts, err := tritium.OptionsFromEnv(".env")          // or reach the node next door from its own env file
nodes, err := client.Nodes()                         // the cluster view
```

`tritium-cli` wraps that client for the shell and adds `nodes`:

```sh
export TRITIUM_KEY=$(openssl rand -hex 32)
go run ./cmd/tritium-cli set hello world      # sealed with $TRITIUM_KEY
go run ./cmd/tritium-cli get hello            # world
valkey-cli -p 8080 get hello                  # "TE1..." ciphertext
go run ./cmd/tritium-cli nodes
```

### Commands

| Command                                     | Notes                                                   |
| ------------------------------------------- | ------------------------------------------------------- |
| `SET key value [EX seconds \| PX millis] [NX]` | Without an expiry the key gets the default TTL       |
| `SETEX key seconds value`                   |                                                         |
| `GET key`, `GETDEL key`, `MGET key [key ...]` |                                                       |
| `DEL key [key ...]`                         |                                                         |
| `EXISTS key [key ...]`                      |                                                         |
| `TTL key`                                   |                                                         |
| `EXPIRE key seconds [NX \| XX \| GT \| LT]` | Seconds must be positive; use `DEL` to remove a key   |
| `ZADD key score member [...]`               | Plain form only; the set's TTL is refreshed to the default |
| `ZRANGEBYSCORE`, `ZREM`, `ZREMRANGEBYSCORE`, `ZCARD` | Passed through; writes replicate                |
| `PING`, `ECHO`, `AUTH`, `HELLO`, `QUIT`     | RESP2 by default, RESP3 after `HELLO 3`                 |
| `INFO [section]`, `CLIENT`, `COMMAND`, `SELECT 0` | Enough for client libraries to connect cleanly    |
| `TRITIUM.NODES`                             | The cluster view as JSON                                |
| `TRITIUM.GOSSIP <node-json>`                | What nodes send each other; replies with the view       |

Every key expires; the default TTL is 17600 seconds. `XX` and `KEEPTTL` are
not supported. `NX` is decided by the node's own store, so two nodes can each
accept the same claim; treat it as first-come per node, not a global lock.

## Messenger

`pkg/messenger` is a secure one-to-one messenger on top of tritium, standard
library only. Identities are an Ed25519 signing key and an X25519 agreement
key, published as a signed bundle under `id:<name>`. A session starts with an
X3DH-style agreement, so you can message someone who is offline, and runs a
hash ratchet in each direction so every message has its own key. The signed
prekey rotates weekly and retired ones are forgotten after thirty days, so a
stolen device unlocks only sessions opened in that window. Mailboxes are named
by secrets derived from the session, so nodes can't see who is talking to
whom; messages are padded so their sizes say little; and everything expires.
A message is deleted from the server only by the read after the one that
delivered it, so a client that stores its state between reads never loses one.

```sh
go run ./cmd/tritium-msg init alice        # identity in ~/.tritium-msg, published as id:alice
go run ./cmd/tritium-msg lookup bob        # prints bob's fingerprint: compare it with bob in person
go run ./cmd/tritium-msg send bob "hey"
go run ./cmd/tritium-msg recv -watch
```

Programs use it as request and reply. `serve` prints each incoming message as
one JSON line on stdout and sends back the JSON lines it reads on stdin; `ask`
speaks from a throwaway identity, so processes never share ratchet state, and
refuses a peer whose fingerprint is not the one pinned with `-fp`:

```sh
go run ./cmd/tritium-msg serve -name bob       # stdout: {"from","fp","time","body"} per message; stdin: {"fp","body"} per reply
echo '{"q":"lunch"}' | go run ./cmd/tritium-msg ask -fp 23FK7-ISTCB-… bob   # prints bob's reply; exit 2 on no reply, 3 on a wrong pin
```

What it does not do yet: forward secrecy across a compromised device (there is
no Diffie-Hellman ratchet, only the hash ratchet), sealed sender on first
contact, groups, or multiple devices per identity. Names are first come, first
served per node; the fingerprint is the identity, the name is a convenience.

## Configuration

Read from `.env` (or the file given by `-config`), then overridden by the environment.

| Variable                 | Default          | Purpose                                                                 |
| ------------------------ | ---------------- | ----------------------------------------------------------------------- |
| `LISTEN_ADDRESS`         | `localhost:8080` | Where the node accepts clients and peers                                |
| `ADVERTISE_ADDRESS`      | bound address    | Address peers dial; set it behind NAT or in containers                  |
| `JOIN_ADDRESS`           | none             | Nodes to join, comma-separated; dialed until they answer and again whenever one drops out, so nodes boot in any order. Unset seeds a new cluster |
| `AUTH_PASSWORD`          | none             | Password clients must `AUTH` with                                       |
| `PEER_PASSWORD`          | `AUTH_PASSWORD`  | Password nodes present to each other as `AUTH peer <password>`; set it so clients can't join the cluster |
| `SECURE_STORE_ADDRESS`   | `localhost:6379` | RESP server this node writes through                                    |
| `SECURE_STORE_PASSWORD`  | none             | `AUTH` for that store and every replica                                 |
| `MAX_SERVER_CONNECTIONS` | `4`              | Connections pooled per RESP server                                      |
| `TLS_CERT`, `TLS_KEY`    | none             | Serve TLS, and dial peers with TLS presenting this certificate          |
| `TLS_CA`                 | system roots     | What peers, and clients under `TLS_CLIENT_AUTH`, must chain to          |
| `TLS_CLIENT_AUTH`        | `false`          | Require client certificates: mutual TLS for clients and between nodes   |

## How it works

Each node owns one RESP primary (replicate that however you like; the compose
file gives each one a replica). A write goes to the node's own primary with
`SETEX`, then fans out to every other node's primary. Reads hit the local
primary only.

Membership is gossip. A joining node asks any member for `TRITIUM.NODES`,
adopts the view, and announces itself to everyone in it with
`TRITIUM.GOSSIP`; after that each node swaps views with a random peer every
5 seconds over the same command. A peer silent for 10 s is degraded, for 15 s
is down and dropped from replication, and for 60 s is forgotten. Node-to-node
traffic uses the same port and TLS settings as clients, authenticated as the
`peer` user.

## Security

- **RAM-only.** Run stores with `--save "" --appendonly no`, as the compose file
  and scripts do, and nothing ever touches disk. Every key expires.
- **Zero dependencies.** Standard library only; `go.mod` has no requirements.
- **Authentication.** Set `AUTH_PASSWORD` and every client must `AUTH`. Set
  `PEER_PASSWORD` too: joining the cluster means every node starts replicating
  its writes to the newcomer's store, so membership has its own credential.
  `TRITIUM.GOSSIP` is refused to anyone not authenticated as `peer`, and under
  `TLS_CLIENT_AUTH` also to any connection without a verified certificate.
- **Encryption in transit.** Set `TLS_CERT` and `TLS_KEY`; add `TLS_CA` and
  `TLS_CLIENT_AUTH=true` for mutual TLS, which covers node-to-node traffic too.
- **Encryption at rest, end to end.** Give the Go client a `Key` and every
  value is sealed with AES-256-GCM before it leaves the process. Nodes, stores
  and the network only ever see ciphertext; key names stay in the clear and are
  bound to the ciphertext, so a value can't be replayed under another name.
  Every client sharing the data needs the same 32-byte key, from
  `tritium.ParseKey` (hex or base64) or `tritium.KeyFromPassphrase`.

The sealed format is `"TE1" || 12-byte nonce || AES-256-GCM(plaintext, aad = key name)`,
so other languages can read it. In Python with `cryptography`:

```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
ct = r.get("hello")
assert ct[:3] == b"TE1"
plaintext = AESGCM(key).decrypt(ct[3:15], ct[15:], b"hello")
```

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
