# Tritium

Tritium is a RAM-only, zero-dependency key-value store that speaks the Redis
protocol. Each node is one static Go binary with its own in-memory store —
or, if you prefer, a small server in front of a RESP store (Valkey, Redis,
Garnet, anything that speaks RESP). Nodes find each other by gossip and
replicate every write into each other's stores, so any node answers for any
key, and any Redis client can talk to any node.

The [tritium-wails](https://github.com/we-be/tritium-wails) desktop client
talks to it. The design notes live in
[this gist](https://gist.github.com/hunterjsb/572f8e3b66dde9551e3fa3652f6b40b7);
docs beyond this file are indexed in [docs/](docs/README.md).

## Run it

A single node, nothing else to install:

```sh
go run ./cmd/tritium             # loads .env if present; environment overrides it
```

Three nodes on bare metal, each with its own embedded store:

```sh
make cluster
go run ./cmd/tritium-monitor     # live dashboard over 8080-8082 (each store read through its node); -config node.env takes addresses and passwords from a node's env file
make cluster-down
```

The same three nodes each in front of a Valkey primary and replica, the way
a node runs with an external store:

```sh
podman compose up --build        # or: docker compose up --build
```

Prebuilt binaries for Linux and macOS, amd64 and arm64, plus 32-bit ARM for a
Pi Zero, are on the [releases page](https://github.com/we-be/tritium/releases);
each tarball holds `tritium`, `tritium-cli`, `tritium-monitor`, `tritium-msg`
and `tritium-load`. `make dist` builds the same set locally, and
`brew install --formula packaging/homebrew/tritium.rb` installs a release.

## Use it

Any Redis client works. From the shell:

```sh
valkey-cli -p 8080 set hello world EX 3600
valkey-cli -p 8081 get hello                  # any node answers
valkey-cli -p 8080 info tritium
```

From Python — see `examples/python/` for a runnable version, including one
that reads back a value sealed the way the Go client seals it:

```python
import redis
r = redis.Redis(port=8080, password="change-me")
r.set("hello", b"world", ex=3600)
r.get("hello")
```

From Go, without pulling in a Redis library, and with values encrypted before
they leave the process — `examples/go/` runs this against a real node:

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

`tritium-cli` wraps that client for the shell and adds `scan`, `nodes` and `events`:

```sh
export TRITIUM_KEY=$(openssl rand -hex 32)
go run ./cmd/tritium-cli set hello world      # sealed with $TRITIUM_KEY
go run ./cmd/tritium-cli get hello            # world
valkey-cli -p 8080 get hello                  # "TE1..." ciphertext
go run ./cmd/tritium-cli scan 'hel*'          # every matching key, its type and TTL
go run ./cmd/tritium-cli del hello
go run ./cmd/tritium-cli nodes
go run ./cmd/tritium-cli events -since 1h     # this node's view of every node's cluster events
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
| `SCAN cursor [MATCH pattern] [COUNT n] [TYPE t]`, `TYPE key`, `DBSIZE` | Read the local primary, like `GET`; the cursor is opaque. `KEYS` stays unsupported — it has no cursor |
| `PING`, `ECHO`, `AUTH`, `HELLO`, `QUIT`     | RESP2 by default, RESP3 after `HELLO 3`                 |
| `INFO [section]`, `CLIENT`, `COMMAND`, `SELECT 0` | Enough for client libraries to connect cleanly    |
| `TRITIUM.NODES`                             | The cluster view as JSON                                |
| `TRITIUM.GOSSIP <node-json>`                | Peer-only. What nodes send each other; replies with the view |
| `TRITIUM.REPLICATE cmd [args...]`           | Peer-only. A write's owner fans this out to every other node's primary |
| `TRITIUM.FORWARD cmd [args...]`             | Peer-only. A write for a key this node doesn't own, sent on to the owner |
| `TRITIUM.PEERLINK <node-json>`              | Peer-only. Hands this connection to the node that answers, which serves the peer over it from then on ([docs/cloud.md](docs/cloud.md)) |
| `ACL WHOAMI`                                | Which identity the connection carries                   |

Every key expires; the default TTL is 17600 seconds. `XX` and `KEEPTTL` are
not supported. Every key has one owner among the live nodes, and its writes
are carried out there (see below), so `NX` is decided in one place: two
nodes racing the same claim get one `OK` between them.

## How a cluster works

Each node owns one RESP primary: its own, in-process, unless
`SECURE_STORE_ADDRESS` points it at an external one (replicate that however
you like; the compose file gives each one a replica). The embedded store
holds strings and sorted sets, expires keys on time, walks `SCAN` without
ever handing a key out twice, and is reached over RESP through connections
that never leave the process, so it behaves exactly like an external store
would — a node restart empties it, and the peers fill it back on rejoin. A
write is carried out by the key's owner — the live node that rendezvous
hashing picks for that key, the same on every node that agrees on the
members — which applies it to its own primary with `SETEX` and fans it out
to every other node's primary, the node that took the client's command
included, before answering. A node handed a write for a key it does not own
forwards it as `TRITIUM.FORWARD`; if the owner cannot be reached it applies
the write itself and fans it out, as every node did before ownership, and a
held or gone peer stops being picked. So the writes to one key are ordered
in one place and `NX` holds cluster-wide, except in the moment two nodes
disagree about the members — a replication timeout, not a key's lifetime.
`KEY_OWNERSHIP=off` restores local-first writes. Every string write also
carries a stamp — a hybrid clock: the millisecond, a count within it, the
node — and the embedded store applies a write only if its stamp is newer
than the key's last, keeping a tombstone after a delete so an older write
arriving late cannot bring the key back. So whatever order writes reach a
node, in that disagreement window or across a partition, every node ends
with the same value: the later write as far as the fleet's clocks agree.
Sorted sets are not stamped; their members are written independently and
apply as they come. An external store keeps no stamps, so a node in front
of one settles by arrival order, as before. Reads hit the local primary
only. A peer that stops answering is held: writes note the keys it
missed instead of waiting on it, and every 5 s the node replays them — the
current value, or the deletion — until it answers again. Every write waits
for its peers by default, so a key read from any node right after the answer
is there; over a slow link `REPLICATION=async` answers once the local store
has the write and feeds peers in order from a queue, and `tritium-load -peer`
shows the lag that buys. A peer that falls too far behind is held and
repaired like one that stopped answering.

Each node keeps its own cluster events — attach, detach, hold, repair,
stall, evict, resync, and its own start — in `tritium:events:<node id>`, a
sorted set scored by time and capped at a day and a few hundred entries so a
flapping peer can't grow it without bound. It replicates like any other key,
so `go run ./cmd/tritium-cli events [-since 1h] [-node NAME]` shows what
happened across the whole fleet from any one node, and the monitor's Recent
Events panel reads the same log.

Membership is gossip. A joining node asks any member for `TRITIUM.NODES`,
adopts the view, and announces itself to everyone in it with
`TRITIUM.GOSSIP`; after that each node swaps views with a random peer every
5 seconds over the same command. A peer silent for 10 s is degraded, for 15 s
is down and dropped from replication, and for 60 s is forgotten. Every live
peer is attached and has the other's store copied over: a peer that restarted
since it was last seen is a fresh incarnation and stale, so the survivor's keys
win there; a newcomer, or a peer back from a partition both sides lived
through, keeps what it holds and only has its gaps filled — and what it
missed while the link was down, which the other side noted while holding it,
is replayed on top, so a key updated on one side of a partition reaches the
other once it heals, and a key written on both sides ends up, on both, with
the later write by its stamp. A node whose own clock stops for
longer than 15 s (stopped, asleep, starved) knows it was the one away and
rejoins as a fresh incarnation itself. Node-to-node traffic uses
the same port and TLS settings as clients, authenticated as the `peer` user.

A node that cannot be dialed — behind NAT, on another network from the rest
of the fleet — sets `LINK_ADDRESS` instead of `JOIN_ADDRESS`: it opens the
connections itself and is served over them with `TRITIUM.PEERLINK`, so
gossip, replication, holding and repair all work unchanged over a socket it
opened rather than one that dialed it. That is how a fleet gets a node that
is always reachable — in the cloud, rather than behind a home NAT — without
a VPN. See [docs/cloud.md](docs/cloud.md) for the design, what changed on the
wire, and what it costs to run one.

## Messenger

`pkg/messenger` is a secure messenger on top of tritium, standard library
only, built from one-to-one sessions: a name can hold several devices and
belong to groups, but every message travels over a pairwise Double Ratchet,
never a shared key. Identities are an Ed25519 signing key and an X25519 agreement
key, published as a signed bundle under `id:<name>`. A session starts with an
X3DH-style agreement, so you can message someone who is offline, and runs a
Double Ratchet from there: every message has its own key, and every change of
direction mixes in fresh Diffie-Hellman, so a copied device state stops
reading the conversation as soon as the other side has answered again. The
signed prekey rotates weekly and retired ones are forgotten after thirty days.
Mailboxes are named by secrets derived from the session, so nodes can't see
who is talking to whom; first contact seals the sender's identity so a node
sees only an ephemeral key; the ratchet header is encrypted too, so a node
can't count messages per direction; messages are padded so their sizes say
little; and everything expires.
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

Names are first come, first served per node; the fingerprint is the identity,
the name is a convenience.

### Devices

A name can hold several devices. The name's own identity keeps publishing at
`id:<name>` exactly as before, so old clients keep working against a name
that has grown devices; each device is a second, ordinary identity of its
own — its own fingerprint, sessions and mailboxes — published under
`id:<name>/<device>`. What makes it a *device* rather than an unrelated name
is a certificate: the name's identity signs the device's long-term keys, so
nobody can attach a device to a name they don't hold. `send` fans out to the
primary and every certified device, each over its own pairwise session; a
message delivered to one device is not visible on another, since there is
no state shared between them — the worker end of this is meant to pin the
list of device fingerprints it expects, the same way it already pins one.

```sh
go run ./cmd/tritium-msg -state ~/.tritium-msg-phone init bob/phone   # the device publishes itself first
go run ./cmd/tritium-msg -state ~/.tritium-msg-phone me                # its fingerprint, read on the device
go run ./cmd/tritium-msg device authorize phone <FINGERPRINT>          # run as bob: certifies bob/phone onto bob
go run ./cmd/tritium-msg device list               # certified devices and their fingerprints
go run ./cmd/tritium-msg send bob "hey"            # reaches bob's primary identity and bob/phone
go run ./cmd/tritium-msg send -fp <FP> bob -file secrets.env   # a secret: pinned to bob's fingerprint, read from a file, never on the command line
```

The device roster shares the bundle's TTL and is refreshed whenever the name
republishes. A roster's version only moves forward on any client that has
seen a newer one, so a copy replayed into the store cannot bring back a
device, or a group member, since removed.

### Groups

A group is a roster its creator signs — member names and a version — published
under `grp:<name>`, first come like any name. There is no group key: a group
send is a pairwise send of the same body to every member (and each of their
devices) over the ordinary sessions, with the group's name folded into the
encrypted plaintext so a receiver's `Receive` can attribute it — after
checking the claim against the group's signed roster, never on the sender's
say-so alone. Only the creator can add or remove members.

```sh
go run ./cmd/tritium-msg group create book-club bob carol
go run ./cmd/tritium-msg group send book-club "meeting friday"
go run ./cmd/tritium-msg group add book-club dave     # creator only
go run ./cmd/tritium-msg group list book-club
```

## Configuration

Read from `.env` (or the file given by `-config`), then overridden by the environment.

| Variable                 | Default          | Purpose                                                                 |
| ------------------------ | ---------------- | ----------------------------------------------------------------------- |
| `LISTEN_ADDRESS`         | `localhost:8080` | Where the node accepts clients and peers                                |
| `ADVERTISE_ADDRESS`      | bound address    | Address peers dial; set it behind NAT or in containers                  |
| `JOIN_ADDRESS`           | none             | Nodes to join, comma-separated; dialed until they answer and again whenever one drops out, so nodes boot in any order. Unset seeds a new cluster |
| `LINK_ADDRESS`           | none             | Peers that cannot dial us back: joined like `JOIN_ADDRESS`, but we open the connections and are served over them ([docs/cloud.md](docs/cloud.md)) |
| `AUTH_PASSWORD`          | none             | Password clients must `AUTH` with                                       |
| `USER_<name>`            | none             | `<password>:<rights>` — a client with only the key prefixes it names, e.g. `pw:rw:node:gateway,sig:;r:board:` |
| `USERS_FILE`             | none             | A file of those entries, one per line, with the bare name on the left of the `=` |
| `AUTH_USER`              | none             | Which of them a client next to this node (`-config`) authenticates as     |
| `PEER_PASSWORD`          | `AUTH_PASSWORD`  | Password nodes present to each other as `AUTH peer <password>`; set it so clients can't join the cluster |
| `SECURE_STORE_ADDRESS`   | none             | RESP server this node writes through; unset, the node runs its own store in-process |
| `SECURE_STORE_PASSWORD`  | none             | `AUTH` for that store and every replica                                 |
| `STORE_MAX_MEMORY`       | none             | Bytes the embedded store keeps (`256M`, `1G`); past it the soonest-expiring keys are evicted, and a write with nothing left to evict is refused |
| `MAX_SERVER_CONNECTIONS` | `4`              | Connections pooled per RESP server                                      |
| `MAX_CLIENTS`            | `10000`          | Connections a node accepts at once; more are turned away with an error. A connection that has not authenticated within 10 s, or is refused five `AUTH`s, is closed |
| `KEY_OWNERSHIP`          | `on`             | Each key's writes go through its owner node, so `NX` and write order hold cluster-wide; `off` writes locally first and fans out from there |
| `REPLICATION`            | `sync`           | `sync`: a write is answered once every peer has it. `async`: answered once this node's store has it; peers are fed in order from a queue |
| `TLS_CERT`, `TLS_KEY`    | none             | Serve TLS, and dial peers with TLS presenting this certificate          |
| `TLS_CA`                 | system roots     | What peers, and clients under `TLS_CLIENT_AUTH`, must chain to          |
| `TLS_CLIENT_AUTH`        | `false`          | Require client certificates: mutual TLS for clients and between nodes   |

## Security

- **RAM-only.** The embedded store never touches disk; run an external one
  with `--save "" --appendonly no`, as the compose file does. Every key expires.
- **Zero dependencies.** Standard library only; `go.mod` has no requirements.
- **Authentication.** Set `AUTH_PASSWORD` and every client must `AUTH`. Set
  `PEER_PASSWORD` too: joining the cluster means every node starts replicating
  its writes to the newcomer's store, so membership has its own credential.
  `TRITIUM.GOSSIP` is refused to anyone not authenticated as `peer`, and under
  `TLS_CLIENT_AUTH` also to any connection without a verified certificate.
- **Users with fewer rights.** `USER_<name>=<password>:<rights>` configures a
  client that may only touch the key prefixes it names — `AUTH <name>
  <password>`, `NOPERM` outside them, never a peer command, and never a command
  whose keys the node cannot locate. It is how a credential lives somewhere the
  node's own password should not. See [docs/cloud.md](docs/cloud.md).
- **Encryption in transit.** Set `TLS_CERT` and `TLS_KEY`; add `TLS_CA` and
  `TLS_CLIENT_AUTH=true` for mutual TLS, which covers node-to-node traffic too.
- **Encryption at rest, end to end.** Give the Go client a `Key` and every
  value is sealed with AES-256-GCM before it leaves the process. Nodes, stores
  and the network only ever see ciphertext; key names stay in the clear and are
  bound to the ciphertext, so a value can't be replayed under another name.
  Every client sharing the data needs the same 32-byte key, from
  `tritium.ParseKey` (hex or base64) or `tritium.KeyFromPassphrase`.

The sealed format is `"TE1" || 12-byte nonce || AES-256-GCM(plaintext, aad = key name)`,
so other languages can read it — `examples/python/sealed.py` is a runnable version of this:

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
```

CI runs all of the above, plus a container build, on every push. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow, including the
zero-dependency rule and `pkg/tritium`'s compatibility expectations.

## TUI

![tritium-monitor](https://github.com/user-attachments/assets/2a00124f-78f7-4721-bb8b-d70bf6733446)
