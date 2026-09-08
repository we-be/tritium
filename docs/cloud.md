# A cloud node, and clients that get less than a node

Status: design + what is built. Read `README.md` first.

Tritium's fleet is a handful of machines on one LAN. Two things break when a
member is not on that LAN:

1. **Gossip assumes every node can dial every advertised address.** The home
   machines sit behind NAT under mDNS names (`bazzite.local`); they can reach a
   public address, nothing can reach them.
2. **There is one client credential.** `AUTH_PASSWORD` opens the whole
   keyspace, `PEER_PASSWORD` opens the cluster itself. Neither belongs in a
   Lambda.

This document answers both, then says what it costs to run.

---

## 1. Connectivity: the peer opens the connection

### The options

| | What it is | Why not |
|---|---|---|
| **Overlay** (WireGuard, Tailscale) | Put every node on one virtual L2 | A dependency, and Lambda cannot join it without a VPC — a NAT gateway is ~$32/mo, six times the node itself |
| **One-way to the cloud** | Home nodes replicate up; everyone reads from the cloud | The plane stops being one plane: a `sig:gateway` written at home never arrives, and the cloud's writes never come home |
| **Reverse channel** ← chosen | The node that can dial opens the connections and is *served* over them | One new command, no new dependency, no change to any existing wire format |

### How it works

A node that cannot be dialed names its reachable peer in `LINK_ADDRESS`:

```sh
LINK_ADDRESS=tritium.example.com:8080     # comma-separated, and joined like JOIN_ADDRESS
```

It then keeps eight connections open to that peer. On each one it authenticates
as the peer user and sends the new command:

```
TRITIUM.PEERLINK <node-json>      → +OK
```

Past that `+OK` **the two ends swap roles on that socket**. The node that dialed
stops being a client and serves the connection; the node that answered parks it
and, from then on, sends its fan-out and its repairs down a connection it never
opened. `internal/server/peering.go` holds both halves: `links` parks inbound
connections by the address their owner advertises, and `dialPeer` takes one from
there instead of opening a socket to a name that does not resolve. A parked
connection is watched, and leaves the park the moment its peer closes it, so an
attach or a fan-out after a home node restarts never takes a dead one.

Two things fall out for free, because a parked connection is an ordinary peer
connection:

- **Authentication is symmetric.** The replication pool sends `AUTH peer <pw>`
  on every fresh connection, parked ones included — so the home node
  authenticates the cloud node on the reverse channel exactly as the cloud node
  authenticated it on the way in. Under `TLS_CLIENT_AUTH` the home node is
  verifying the certificate the cloud node presented, which is the right check.
- **Outages take the path they already took.** A linked peer that stops
  answering is *held*, its missed keys noted, its backlog *parked by address*
  when it is detached, and replayed when it links again. That machinery
  (`pkg/storage/store.go`) needed no changes at all.

The one thing the cloud node must not do is dial a linked peer for gossip:
`cluster.dialable` skips them, and they gossip *to* the cloud on their own
five-second schedule. Their inbound gossip refreshes their `LastSeen` there, so
health, degradation and eviction all work unchanged.

### What changed on the wire

**One new command, `TRITIUM.PEERLINK <node-json>`, peer-only.** Nothing else.
`TRITIUM.GOSSIP` and `TRITIUM.REPLICATE` carry exactly the bytes they carried
before, and `NodeInfo` gained no field — the parked-connection registry *is* the
record that a peer is reverse-reachable.

Rollout is therefore not lockstep. An old node asked to `PEERLINK` answers `ERR
unknown command`, and the linking node logs it and retries; an old node that
links to a new one is simply a node with no `LINK_ADDRESS`. **Both fleet nodes
must roll together only to get the reverse path**, never to keep working.

### What this does not do

**The cloud node is not a relay.** A write arrives as `TRITIUM.REPLICATE` and is
applied to the local store only — it is never fanned out again, because the
sender already sent it to everyone it knows. So two home nodes reach each other
directly or not at all. On Hunter's fleet they are on one LAN and do; a future
node on a third network would not.

Making the cloud node relay needs the sender's identity on the wire (a `via`
argument on `TRITIUM.REPLICATE`, so a relayed write cannot echo back to the node
that made it and cannot clobber a newer local write on the way). That is a real
wire change that must roll to both fleet nodes together, which is why it is a
separate item and not this one. It is in `BACKLOG.md`.

---

## 2. Permission: users with key prefixes

A node may now configure clients that get strictly less than `AUTH_PASSWORD`:

```sh
USER_gateway=<password>:rw:node:gateway,sig:gateway,sig:gateway:;r:board:,fleet,node:,id:
```

The shape is `USER_<name>=<password>:<rights>`, where rights is `;`-separated
clauses of `<r|w|rw>:<key-or-prefix>[,...]`. An entry ending in `:` or `/`
covers everything under it; any other names one key exactly, so `fleet`
grants that one key, `sig:gateway` the signal index and `sig:gateway:` the
entries under it. In the process environment the entry is
`TRITIUM_USER_<name>`, so `USER_ID` and its friends are never mistaken for
one. A password may not contain `:`.

The same entries can live in a file a human edits, one per line, with the bare
name on the left of the `=`:

```sh
# /etc/tritium/users        (USERS_FILE=/etc/tritium/users)
gateway=<password>:rw:node:gateway,sig:gateway;r:board:,fleet,node:,id:
chatbot=<password>:rw:hello:,mbx:,msg:;r:id:
```

Then `AUTH gateway <password>`, and `ACL WHOAMI` says which identity a
connection carries.

### The rules

- **Deny by default.** `internal/server/acl.go` knows where each command's keys
  are. A command that is not in that table and not in the short list of
  key-less ones (`PING`, `ECHO`, `INFO`, `CLIENT`, `COMMAND`, `SELECT`,
  `TRITIUM.NODES`, `ACL`) is `NOPERM` for a user. A command added later — `SCAN`
  is landing this same wave — is refused to users until someone decides how it
  is scoped. That is the safe direction: `SCAN` enumerates the keyspace and has
  no prefix to check.
- **Every key is checked.** `DEL a b` needs write on both; `MGET` needs read on
  all of them.
- **Never a node.** `TRITIUM.GOSSIP`, `TRITIUM.REPLICATE` and `TRITIUM.PEERLINK`
  are refused to a user however the node is configured, including a node with no
  passwords at all.
- **`default` and `peer` are unchanged.** They carry no restrictions, and a
  `USER_default` or `USER_peer` is a configuration error.

### What the messenger needs

`pkg/messenger` touches exactly four prefixes, which is what makes it grantable:

| Key | Command | Rights |
|---|---|---|
| `id:<name>` | `SET` on publish, `GET` on lookup | write its own name, read the ones it talks to |
| `hello:<hash>` | `ZADD`/`ZRANGEBYSCORE`/`ZREM`/`EXPIRE` — first contact | `rw` |
| `mbx:<hash>` | the same, per session | `rw` |
| `msg:<mailbox>:<id>` | `SET`/`MGET`/`DEL` — the envelopes | `rw` |

So a messenger user is `rw:hello:,mbx:,msg:;r:id:`, plus `w:id:<its own name>`
if it publishes a bundle. An asker that only sends (`tritium-msg ask`) does not
publish: its bundle rides inside the sealed hello.

`pkg/tritium.ClientOptions` gained a `User` field (backward compatible — empty
is the old `AUTH <password>`), and `AUTH_USER` in a node's env file makes
`OptionsFromEnv` — so `tritium-cli` and `tritium-msg` — authenticate as one.

---

## 3. Replication across the WAN

**Both ends of a link feed the other from a queue, on their own.** A home node
left to wait on the cloud node pays a WAN round trip on every `SET`, including
the ones going to the machine next to it: measured on the fleet at v0.15.0,
where nothing had set `async`, SET p50 was 30 ms against 5.9 ms on wifi alone.
So since v0.16.0 a peer named in `LINK_ADDRESS`, and on the cloud node a peer
that linked in, is fed in order from a queue whatever `REPLICATION` says, while
the machines beside each other still wait on each other. `REPLICATION=async`
goes further: it answers as soon as the local store has the write and feeds
every peer from a queue.

What that costs: a moment in which a key written on one node is not yet on
another. Nothing on the fleet reads its own write from a different node —
presence is written by each node about itself and read locally, signals are
polled once a second, boards are a week of history, and the messenger's asker
and responder each poll their own node. The one thing it would break is a client
that writes on node A and immediately reads on node B; mubs has none.

**Presence and the standby gate.** The beat is 60 s and `node:<id>` lives 90 s,
against a replication lag of milliseconds — the TTLs need no change. What *does*
matter is that the gateway's presence record carries an explicit `role`:
`mubslib`'s fleet reader treats a *missing* role as primary, so a gateway that
published `{caps, ts}` and nothing else would look like a primary worker to a
standby machine's gate and stop it from taking the queue. `role: "cloud"` is
therefore not cosmetic. The plane stays what it was: a positive answer only, and
DynamoDB the authority whenever it shows no primary.

**An hour with the home link down.** The cloud node holds both home replicas
within 2 s (a failed write to a replica holds it rather than waiting out the
deadline), evicts them from its view after 60 s of silence, and parks their
backlogs by address. `node:bazzite` and `node:macair` expire off the cloud node
90 s after the last beat, so the gateway correctly sees an empty fleet and
drains the queue itself. Meanwhile each home node holds the cloud node the same
way and keeps serving its LAN peer. When the link returns the home nodes
re-`PEERLINK`, the cloud re-attaches with the parked backlog inherited, and a
resync fills the rest — overwriting if the peer restarted, filling only gaps if
it merely lost the link. A backlog past 10 000 keys becomes a full copy instead
of a replay. None of this is new; the reverse channel just rides it.

---

## 4. The mubs side

Written up in mubs' own `docs/tritium.md` (section "The cloud node and the
gateway"). In short: `mubslib.tritium.Client` takes a user, `from_cloud()` builds
one from Lambda env, and the gateway publishes `node:gateway` and drains
`sig:gateway` on each invocation it does work, behind an env gate.

**The bridge from a Lambda is not built, and here is what it would take.**
`mubslib.bridge` shells out to `tritium-msg`, which does not exist in a Lambda:

1. Ship `tritium-msg` for the function's architecture in the mubslib layer.
   `make dist` already builds `linux/amd64` and `linux/arm64`; the binary is a
   few MB and the layer has room.
2. Give it a config. It reads a node env file through `-config`; a Lambda has
   none. Either write one to `/tmp` on cold start from the env vars, or add
   `-addr`/`-user`/`-password`/`-ca` flags. The `User` field this wave added to
   `ClientOptions` is the part that was missing.
3. Grant the messenger user `rw:hello:,mbx:,msg:;r:id:` on the cloud node.
4. Keep the identity in `/tmp` — an asker's identity is throwaway per request,
   so a cold start costs one key generation.
5. Pin the answering node's fingerprint the way the desktop does
   (`MUBS_BRIDGE_PEER=macair:<FP>`), from Lambda env.

Cost is one exec (~50 ms) plus the messenger's own floor (~1 s: a key agreement,
two store round trips each way, and the responder's half-second poll). **Do not
port the messenger to Python** — a second implementation of a Double Ratchet is
a second place for it to be wrong.

---

## 5. Hosting and cost

The node is one static Go binary and an env file. Since the embedded store
landed this same wave there is no Valkey to run beside it, and nothing on disk:
a 512 MB box is ample.

| Option | Monthly | Notes |
|---|---|---|
| **Lightsail nano, us-east-1** | **$5.00** | 512 MB / 2 vCPU / 20 GB SSD / 1 TB transfer, static IPv4 included. Same account, same bill, one resource |
| EC2 t4g.nano on demand | ~$7.30 | $3.07 instance + $0.64 for 8 GB gp3 + $3.60 for the IPv4 address. ~$6.10 with a 1-year no-upfront Savings Plan |
| Fly.io shared-cpu-1x 256 MB | ~$2–4 | Cheapest that works, but a second vendor and a machine that moves under you |
| Hetzner CX22 | ~$4.10 | Best hardware per dollar by far, but EU — ~90 ms to a us-east-1 Lambda, and a second vendor |
| Oracle Always Free arm64 | $0 | Hunter's last Oracle host expired out from under him (xn-mc). No |

**Recommendation: Lightsail nano, $5.00/mo.** Latency to the Lambda is the
argument as much as the price — the gateway pays the round trip on every
invocation, and same-region is sub-millisecond. Add $0.40/mo for one Secrets
Manager secret and the total is **$5.40/mo, about $65 a year**. Data transfer is
nowhere near the included terabyte: presence is a few hundred bytes a minute per
node.

---

## 6. The infra proposal — Hunter's call, not applied

Everything below is `infra/` (Pulumi/Go), which agents do not touch. This is the
diff to make, not a diff that was made. **Nothing was provisioned, and no
credential was created or read.**

### a. The box

Lightsail instance `mubs-tritium`, us-east-1, blueprint `debian_12`, bundle
`nano_3_0`, static IP attached. Firewall: **TCP 8080 open**, SSH restricted to
Hunter's address. Nothing else — the node is the only listener.

On it: `~/.local/bin/tritium` from the release tarball, `/etc/tritium/node.env`
(0600), a systemd unit, and a `tritium.service` that does not restart on a
config reload (the store is RAM-only, so a bounce drops presence).

```sh
# /etc/tritium/node.env
LISTEN_ADDRESS=0.0.0.0:8080
ADVERTISE_ADDRESS=tritium.mubs.example:8080
AUTH_PASSWORD=...            # the fleet's client password
PEER_PASSWORD=...            # the fleet's peer password
TLS_CERT=/etc/tritium/node.crt
TLS_KEY=/etc/tritium/node.key
TLS_CA=/etc/tritium/ca.crt
USERS_FILE=/etc/tritium/users
```

Certificates come from the fleet's existing CA
(`scripts/install-tritium.sh certs`, in mubs) — one more name, copied up once.
The advertised address must be the public name the home nodes dial.

### b. The two home nodes

One line each in `~/.config/mubs/tritium.env`:

```sh
LINK_ADDRESS=tritium.mubs.example:8080
```

Both nodes need a tritium build with `TRITIUM.PEERLINK` before either line goes
in — the link is refused by an older node, harmlessly, until then.

### c. Secrets

One new secret, `mubs/tritium-cloud`, holding **only the gateway user's
password** — not `AUTH_PASSWORD`, not `PEER_PASSWORD`, neither of which may ever
reach the cloud. The gateway's IAM role gets `secretsmanager:GetSecretValue` on
that one ARN.

### d. Gateway Lambda environment

| Variable | Value |
|---|---|
| `MUBS_TRITIUM_CLOUD_ADDR` | `tritium.mubs.example:8080` |
| `MUBS_TRITIUM_CLOUD_USER` | `gateway` |
| `MUBS_TRITIUM_CLOUD_SECRET` | `mubs/tritium-cloud` (the name, resolved at runtime) |
| `MUBS_TRITIUM_CLOUD_CA` | the CA certificate, inline PEM — public, not a secret |

The function stays **out of any VPC**: it reaches the node over the public
internet on 8080 under TLS pinned to the fleet CA, which is why there is no NAT
gateway on this bill.

### e. The user entry

```sh
# /etc/tritium/users on the cloud node
gateway=<the secret's value>:rw:node:gateway,sig:gateway;r:board:,fleet,node:,id:
```

That is the whole blast radius of the credential in the cloud: it can say where
it is, drain its own signal mailbox, and read the boards and the fleet. It
cannot write another node's presence, cannot signal another node, cannot read a
message, and cannot join the cluster.
