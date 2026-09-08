# Backlog

What the loop works from. One item per iteration, test-verified, pushed when
CI is green, rolled to the mubs fleet nodes when it changes what a node does.
Check an item off with the commit that closed it.

## What the loop works toward

The lists below are hints, not the plan (Hunter, 2026-09-08: "I don't know
that backlog is super accurate"). The plan is inferred from what tritium is
for: the mubs fleet plane — a zero-dependency, Redis-compatible store that a
few machines on a LAN and one always-up cloud node share, and the private
channel between them. In order, it has to be:

1. **Right under trouble** — restarts, upgrades, partitions, a frozen peer, a
   full store. An iteration that finds a way to lose or misorder a write is
   worth more than a feature.
2. **Safe on an internet-reachable port** — one credential per role, a bound
   on everything a stranger can send, nothing in gossip a user should not see.
3. **Legible to whoever runs it**, person or agent — events, INFO, the
   monitor, the CLI and the desktop browser answer "what happened" without a
   shell on the node.
4. **Cheap** — one static binary, RAM-only, a $5 node, no WAN round trip a
   write does not need.
5. **Small** — zero deps, one way to do each thing, no code kept for a caller
   that is gone. `make lint` gates on staticcheck and deadcode for this.

Each iteration picks one and moves it: a fix, a measurement, a probe that
finds nothing, or a cut. Not every iteration ships.

## Now

- [x] Resync a peer's store on attach — 2026-09-06
- [x] A node upgrade wipes its store — mubs 0f832c9: `systemctl --user reload mubs-tritium` (SIGHUP) restarts the node alone, 2026-09-06
- [x] `linux/arm` (GOARM=6) in `make dist` for a Pi Zero worker; `linux/arm64` already covers a Zero 2 W on a 64-bit OS — v0.6.0, 2026-09-06
- [x] Load test: `cmd/tritium-load` drives a SET/GET/ZADD mix at a rate and reports p50/p95/p99 and, with `-peer`, replication lag — 2026-09-06
- [x] Replication through the peer's node (`TRITIUM.REPLICATE`, peer-only) — stores bind to loopback on the mubs fleet; found and fixed on the way: a write on a dead pooled connection was dropped (now retried across every slot) — v0.7.0, 2026-09-06
- [x] Chaos testing: `TestChaos` (3 s in CI, `make chaos` for a 30 s soak, `TRITIUM_CHAOS_SEED` replays) kills and restarts a three-node lab under writes and checks convergence — found a fan-out deadlock on detach and an invisible quick restart, both fixed — 2026-09-06
- [ ] Chaos against the real fleet: the same actions on bazzite and the Air (reload, kill the store, drop the link), watched through `mubs fleet` — partition by SIGSTOP done 2026-09-06: found replica writes had no deadline (a frozen peer stalled writes until detach — fixed b28e3f4); store kill done: the node stayed up answering errors with nothing to heal it (fixed in the mubs wrapper); eviction-length freeze done: the thawed peer was re-learned but never re-attached, and a same-incarnation return could overwrite the survivor — fixed 90dfcb6 (ensure-attached, incarnation-based overwrite, self-stall detection). A real network cut (both sides alive) is now tested in the lab (`TestPartitionHeals`, each node behind a cuttable TCP link): found that a held peer's missed keys died with the detach, so a key updated during a partition longer than 15 s stayed stale on the other side — fixed (backlogs are parked by address and inherited on re-attach). The same cut on the real fleet needs a firewall rule or the cross-network Mac
- [x] Monitor: the cluster view carries each node's `version`, `seeds` (what it dials) and replica counts (held ones flagged); the node's `INFO store` proxies its loopback store's version, keys, memory and uptime, and the monitor reads every store through its node (`-store-password` gone) — 2026-09-06
- [ ] Ownership weight per node: `ELECTRONEGATIVITY=<n>` in a node's env, carried in its gossip record, weighting rendezvous hashing — weight 0 is never a key's owner and never marked leader, so the cloud hub carries replicas and orders nothing (bazzite highest, macair mid, valence 0). Today a linked peer is skipped as owner only from the spokes' view; the hub itself can still pick itself for a write the gateway hands it, so the two views can disagree — the weight closes that. Older nodes ignore the field. Hunter, via the mubs session, 2026-09-08.

## Visibility (agreed 2026-09-06; the OSS steward works these after Now)

- [x] Fleet event log in the plane: each node records its cluster events — attach, detach, hold, repair (keys), stall, evict, resync (keys, took), start — as a capped (500), 24h TTL'd sorted set at `tritium:events:<node id>`, replicated like any key; `tritium-cli events [-since 1h] [-node NAME]` and the monitor's Recent Events panel merge every node's log from the local store — 2026-09-07
- [ ] Richer gossip stats: NodeStats carries writes/s, keys, memory, last repair time and the store's uptime, so every consumer gets them from the view without dialing each node
- [ ] Small answers for the agents that operate the fleet: `CLIENT LIST` (who is connected — worker, bridge, CLI), `tritium-cli where <key>` (which nodes hold it, TTL on each), `tritium-msg status` (bridge sessions, last message, latency)
- [ ] (mubs, not here) plane health on the Discord status board — node versions, held replicas, peer state — and a page only on a sustained condition such as a peer held for more than ten minutes

## Later

- [ ] `TestSimultaneousHello` failed once in CI (2026-09-07, "received [a1], want [a3]": a hello's first message delivered twice after the tie-break) and never in 380 local runs, one CPU included; find the interleaving before it bites a real simultaneous first contact

- [ ] Metrics export for a Grafana stack: a Prometheus text endpoint is zero-dep; OpenTelemetry means the OTel SDK (a dependency) or a hand-rolled OTLP exporter — decide when the stack exists. Until then the plane's own event log and gossip stats are the time series

- [x] Writes to one key ordered in one place: key ownership by rendezvous hash, `TRITIUM.FORWARD` to the owner, local fallback when it is out of reach (`KEY_OWNERSHIP`) — 2026-09-07. Write stamps followed the same day: a hybrid clock per node, `STAMPED <n>` on every string write, the embedded store keeps the stamp and a tombstone, resync and repair carry stamps, so both sides of a partition settle on the later write; an external store still settles by arrival
- [x] Messenger groups; multiple devices per identity — devices are a second identity per device, certified onto a name by its primary identity (`devices:<name>`); groups are a creator-signed roster (`grp:<name>`) sent by pairwise fan-out, group name carried in the encrypted plaintext; envelope format unchanged — `msg-devices` branch, 2026-09-07
- [x] A cloud node, so a fleet that spans networks has a member that is always up — 2026-09-07, `docs/cloud.md`
- [ ] Relay through a node that both sides can reach: today `TRITIUM.REPLICATE` is applied and never fanned out again, so two nodes that cannot dial each other cannot exchange writes even when both link to the same cloud node. Needs the sender's identity on the wire (a `via` argument, so a relayed write cannot echo back or clobber a newer local write) — a change both fleet nodes must take together. Only matters once a machine leaves the LAN

## Done

- [x] A peer across a link is fed from a queue on its own, `REPLICATION` left alone: measured on the fleet, every write on a LAN machine waited on Lightsail (SET p50 30 ms, GET 0.1 ms) because no node had set `async` — v0.16.0, 2026-09-08
- [x] Embedded store: a node is its own store when no store address is configured (`internal/memstore`: strings and sorted sets, expiry heap, bucketed SCAN, `STORE_MAX_MEMORY` with soonest-expiry eviction, reached over in-process RESP; the test store is the same code) — 2026-09-07
- [x] SCAN/TYPE/DBSIZE for clients: `pkg/tritium.Scan`, `tritium-cli scan`, key browser in tritium-wails — 2026-09-07
- [x] Cloud node + adapter so the mubs serverless worker can join the plane with a lesser credential (`LINK_ADDRESS`/`TRITIUM.PEERLINK` reverse peering, `USER_<name>` prefix rights, `docs/cloud.md`) — 2026-09-07
- [x] Public-project polish: README rewritten front to back, `go doc`-quality package comments on every exported package, a `docs/README.md` index, `examples/python` and `examples/go`, `packaging/homebrew/tritium.rb`, `CONTRIBUTING.md` — 2026-09-07

- [x] Asynchronous replication as a knob: `REPLICATION=async` answers once the local store has a write and feeds each peer in order from a queue (coalesced batches, one writer per peer); a peer that falls 4096 fan-outs behind is held and repaired. Default stays `sync`; the cross-network fleet decides per node — 2026-09-06

- [x] Sync pipelining: a resync and a repair read a whole SCAN page in two pipelined round trips (types and TTLs, then values) and send it to the peer in one; `cluster: resynced` logs how long it took — 2026-09-06

- [x] Held replicas: a replica the transport fails to reach is held (writes note their keys instead of waiting out the 2 s deadline) and the health tick's `Store.Repair` replays exactly what it missed — current values, rebuilt sorted sets, deletions — before releasing it; past 10k keys the repair is a full copy. Closes the gap where a freeze shorter than the 15 s detach window silently lost updates on the peer — 2026-09-06

- 2026-09-06 v0.7.0: replication via the peer's node, retry across dead pooled connections · v0.6.0: resync on attach, 32-bit ARM · v0.5.0: encrypted ratchet headers · v0.4.0: Double Ratchet, sealed hello · v0.3.x: ask/serve, -config, OptionsFromEnv · v0.2.x: seed re-join, release binaries, -store-password · v0.1.0: RESP front end, encryption, TLS, peer credentials
