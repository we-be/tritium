# Backlog

What the loop works from, in order. One item per iteration, test-verified,
pushed when CI is green, rolled to the mubs fleet nodes when it changes what a
node does. Check an item off with the commit that closed it.

## In flight — 2026-09-07 wave (claimed; the steward skips these)

- embedded store: a node is its own store when no store address is configured — lead
- SCAN/TYPE/DBSIZE for clients, `pkg/tritium.Scan`, key browser in tritium-wails — agent
- fleet event log in the plane (the Visibility item below) — agent
- messenger: several devices per name, groups by pairwise fan-out — agent
- cloud node + adapter so the mubs serverless worker can join the plane with a lesser credential — agent
- public-project polish (README, package docs, examples) — agent, after the above merge

## Now

- [x] Resync a peer's store on attach — 2026-09-06
- [x] A node upgrade wipes its store — mubs 0f832c9: `systemctl --user reload mubs-tritium` (SIGHUP) restarts the node alone, 2026-09-06
- [x] `linux/arm` (GOARM=6) in `make dist` for a Pi Zero worker; `linux/arm64` already covers a Zero 2 W on a 64-bit OS — v0.6.0, 2026-09-06
- [x] Load test: `cmd/tritium-load` drives a SET/GET/ZADD mix at a rate and reports p50/p95/p99 and, with `-peer`, replication lag — 2026-09-06
- [x] Replication through the peer's node (`TRITIUM.REPLICATE`, peer-only) — stores bind to loopback on the mubs fleet; found and fixed on the way: a write on a dead pooled connection was dropped (now retried across every slot) — v0.7.0, 2026-09-06
- [x] Chaos testing: `TestChaos` (3 s in CI, `make chaos` for a 30 s soak, `TRITIUM_CHAOS_SEED` replays) kills and restarts a three-node lab under writes and checks convergence — found a fan-out deadlock on detach and an invisible quick restart, both fixed — 2026-09-06
- [ ] Chaos against the real fleet: the same actions on bazzite and the Air (reload, kill the store, drop the link), watched through `mubs fleet` — partition by SIGSTOP done 2026-09-06: found replica writes had no deadline (a frozen peer stalled writes until detach — fixed b28e3f4); store kill done: the node stayed up answering errors with nothing to heal it (fixed in the mubs wrapper); eviction-length freeze done: the thawed peer was re-learned but never re-attached, and a same-incarnation return could overwrite the survivor — fixed 90dfcb6 (ensure-attached, incarnation-based overwrite, self-stall detection). A real network cut (both sides alive) is now tested in the lab (`TestPartitionHeals`, each node behind a cuttable TCP link): found that a held peer's missed keys died with the detach, so a key updated during a partition longer than 15 s stayed stale on the other side — fixed (backlogs are parked by address and inherited on re-attach). The same cut on the real fleet needs a firewall rule or the cross-network Mac
- [x] Monitor: the cluster view carries each node's `version`, `seeds` (what it dials) and replica counts (held ones flagged); the node's `INFO store` proxies its loopback store's version, keys, memory and uptime, and the monitor reads every store through its node (`-store-password` gone) — 2026-09-06

## Visibility (agreed 2026-09-06; the OSS steward works these after Now)

- [ ] (claimed, 2026-09-07 wave) Fleet event log in the plane: each node records its cluster events — attach, detach, hold, repair (keys), stall, evict, resync (keys, took) — as a capped, TTL'd sorted set in its store, replicated like any key, so `tritium-cli`, the monitor and `mubs fleet` can show what happened in the last hours across the fleet from any node
- [ ] Richer gossip stats: NodeStats carries writes/s, keys, memory, last repair time and the store's uptime, so every consumer gets them from the view without dialing each node
- [ ] Small answers for the agents that operate the fleet: `CLIENT LIST` (who is connected — worker, bridge, CLI), `tritium-cli where <key>` (which nodes hold it, TTL on each), `tritium-msg status` (bridge sessions, last message, latency)
- [ ] (mubs, not here) plane health on the Discord status board — node versions, held replicas, peer state — and a page only on a sustained condition such as a peer held for more than ten minutes

## Later

- [ ] Metrics export for a Grafana stack: a Prometheus text endpoint is zero-dep; OpenTelemetry means the OTel SDK (a dependency) or a hand-rolled OTLP exporter — decide when the stack exists. Until then the plane's own event log and gossip stats are the time series

- [ ] Last-writer-wins for keys written on both sides of a partition: today whichever side's replay lands last wins; a per-key write stamp carried in `TRITIUM.REPLICATE` and compared by the receiving node would make it deterministic. Only matters when both networks write the same key — mubs' presence and mailboxes are per node
- [ ] (claimed, 2026-09-07 wave) Messenger groups; multiple devices per identity
- [ ] (claimed, 2026-09-07 wave) A cloud node, so a fleet that spans networks has a member that is always up

## Done

- [x] Asynchronous replication as a knob: `REPLICATION=async` answers once the local store has a write and feeds each peer in order from a queue (coalesced batches, one writer per peer); a peer that falls 4096 fan-outs behind is held and repaired. Default stays `sync`; the cross-network fleet decides per node — 2026-09-06

- [x] Sync pipelining: a resync and a repair read a whole SCAN page in two pipelined round trips (types and TTLs, then values) and send it to the peer in one; `cluster: resynced` logs how long it took — 2026-09-06

- [x] Held replicas: a replica the transport fails to reach is held (writes note their keys instead of waiting out the 2 s deadline) and the health tick's `Store.Repair` replays exactly what it missed — current values, rebuilt sorted sets, deletions — before releasing it; past 10k keys the repair is a full copy. Closes the gap where a freeze shorter than the 15 s detach window silently lost updates on the peer — 2026-09-06

- 2026-09-06 v0.7.0: replication via the peer's node, retry across dead pooled connections · v0.6.0: resync on attach, 32-bit ARM · v0.5.0: encrypted ratchet headers · v0.4.0: Double Ratchet, sealed hello · v0.3.x: ask/serve, -config, OptionsFromEnv · v0.2.x: seed re-join, release binaries, -store-password · v0.1.0: RESP front end, encryption, TLS, peer credentials
