# Backlog

What the loop works from, in order. One item per iteration, test-verified,
pushed when CI is green, rolled to the mubs fleet nodes when it changes what a
node does. Check an item off with the commit that closed it.

## Now

- [x] Resync a peer's store on attach — 2026-09-06
- [x] A node upgrade wipes its store — mubs 0f832c9: `systemctl --user reload mubs-tritium` (SIGHUP) restarts the node alone, 2026-09-06
- [x] `linux/arm` (GOARM=6) in `make dist` for a Pi Zero worker; `linux/arm64` already covers a Zero 2 W on a 64-bit OS — v0.6.0, 2026-09-06
- [ ] Load test: a `cmd/tritium-load` or `make load` that drives SET/GET/ZADD at a rate against a node and reports p50/p99 and replication lag between two nodes
- [x] Replication through the peer's node (`TRITIUM.REPLICATE`, peer-only) — stores bind to loopback on the mubs fleet; found and fixed on the way: a write on a dead pooled connection was dropped (now retried across every slot) — v0.7.0, 2026-09-06
- [x] Chaos testing: `TestChaos` (3 s in CI, `make chaos` for a 30 s soak, `TRITIUM_CHAOS_SEED` replays) kills and restarts a three-node lab under writes and checks convergence — found a fan-out deadlock on detach and an invisible quick restart, both fixed — 2026-09-06
- [ ] Chaos against the real fleet: the same actions on bazzite and the Air (reload, kill the store, drop the link), watched through `mubs fleet` — partition by SIGSTOP done 2026-09-06: found replica writes had no deadline (a frozen peer stalled writes until detach — fixed b28e3f4); store kill done: the node stayed up answering errors with nothing to heal it (fixed in the mubs wrapper); eviction-length freeze done: the thawed peer was re-learned but never re-attached, and a same-incarnation return could overwrite the survivor — fixed 90dfcb6 (ensure-attached, incarnation-based overwrite, self-stall detection). A real network cut (both sides alive) is still untested — needs a firewall rule or the cross-network Mac
- [ ] Monitor: a node's `SEED` column reads false everywhere once every node lists the others as seeds; show `seeds` (what it dials) instead. Its per-store panel also can't reach loopback stores any more — proxy `INFO` through the node

## Later

- [ ] Sync pipelining: copy keys in batches of a page instead of one round trip per key
- [ ] Writes pay the peer round trip synchronously (SET p50 4.6 ms vs GET 161 µs on the LAN fleet): consider acknowledging after the primary write and fanning out from a queue — faster, but a write would no longer be on the peer when acked; decide with the cross-network work
- [ ] Messenger groups; multiple devices per identity
- [ ] A cloud node, so a fleet that spans networks has a member that is always up

## Done

- [x] Held replicas: a replica the transport fails to reach is held (writes note their keys instead of waiting out the 2 s deadline) and the health tick's `Store.Repair` replays exactly what it missed — current values, rebuilt sorted sets, deletions — before releasing it; past 10k keys the repair is a full copy. Closes the gap where a freeze shorter than the 15 s detach window silently lost updates on the peer — 2026-09-06

- 2026-09-06 v0.7.0: replication via the peer's node, retry across dead pooled connections · v0.6.0: resync on attach, 32-bit ARM · v0.5.0: encrypted ratchet headers · v0.4.0: Double Ratchet, sealed hello · v0.3.x: ask/serve, -config, OptionsFromEnv · v0.2.x: seed re-join, release binaries, -store-password · v0.1.0: RESP front end, encryption, TLS, peer credentials
