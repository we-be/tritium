# Backlog

What the loop works from, in order. One item per iteration, test-verified,
pushed when CI is green, rolled to the mubs fleet nodes when it changes what a
node does. Check an item off with the commit that closed it.

## Now

- [x] Resync a peer's store on attach — 2026-09-06
- [x] A node upgrade wipes its store — mubs 0f832c9: `systemctl --user reload mubs-tritium` (SIGHUP) restarts the node alone, 2026-09-06
- [x] `linux/arm` (GOARM=6) in `make dist` for a Pi Zero worker; `linux/arm64` already covers a Zero 2 W on a 64-bit OS — v0.6.0, 2026-09-06
- [ ] Load test: a `cmd/tritium-load` or `make load` that drives SET/GET/ZADD at a rate against a node and reports p50/p99 and replication lag between two nodes
- [ ] Replication through the peer's node, not straight into its store, so a peer's Valkey never has to be reachable — the precondition for a cross-network replica (a second Mac on another network is coming)
- [ ] Chaos testing: a script that kills and restarts nodes and stores at random, partitions them, and checks the fleet converges (presence, resync, seed re-join) — run against a three-node lab, then the real fleet
- [ ] Monitor: a node's `SEED` column reads false everywhere once every node lists the others as seeds; show `seeds` (what it dials) instead

## Later

- [ ] Sync pipelining: copy keys in batches of a page instead of one round trip per key
- [ ] Messenger groups; multiple devices per identity
- [ ] A cloud node, so a fleet that spans networks has a member that is always up

## Done

- 2026-09-06 v0.5.0: encrypted ratchet headers · v0.4.0: Double Ratchet, sealed hello · v0.3.x: ask/serve, -config, OptionsFromEnv · v0.2.x: seed re-join, release binaries, -store-password · v0.1.0: RESP front end, encryption, TLS, peer credentials
