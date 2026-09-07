# Documentation

Start with the root [README.md](../README.md) — it covers running a node, the
command surface, how a cluster works, the messenger, configuration and
security. What's here goes deeper on one topic each:

- [cloud.md](cloud.md) — a cloud node for a fleet that spans networks: reverse-channel
  peering (`LINK_ADDRESS`/`TRITIUM.PEERLINK`) for a member that can't be dialed,
  per-user key-prefix ACLs (`USER_<name>`) for a client that should get less
  than a node's own password, and what it costs to host one.
- [benchmarks.md](benchmarks.md) — what a node, the embedded store and the fleet measure, and what the memory cap really bounds
