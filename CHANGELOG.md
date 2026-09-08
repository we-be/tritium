# Changelog

Every release since the first, one line each. Release notes on GitHub are auto-generated
compare links, so this is the readable index of what a version actually changed — check it
before bisecting a behaviour change, and add a line when you tag.

Dates are tag dates. `gh release list --repo we-be/tritium` is the authoritative list;
`https://github.com/we-be/tritium/compare/vA...vB` shows any range in full.

## v0.18.x — the hub relays (2026-09-08)

- **v0.18.0** — **Relay through the hub.** A node marks what it sends the hub with
  `RELAY n addr…`, the peers it could not deliver to itself (held, or never reachable);
  the hub applies the write and sends the plain command on from its own pools. Nothing
  relays twice, stamps settle duplicates, and a hub older than this is sent plain writes,
  so the hub rolls first. Also a Prometheus text endpoint behind `METRICS_ADDRESS`
  (off by default, no auth — bind it to loopback or a private interface).
- **v0.18.1** — **Homebrew.** Every release attaches its own `tritium.rb` (version and
  checksums filled in by `make formula`), and the tap `we-be/homebrew-tritium` copies the
  latest one, so `brew install we-be/tritium/tritium` installs the current release.

## v0.17.x — ownership weights and forwarding (2026-09-08)

- **v0.17.8** — a forwarded write costs one round trip: the owner answers with what it sent.
- **v0.17.7** — every connection's TCP keepalive is its own, spread over 30–60 s. The fleet's
  ~30 connections probing in step had been a 184-packet/10 ms burst against the cloud node's
  packet-rate allowance.
- **v0.17.5 / v0.17.6** — `tritium-msg status`: every session, last used, messages each way,
  unread waiting — and it does not take the state lock (every other verb does, and the
  bridge's `serve` holds it).
- **v0.17.4** — server/replica/memstore split by concern; forward-refusal step.
- **v0.17.3** — `tritium-cli where KEY`: every node's copy, with type, TTL and digest.
- **v0.17.2** — `CLIENT LIST`.
- **v0.17.1** — gossip stats.
- **v0.17.0** — **ELECTRONEGATIVITY.** Each node competes for a key with as many rendezvous
  points as its weight; **weight 0 never owns a key and is never leader**, which is what makes
  a cloud hub a pure replica rather than an owner by luck. Rides the gossip record; **absent
  reads as 1**, so a half-rolled fleet stays consistent. Also `Store` → `internal/replica`.

## v0.16.x — the WAN link learns to stay up (2026-09-08)

- **v0.16.4** — pooled gossip; `info`. **The end of a real liveness bug:** through v0.16.1 the
  machines wrote the hub off **38 times in six hours**. After 0.16.4, rate-limit drops and
  retransmits are zero and the LAN SET p50 went 30 ms → 7 ms. If plane latency ever regresses,
  check the version before anything else.
- **v0.16.3** — AUTH sent down a link as it is handed over; spares had been dying every 10 s on
  the spoke's auth deadline.
- **v0.16.2** — dead links dropped at once; dial-phase telemetry.
- **v0.16.1** — gossip liveness: a peer not heard from since the last round is dialled too, and
  a linked peer is never dialled by anyone else.
- **v0.16.0** — links are queue-fed, so no write waits on the WAN.

## v0.12 – v0.15 — the store, then the hardening (2026-09-07 → 09-08)

- **v0.15.0** — store TLS, per-command deadline, distinct peer password.
- **v0.14.3** — parser limits (512 MiB bulk, 2²⁰ array, depth 32).
- **v0.14.2** — `GET` on a zset is `WRONGTYPE`.
- **v0.14.1** — arm build fix.
- **v0.14.0** — security sweep and review: the threat model, invite procedure and lost-machine
  playbook in `docs/security-review.md`.
- **v0.13.0 / v0.13.1** — write stamps (hybrid clock, `STAMPED <n>`, 24 h tombstones, later
  write wins across a partition); benchmarks.
- **v0.12.0** — embedded store, `SCAN`, devices and groups, the event log, cloud peering with
  ACLs, key ownership.

## v0.1 – v0.11 — the first two days (2026-09-05 → 09-06)

- **v0.6 – v0.11.1** — resync on attach, replication via the peer's node, chaos and partition
  tests, held replicas, sync pipelining, the async knob, parked backlogs.
- **v0.1 – v0.5** — the RESP front end, client-side sealing, TLS, the peer credential, seed
  rejoin, `ask`/`serve`, the Double Ratchet, encrypted headers.

## Conventions

Zero dependencies is core to the project, so a release never adds one. `pkg/tritium` and the
six view types in `pkg/storage` are a contract (see `CONTRIBUTING.md`); everything under
`internal/` moves freely and needs no changelog line unless behaviour changed.

Rolling the fleet: bazzite → Air → cloud, waiting for replicas to come back between nodes. After
tagging, bump the pin in mubs `scripts/install-tritium.sh` and `scripts/provision-valence.sh`.
