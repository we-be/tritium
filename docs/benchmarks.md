# Benchmarks

Measured 2026-09-07 on tritium v0.13.0. One box is a 12-core Linux desktop;
the fleet is that desktop and an M1 MacBook Air on the same wifi (ping
between them 5–100 ms that afternoon, 50 ms average). Every run is
`tritium-load` for 5–10 s over 10 000 keys of 64-byte values unless noted;
`-rate 0` is unbounded. Reproduce with `go test -bench . ./internal/memstore/
./internal/server/` for the in-process numbers and the commands in each
section for the rest.

## One node, one box

| Node | Store | Load | ops/s | SET p50 | GET p50 |
|---|---|---|---|---|---|
| v0.13.0 | embedded | mix 50/45/5, 4 conns | 53 800 | 72 µs | 64 µs |
| v0.13.0 | embedded | mix, 32 conns | 131 400 | 197 µs | 187 µs |
| v0.13.0 | embedded | GET only, 32 conns | 185 700 | — | 131 µs |
| v0.13.0 | embedded | mix, 4 KiB values, 32 conns | 90 000 | 262 µs | 217 µs |
| v0.13.0 | embedded, TLS | mix, 4 conns | 48 800 | 79 µs | 71 µs |
| v0.13.0 | embedded, TLS | mix, 32 conns | 133 700 | 198 µs | 188 µs |
| v0.13.0 | Valkey on loopback | mix, 4 conns | 32 700 | 115 µs | 108 µs |
| v0.13.0 | Valkey on loopback | mix, 32 conns | 48 600 | 638 µs | 626 µs |
| v0.12.0 | embedded (no stamps) | mix, 4 conns | 53 300 | 70 µs | 66 µs |
| v0.11.1 | Valkey (no ownership, no stamps) | mix, 4 conns | 34 500 | 111 µs | 107 µs |

What that says:

- The embedded store is 1.6× faster than a loopback Valkey at 4 connections
  and 2.7× at 32: the store is reached over an in-process pipe (5 µs a round
  trip against 23 µs over loopback TCP), and it is not behind one
  single-threaded server that four pooled connections queue on.
- Write stamps cost nothing measurable at the node (v0.12.0 → v0.13.0). In
  the store itself a stamped SET is 720 ns against 575 ns unstamped.
- TLS costs about 10 % at 4 connections and nothing at 32.
- Key ownership and the event log cost nothing on one node (v0.11.1 → v0.13.0
  in front of the same Valkey).
- A node is bound by syscalls, not by the store: under a CPU profile more
  than half the time is the kernel reading and writing sockets, and the
  store's own work does not reach the top of the list. Throughput scales
  with connections; per-request latency at 70 µs is the cost of one
  request-reply exchange without pipelining.

## The fleet over wifi

Two nodes, `-config` pointing at a node's env, 2 000 keys, `-peer` measuring
how long a write takes to be readable on the other node.

| Ownership | Load | ops/s | SET p50 | SET p99 | replication lag p50 |
|---|---|---|---|---|---|
| on | 300/s mix | 300 | 7.9 ms | 16.1 ms | 11.7 ms |
| on | SET only, unbounded, 4 conns | 408 | 9.0 ms | 17.5 ms | — |
| on | GET only, unbounded, 4 conns | 52 900 | — | — | reads are local |
| off | 300/s mix | 300 | 5.6 ms | 8.9 ms | 10.0 ms |
| off | SET only, unbounded, 4 conns | 653 | 6.0 ms | 10.9 ms | — |

A write already paid one wifi round trip to reach the peer; with ownership
half the writes pay a second one to reach the key's owner first, so the
median SET grows by about half a round trip, 2.3 ms here. That is the price
of `NX` and write order holding across the cluster. Reads never leave the
node.

## The fleet with a cloud node

Measured 2026-09-08, the same desktop and Air on wifi plus a Lightsail nano
in us-east-1 linked from both (`LINK_ADDRESS`; TCP handshake to it 21 ms).
`tritium-load -config <the desktop's env> -rate 100 -duration 5s -keys 50
-conns 2 -peer <the Air>`, run after each release rolled to all three.

| Release | What changed | SET p50 | ZADD p50 | lag to the Air p50 |
|---|---|---|---|---|
| v0.15.0 | the cloud node joined; every write waited for it | 29.7 ms | 30.1 ms | 36 ms |
| v0.16.0 | a peer across a link is fed from a queue on its own | 12.3 ms | 5.6 ms | 19 ms |
| v0.17.0 | `ELECTRONEGATIVITY`: the cloud node never owns a key | 7.2 ms | 11.3 ms | 13 ms |

GET stayed at 0.1 ms throughout: reads never leave the node. At v0.15.0
every write paid the round trip to the cloud, including writes for the
machine beside the writer, because the link was waited on like any other
peer. Queue-feeding the link took that out. At v0.16.0 the cloud node
could still own a machine's key from the machine's side — a WAN forward,
then the write fed back to the writer from a queue — and the weights
closed that. What is left in the 7 ms is one wifi round trip for a key the
desktop owns and two in series for one the Air owns (the forward, then the
owner's fan-out back); the ZADD column is a single key and lands on
whichever node the hash picks. The backlog holds the one-round-trip design
for the forwarded case.

Between v0.16.0 and v0.17.0 the cloud node also stopped being churned: the
spare link connections had died and been reopened every ten seconds on the
spoke's auth deadline, and gossip had opened a TCP+TLS connection per round.
With both gone, the nano's packet-rate allowance drops and TCP retransmits
went to zero (`ethtool -S`, `nstat`); the handshake stalls seen before were
those bursts.

## Memory

200 000 keys written through a node, then the store's own accounting against
the process:

| Value | store_used_memory | live heap per key | RSS |
|---|---|---|---|
| 64 B | 29.5 MB before the fix, 54.6 MB after | 274 B | 236 MB |
| 1 KiB | 221 MB | 1 234 B | 616 MB |

The store charges each key its bytes plus a fixed overhead; that overhead
was 64 bytes and the live heap said 210, so `STORE_MAX_MEMORY` undercounted
small keys by two. It is 200 now, and the charge matches the live heap
within a percent at both sizes. RSS is higher than the live heap because
the Go runtime keeps garbage up to the live size before collecting; with a
cap set the node also gives the runtime a soft limit of 1.5× the cap, so a
store at its cap stays near that in RSS rather than twice it.

## In the store

`go test -bench . ./internal/memstore/`, one core:

| Operation | Time | Allocations |
|---|---|---|
| SET, 64-byte value | 555 ns | 3 |
| SET with a stamp | 720 ns | 3 |
| GET | 136 ns | 0 |
| ZADD | 362 ns | 2 |
| SCAN, one page of 1 000 over 100 000 keys | 613 µs | 5 073 |
| SET round trip over the in-process pipe | 5.0 µs | 30 |
| SET round trip over loopback TCP | 23 µs | 30 |
| 100 SETs pipelined over the pipe | 3.1 µs each | 30 each |

The 30 allocations a round trip are the RESP reader building the argument
strings and the store building its reply; a SCAN page allocates once per
key. Neither shows up under a node's load, where the kernel dominates.
