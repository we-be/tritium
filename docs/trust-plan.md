# Trust levels and replication surfaces — the plan

Opened 2026-09-09; the aim is to finish in Q1 2027. This is the plan of
the plan: what the long-run primitive is, what keeps it from turning into
spaghetti, what can only be learned by running it, and the order — which
is the real lever. The loop takes increments from the order below, one per
iteration, top-down; the phase gates are Hunter's read.

## Why

A tritium node knows three callers. `default` is a client with the whole
keyspace. `USER_<name>` is a client with key-prefix rights and nothing else:
SCAN, the cluster view and every peer command are refused. `peer` is
another node, and there is exactly one level of it — a peer gets every key
replicated to it, the fleet's plaintext state included, sees every member's
address, and may write anything through `TRITIUM.REPLICATE`.

So a *person* can already connect securely: the messenger was built for an
untrusted node, mailbox names are secrets derived from the session, and the
cloud gateway has lived on a `USER_` credential since the cloud node went
up. A *node* cannot join without being handed the fleet. That is the gap.
The end state: a node joins for a surface — a set of key prefixes — and
gets that surface only. Replication, ownership, the view, repair and relay
are all bounded by it, enforced in one place, with no change on the wire.

## What the keyspace says

Census of the live fleet plane, 2026-09-09, by prefix (`tritium-cli scan`):

| prefix    | keys | what                              | surface  |
|-----------|-----:|-----------------------------------|----------|
| `board:`  |   15 | mubs boards and signals            | fleet    |
| `node:`   |    3 | worker presence                    | fleet    |
| `sig:`    |    1 | a worker's signal set              | fleet    |
| `fleet`   |    1 | the queue claim                    | fleet    |
| `tritium:`|    3 | each node's event log              | fleet    |
| `id:`     |    3 | messenger identities               | public   |
| `hello:`, `mbx:`, `msg:`, `devices:`, `grp:` | 0 now (they expire) | the messenger's mailboxes and rosters | public |

No key sits in two. Two surfaces exist in practice already, and a prefix
partitions them cleanly; the plan builds on that rather than on a new key
scheme. The census is re-run at every phase gate (increment 1 makes it a
command), because a prefix that stops partitioning is the first thing that
would invalidate the design.

## The primitive

Rights are the primitive, and they exist: `<r|w|rw>:<prefix>[,...]`, the
`USER_` grammar. Everything else is built from them.

- A **surface** is a named rights set: `SURFACE_public=rw:hello:,mbx:,msg:;w:id:;r:id:,devices:,grp:`.
- A **principal** is a user or a peer, and holds rights, inline or by name (`@public`).
- A peer's **read rights** are what this node sends it; its **write rights**
  are what this node accepts from it; its **weight** says whether it may
  own a key at all.

A trust level is then a name for a combination, not a fourth mechanism:

| level | name    | rights            | replicated to | owns keys        | writes wait on it |
|------:|---------|-------------------|---------------|------------------|-------------------|
| 0     | client  | a subset          | no            | no               | no                |
| 1     | replica | a subset          | its surface   | no (weight 0)    | no (queued)       |
| 2     | member  | a subset          | its surface   | within it        | yes, within it    |
| 3     | fleet   | everything        | everything    | everywhere       | yes               |

Today's `USER_` is level 0 and today's `peer` is level 3. The plan adds 1,
then 2, and only if 2 turns out to be wanted; an outsider's node is a
replica until there is a reason it should own anything.

## The rules

These are what keep six months of increments from becoming spaghetti.
An increment that needs to break one is a sign the primitive is wrong, and
the plan changes before the code does.

1. **One policy.** `config.May` and the ACL's command table grow into the
   single answer to "may this principal read, write, receive or own this
   key". The session's `allow`, the replica fan-out, `ownerOf`, relay,
   repair and resync all ask it; none grows a check of its own.
2. **Enforced at the node, never in a client.** `tritium-cli`, the desktop
   browser, a Lambda over RESP and the `pkg/tritium` SDK all get it for
   free, whatever shape the caller is. Clients stay dumb.
3. **No wire change.** Scoping is what a node chooses to send and to
   accept. An old node and a new one interoperate; a scoped peer that
   reaches an old node is a user there and gets NOPERM on every peer
   command. The ask/serve envelope and the `TRITIUM.*` commands do not
   move. (Every wire change so far has meant a joint rollout; this
   program needs none.)
4. **Off until configured.** A node with today's env behaves as today after
   every increment. There is no flag day.
5. **The `peer` special cases shrink, never grow.** `peer` becomes a
   principal with every right; `isPeer` and `peerOnly` retire as the policy
   absorbs them. An increment that adds an `if peer` is doing it wrong.
6. **A unit is whole or it is not shipped.** Each increment is one knob or
   command, one enforcement point, one test on the failure that matters,
   one line in INFO, an event or a metric, one CHANGELOG line, and a
   statement of whether the fleet rolls. Nothing is "done" until it runs
   behind its real gate on the real fleet.

## What we can only learn by running it

The order below is a best guess; these questions are what revise it. Each
has an instrument, and each phase gate reads them before the next phase
is committed to.

- **Q1 — does a prefix partition the keyspace?** Instrument: the census
  command. Answered yes at n=26; re-read monthly, since mubs adds prefixes.
- **Q2 — what does a queued replica look like over a WAN for weeks?** The
  cloud node already is one: weight 0, fed from a queue when held. Its
  `tritium_*` metrics and `tritium-load -peer` say what lag and backlog
  look like; a daily scrape into a file is enough until a Grafana stack is
  wanted.
- **Q3 — does repair work on a partial keyspace?** A held replica's missed
  keys are replayed on re-attach, and resync copies by SCAN. Both must
  learn to copy only what the peer may hold. The chaos test with a scoped
  lab node, and the real-fleet cut recipe (both IP families), answer it.
- **Q4 — what does a scoped peer see?** A lab node dumps its view and its
  gossip; the view scoping in phase 2 is designed from that dump, not
  from guesswork.
- **Q5 — does it hold with a real outsider?** A second cloud node in
  another region, or someone else's box, joins `public` and carries
  messenger traffic for a month.

## The order

### Phase 0 — measure and name (September–October 2026). No behaviour changes.

1. **`tritium-cli prefixes`** — keys by prefix and type across every node,
   the census as a command. Answers Q1 on demand.
2. **`SURFACE_<name>=<rights>`** in config; a `USER_` line may say
   `@name` in place of inline rights. Test: the fleet's real env parses to
   rights identical to today's.
3. **The policy in one place.** `config.May` and the ACL's keyed-command
   table move under one name that answers for a principal; `allow` calls
   it; `peer` becomes a principal with every right. Complexity and deadcode
   gates hold the line.
4. **Peers are principals.** `PEER_<name>=<password>:<rights>`; such a peer
   authenticates as `AUTH <name> <password>`; the unscoped `peer` keeps
   working. Accept-side only: `TRITIUM.REPLICATE` and `TRITIUM.FORWARD`
   from a scoped peer are checked against its write rights, and a `RELAY`
   from one is refused. Test: a scoped peer writing outside its rights is
   NOPERM, and a fleet write it sends for a key it may write lands.

### Phase 1 — the surface is real (October–November 2026).

5. **Fan-out honours read rights.** The replica store knows each replica's
   rights and skips a command whose key the replica may not hold — one
   filter in the fan-out, counted per peer in metrics.
6. **Weight.** A scoped peer is weight 0 unless its rights are everything;
   `ownerOf` never picks it and nothing is forwarded to it.
7. **Never waited on.** A scoped replica is queued from the start, the way
   a held one is, so the fleet's write latency does not change. Measured
   with `tritium-load` before and after.
8. **Chaos with a scoped node.** The lab cluster gains a `public`-only node;
   repair and resync copy only what it may hold (SCAN MATCH per prefix).
   Finds what Q3 hides.

*Gate:* a fourth node on bazzite joins the real fleet as a `public` replica
for two weeks. Read Q2, Q3 and Q4 from it.

### Phase 2 — the view and the edges (December 2026).

9. **View scoping**, designed from the phase-1 dump: a scoped peer's
   `TRITIUM.NODES` and gossip carry only the nodes that share a surface
   with it — the hub and other public replicas — never a fleet address.
   Gossip merge takes no member on a scoped peer's word (`PEER_ALLOW`
   already refuses strangers; this makes the refusal structural).
10. **Relay honours rights.** The hub relays a write to a scoped peer only
    for a key in its rights.
11. **Links for scoped peers.** A NAT'd outsider links out to the hub as
    the fleet's own machines do.
12. **Second cloud node** (Q5), a month.

### Phase 3 — identity and admission (January 2027).

13. **Peer identity by certificate.** Under `TLS_CLIENT_AUTH` a peer's
    name is its certificate's, rights looked up by name, no shared
    password. A scoped outsider gets a certificate signed by the fleet CA.
14. **Invite.** `tritium-cli invite NAME @public` mints a one-time
    credential and hands it over the messenger — the security review's
    new-machine flow, as one command.
15. **Bounds per principal.** Connections, operations per second and bytes
    held, so a guest can neither fill the store nor spend the cloud node's
    packet allowance.

### Phase 4 — legible and shipped (February–March 2027).

16. **Surfaces are visible**: INFO, the attach event, the monitor, the
    desktop browser and `tritium-cli nodes` all say which surface a peer
    holds.
17. **Docs and the security review** gain the model; the SDK gains nothing,
    since the node enforces.
18. **An outsider's trial**, a month, on a node nobody in the fleet runs.
    If it holds, this is the 1.0 story; that call is Hunter's.

## Deliberately not in it

- **A key in two surfaces.** No: a key has one surface, by longest prefix.
- **Per-key rights inside a surface** (only a mailbox's owner may delete
  from it). The messenger's secret mailbox names already give this; it
  comes back only if a census shows a guessable name.
- **Federation between two fleets.** A scoped peer on the hub is the seam
  if it is ever wanted; nothing here should make it harder.
- **A second policy language.** Rights are it.

## How an increment ships

One per loop iteration, from the top of the order: implement,
`make lint && go test -race ./...`, `make integration` when the store is
touched, `make chaos` when replication is, commit, push, CI green, a
CHANGELOG line, and either a fleet roll (the recipe in
[cloud.md](cloud.md): hub first when the hub must understand something
new) or "nothing to roll" said outright. The phase gates are read in
prose, with the instruments' numbers, before the next phase starts.
