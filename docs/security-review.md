# Security review, 2026-09-07

Why now: the fleet plane is in production, a node is about to run on a
laptop that travels, a node on a small cloud box will have its port open to
the internet, and the messenger will hand secrets to a machine being set up.
This is what was reviewed, what was found and changed, and the procedures
that follow from it. The policy for reporting a vulnerability is in
[SECURITY.md](../SECURITY.md).

## Threat model

Who can do what, and what stops them:

| Attacker | Can | Stopped by |
|---|---|---|
| Anyone on the same wifi as a node | See packets; connect to a listening port | TLS (values and commands in transit); AUTH_PASSWORD for clients, PEER_PASSWORD plus a cert under TLS_CLIENT_AUTH for peers; a node that only links out (`LINK_ADDRESS`) can bind loopback and accept nothing at all |
| Anyone on the internet reaching the cloud node's port | Try passwords; open connections and hold them; send garbage | Five refused AUTHs close the connection; an unauthenticated connection is closed after 10 s; MAX_CLIENTS caps connections; TLS 1.2+; mutual TLS for peers |
| A client holding a `USER_<name>` credential (the gateway Lambda) | Read and write the key prefixes it was given | Deny-by-default ACL: keys outside its prefixes, SCAN, the cluster view and every peer command are NOPERM |
| A client holding AUTH_PASSWORD | Read and write every key; see the cluster view; run SCAN | The password itself; sealed values (a client-side `Key`) for what must stay private from other clients; the messenger for what must stay private from the store |
| A peer holding PEER_PASSWORD (and a cert) | Everything a client can, plus join the cluster, receive every write, inject gossip, forward writes | Nothing: a peer is a node you run. Guard the credential, and rotate it when a machine is lost |
| The store operator (whoever runs a node) | Read and write every key, watch traffic | The messenger: it sees ciphertext, sealed hellos, padded bodies; bundles and rosters are signed; rosters only move forward; a replayed or tampered message is dropped |
| Someone holding a stolen laptop's files | Its tritium.env (passwords, its cert and key), its messenger identity and sessions, whatever a worker's env held | The playbook below: rotate what it held, revoke its identity |

## What was checked

The node's AUTH and HELLO paths, what runs before authentication, the peer
test under TLS_CLIENT_AUTH, the ACL table and prefix checks, TLS
configuration (versions, verification, ServerName on dials, client auth),
TRITIUM.PEERLINK, TRITIUM.FORWARD, TRITIUM.REPLICATE with stamps, gossip
input, the embedded store's parsing and pattern matching, bulk-size limits,
connection limits, error and log output, the CLI tools' handling of
secrets and state files, config loading, and pkg/messenger end to end
(agreement, ratchet, headers, hellos, padding, replay, bundles, prekeys,
devices, groups, state on disk). A second, independent read of the
messenger and the auth paths was done in parallel; its findings are in the
last section.

## Found and changed

1. **AUTH could be guessed without limit.** A connection could try
   passwords forever, and an unknown user name was refused faster than a
   wrong password. Now: five refusals close the connection and log the
   client address; an unknown user costs the same time as a wrong password.
2. **Idle connections were free.** An open port could be filled with
   connections that never authenticate, each holding a descriptor and a
   goroutine. Now: 10 s to authenticate, then closed; `MAX_CLIENTS`
   (default 10 000) caps connections, the rest are told so and dropped.
3. **A peer could freeze a key.** A replicated write stamped into the far
   future would win every later write to its key for the key's lifetime
   and pull every node's clock forward with it. Now: a stamp more than an
   hour past the receiving node's clock is refused and does not move the
   clock.
4. **The SCAN pattern matcher was exponential.** A pattern with many
   stars, the CVE-2022-36021 shape against Redis, could pin a core from any
   client with SCAN. Now: the matcher walks both strings once and backtracks
   only to the last star; the same pattern completes in microseconds.
5. **A prefix-limited user could read the cluster view.** TRITIUM.NODES
   returned every member's address to any user. Now: NOPERM for users;
   the default user and peers keep it.
6. **A world-readable config file went unremarked.** Now: a config or
   users file readable by other users of the machine is named in the log
   at startup.
7. **A secret sent with `tritium-msg send` sat on the command line**,
   readable by every process on the sender's box. Now: `send NAME -file
   PATH` and `send NAME -` (stdin); `send -fp FP` refuses to send unless the
   published bundle is the fingerprint read on the other machine, and sends
   to that identity alone; `recv -raw [-fp FP]` writes bodies exactly as
   sent, from the pinned sender only.

## Looked at and left as is

- **No per-address throttling of AUTH.** Five tries per connection and a
  reconnect each time is the limit. For a port on the internet the real
  answer is `TLS_CLIENT_AUTH=true`: a client without a certificate from the
  CA never gets to AUTH. The cloud node should run that way; the Lambda can
  carry a client certificate in its secret as easily as a password.
- **TLS minimum is 1.2.** Every client tritium meets (Go, Python's ssl,
  valkey-cli) speaks 1.3; 1.2 stays for anything older. No cipher list is
  pinned; Go's defaults are current.
- **512 MiB bulk strings.** A client with AUTH can send a value that large;
  the embedded store's cap evicts around it. Same as Redis's default.
- **INFO is open to users.** It names the node, its address, the store's
  size. Useful to an operator holding a lesser credential, and nothing
  a lesser credential can act on.
- **Peers are fully trusted.** A peer can inject any membership view and
  any write. That is what a peer is; there is no partial peer. A node you
  do not run should be a user, not a peer.
- **No certificate revocation.** Losing a node's cert is handled by
  rotating PEER_PASSWORD (a peer needs both) and, if you like, reissuing
  the CA. A revocation list would add a config knob for a case the
  playbook already covers.
- **Devices share no state.** A message to one of a name's devices is not
  seen by the others; that is documented, and matches how the worker pins
  a list of device fingerprints.
- **The first message to a name carries the sender's bundle inside a
  sealed hello**, so the store learns who is talking only if it can break
  the sealing. Names on the plane are still visible as keys: `id:<name>`
  says who exists, `mbx:<hex>` does not say who reads it.

## The independent review

A second reviewer read the messenger and the node's auth paths without
seeing this document, demonstrating each finding in a scratch copy. What
came of each:

| Finding | Done |
|---|---|
| `export AUTH_PASSWORD=…` in a dotenv file loaded as no password at all, and a passwordless node treats every connection as a peer | `export` is stripped; a key with whitespace is an error; a node with no password refuses to listen off loopback unless `ALLOW_NO_AUTH=true` |
| Any holder of `PEER_PASSWORD` could gossip a node of its own into the cluster and be sent every key, `PEER_PASSWORD` included | `PEER_ALLOW` names the addresses that may be members; under `TLS_CLIENT_AUTH` a peer must hold a certificate for the address it announces, in gossip and in a link |
| A roster's version was signed as 32 bits and compared as more, so a replayed roster could be given a version nothing could beat | Rosters sign the version as 64 bits, reject one out of range, and put a length before every field, so no two fields read as one |
| A bundle names whatever its sender likes; group membership was checked against that name | A session settles, once, whether `id:<name>` holds its peer's fingerprint; `Message.Verified` says so, the CLI marks the rest, and a group tag from an unverified name counts for nothing |
| A bundle's own signing key was not under its signature | Bundles signed from now on (`v: 2`) cover it; earlier bundles verify until republished, which the workers do hourly |
| A user could pin its keys past everyone else's in the eviction order with a long TTL | A user's TTL is capped at the default |
| `INFO` handed a user the node's address and store | A user gets the health sections only |
| `SCAN COUNT` could ask for the whole keyspace under the store's lock | A page is at most 10 000 keys |
| Four guesses per connection, reconnecting, left no trace and no limit | Every refusal is logged; an address past twenty in ten minutes is shut out for ten |
| A password holding a quote loaded as its first characters | An error |
| A user could be planted by any `USER_*` variable in the environment | In the environment the entry is `TRITIUM_USER_<name>` |
| `-config` dialled a node with a public certificate in the clear | `-config` carries whether the node serves TLS |
| A stamp was observed before the command was judged | Judged first |
| State files kept a mode they were restored with, and were truncated before rewritten | Written beside, synced, renamed; directory and files set to 0700/0600 |
| A name could carry terminal control sequences | Refused at creation and at verification; the CLI prints the full fingerprint and no control characters |
| A right was a bare prefix: `fleet` also granted `fleet-master-key` | A right ending in `:` or `/` is a prefix; any other names one key |
| The quickstart compose file had no passwords on every interface | Passwords from `.env`, ports on loopback |
| A key of all zeros parsed | Refused |

Left as they are, with the reasons:

- **Identity and session state are unencrypted on disk.** On the travelling
  laptop this is what full-disk encryption is for; a passphrase over the
  state files is on the backlog, and the playbook covers a loss meanwhile.
- **The connection pattern shows who talks to whom.** The package doc now
  says so. A node that can watch its own query stream can pair the
  connection that published a name with the mailboxes it polls; the
  contents stay sealed. Cover traffic would hide it and is not worth its
  cost here.
- **A sealed value can be rolled back by the store** to an earlier
  ciphertext under the same key: nothing in the value says which write it
  was. A version in the associated data would settle it; no client of the
  sealing uses rotating secrets yet.
- **Random 96-bit nonces** in the client-side sealing meet the birthday
  bound around four billion seals under one key; rotate the key long
  before. The messenger derives its nonces and is not affected.
- **`MAX_CLIENTS` is checked a moment before the count moves**, so a burst
  can overshoot it slightly. Bounded and harmless.

## Handing secrets to a new machine

The messenger carries them end to end; the plane and its stores see
ciphertext with a 60 s life. What the new machine needs first is a way to
reach a node, and that need not be the node's own password: an invite user
with rights over its own name and the mailbox keys is enough
(`TestInviteUserRunsTheMessenger` runs exactly this).

On a node the new machine can reach (the cloud node, or a LAN node):

```sh
# in the node's env (or USERS_FILE), then reload the node
USER_setup=<one-time-password>:w:id:travel;r:id:,devices:;rw:hello:,mbx:,msg:
```

That grants writes to the one key `id:travel`, reads of every published
identity and roster, and the mailboxes. It cannot touch another name, and
it cannot see the cluster.

On the new machine, with the node's address, its CA certificate (public)
and the one-time password:

```sh
export TRITIUM_USER=setup TRITIUM_PASSWORD='<one-time>'          # env, not flags: nothing on a command line
tritium-msg -addr <node>:8080 -ca ca.crt -state ~/.config/mubs/msg init travel
tritium-msg -addr <node>:8080 -ca ca.crt -state ~/.config/mubs/msg me    # read the fingerprint here, on this screen
tritium-msg -addr <node>:8080 -ca ca.crt -state ~/.config/mubs/msg recv -watch -raw -fp <SENDER'S FINGERPRINT> > tritium.env
chmod 600 tritium.env
```

On the sending machine, after hearing the new machine's fingerprint out of
band — read aloud, or on its screen, never from the plane:

```sh
tritium-msg send -fp <NEW MACHINE'S FINGERPRINT> travel -file tritium.env
```

Then remove `USER_setup` from the node and reload it: the invite was for
one machine, once. The new machine now holds its real env and joins as a
node. Both sides pinned each other's fingerprint, so a bundle swapped on
the plane by whoever can write `id:travel` is refused on both ends.

## A node on a laptop that travels

- `LISTEN_ADDRESS=127.0.0.1:8080` and `LINK_ADDRESS=<cloud node>`: the
  laptop opens the connections and is served over them; nothing on hotel
  wifi can connect to it. Local tools reach it on loopback.
- `TLS_CA`, its own cert and key, `PEER_PASSWORD`; `TLS_CLIENT_AUTH=true`
  on the cloud node so only certificate holders reach AUTH there.
- `REPLICATION=async` on the laptop: a write should not wait for a hotel
  round trip.
- A linked node is never a key's owner and applies what it receives, so
  a stolen laptop's node has no more than a client has: the plane's keys
  as of its last sync, and the credentials in its files.
- Full-disk encryption on the laptop covers the files; the playbook covers
  the credentials.

## The cloud node

- `TLS_CERT`/`TLS_KEY` from the fleet's CA, `TLS_CA` and
  `TLS_CLIENT_AUTH=true`: peers and clients present certificates.
- `PEER_PASSWORD` distinct from `AUTH_PASSWORD`; the Lambda gets a
  `USER_gateway` entry with its prefixes, never the node's password.
- `MAX_CLIENTS` at the default; `STORE_MAX_MEMORY` set; the box's firewall
  admits 8080 and nothing else; ssh by key only.
- Watch the log for "closing a connection after repeated AUTH failures".

## When a machine is lost

Do these on the surviving nodes; none needs the lost one.

1. Rotate `PEER_PASSWORD` and `AUTH_PASSWORD` in every surviving node's
   env, and every client env that carries them, then reload the nodes
   together. The lost machine's cert alone no longer gets it in.
2. Remove any `USER_<name>` entry the lost machine used.
3. Retire its messenger identity: `DEL id:<name>` (and `id:<name>/<device>`
   entries), re-sign any device roster or group that listed it
   (`device authorize` the others again; `group remove`), and drop its
   fingerprint from every pin: worker envs (`MUBS_BRIDGE_PEER`), chat
   clients, `-fp` flags in scripts.
4. If it held a worker env: rotate the bridge token and whatever else was
   in that file.
5. If it held the CA's private key (it should not have; keep the CA on
   one machine): reissue the CA and every cert.
