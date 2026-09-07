# Security Policy

## Supported versions

Tritium is pre-1.0 (`v0.x` tags). There's no long-term-support branch: fixes
land on `main` and go out in the next tagged release.

## Reporting a vulnerability

Open an issue for anything that doesn't need to stay private. For something
sensitive, use this repository's private vulnerability reporting under
GitHub's **Security** tab instead of a public issue.

## What's in scope

What this project actually claims, and where it's enforced:

- `AUTH_PASSWORD` / `PEER_PASSWORD` gating and `USER_<name>` key-prefix ACLs
  (`internal/server/acl.go`, `internal/server/commands.go`)
- TLS and mutual TLS between nodes and clients (`internal/server/tls.go`)
- end-to-end AES-256-GCM sealing in `pkg/tritium` (`pkg/tritium/crypto.go`) —
  a node never holds the key or sees plaintext for a value sealed that way;
  see the README's Security section for the wire format
- the messenger's X3DH/Double Ratchet session and header encryption
  (`pkg/messenger`), which keeps mailbox contents and metadata from the node
  relaying them

The store itself is not a trust boundary: it's RAM-only and every key
expires, but a node with `AUTH_PASSWORD` (or none set) can read anything not
sealed client-side, same as any Redis-protocol server can read what it's
handed in the clear.

The latest review, with the threat model and the operating procedures that follow from it, is [docs/security-review.md](docs/security-review.md).
