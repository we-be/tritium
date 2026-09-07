# Contributing

## Build

```sh
go build ./...        # or: make build — binaries in bin/
```

Go 1.26, standard library only.

## Before sending a change

```sh
make lint && go test -race ./...
```

`make lint` runs `gofmt`, `go vet` and `go fix`; all three must be silent.
Tests run in two modes and both must pass — the fake and a real RESP server
are expected to agree on every command:

```sh
make test          # against tritium's own in-process RESP fake (internal/resptest)
make integration   # the same tests, against a real server:
                    #   podman run --rm -p 6379:6379 valkey/valkey:8-alpine valkey-server --save "" --appendonly no
                    #   make integration
```

If the change touches membership, replication or repair, also run the longer
soak before sending it: `make chaos` (kills and restarts a lab cluster under
writes, checks convergence; CI only runs the 3-second version).

## Zero dependencies

`go.mod` has no requirements, and a change that adds one will be turned down.
This isn't a style preference: a node is one static binary an operator trusts
with its keys, and everything it links is everything they have to audit.
`internal/resp`, `internal/memstore` and `pkg/messenger`'s own Double Ratchet
exist because the alternative, for each, was a dependency.
`examples/go` is its own module (a separate `go.mod` that replaces the parent
with a local path) for the same reason — it can use whatever it needs without
touching the root module's dependency list.

## pkg/tritium

`pkg/tritium` is what `tritium-cli`, `tritium-msg` and the external
[tritium-wails](https://github.com/we-be/tritium-wails) desktop client build
on. Treat its exported names as a contract: adding a field or a method is
fine (`ClientOptions.User` shipped that way); changing a signature or
removing an export needs a reason worth breaking every caller for.
`internal/*` carries no such promise — it moves as fast as the node does.

## Style

Comments and commit messages say the domain *why*, not the language
mechanics — read a few files in this tree for the voice before adding to it.
Tests are few and specific: one test per real failure mode, not a parity pin
on wording.
