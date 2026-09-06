.PHONY: build dist test integration chaos lint image cluster cluster-down clean

VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS = -s -w -X github.com/we-be/tritium/internal/server.Version=$(VERSION)
PLATFORMS = linux/amd64 linux/arm64 linux/arm darwin/arm64 darwin/amd64   # linux/arm is GOARM=6: a Pi Zero

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

# Every binary for every platform, one tarball each, in dist/. Pure Go, so no
# cross toolchains: this is what the release workflow publishes.
dist:
	rm -rf dist && mkdir -p dist
	for p in $(PLATFORMS); do \
	  os=$${p%/*}; arch=$${p#*/}; out=dist/tritium-$(VERSION)-$$os-$$arch; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch GOARM=6 go build -trimpath -ldflags "$(LDFLAGS)" -o $$out/ ./cmd/... || exit 1; \
	  tar -C dist -czf $$out.tar.gz $$(basename $$out) && rm -r $$out; \
	done
	ls -l dist

test:
	go test -race -count=1 ./...

# Same tests, against a real server. Start one with:
#   podman run --rm -p 6379:6379 valkey/valkey:8-alpine valkey-server --save "" --appendonly no
integration:
	TRITIUM_RESP_ADDR=$${TRITIUM_RESP_ADDR:-localhost:6379} go test -race -count=1 ./...

# A long chaos run: nodes killed and restarted at random under writes, then convergence
# checked. TRITIUM_CHAOS_SEED=<n> replays one.
chaos:
	TRITIUM_CHAOS_SECONDS=$${TRITIUM_CHAOS_SECONDS:-30} go test -race -count=1 -timeout 5m -run TestChaos -v ./internal/server/

lint:
	test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	test -z "$$(go fix -diff ./...)" || { go fix -diff ./...; exit 1; }

image:
	podman build -t tritium .

cluster: build
	./start-cluster.sh

cluster-down:
	./stop-cluster.sh

clean:
	rm -rf bin dist logs
