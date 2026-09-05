.PHONY: build test integration lint image cluster cluster-down clean

build:
	go build -trimpath -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

# Same tests, against a real server. Start one with:
#   podman run --rm -p 6379:6379 valkey/valkey:8-alpine valkey-server --save "" --appendonly no
integration:
	TRITIUM_RESP_ADDR=$${TRITIUM_RESP_ADDR:-localhost:6379} go test -race -count=1 ./...

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
	rm -rf bin logs
