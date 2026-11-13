BINARY := queuelens
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Local infrastructure. The compose file maps PostgreSQL to 55433 and Redis to
# 56379 so they cannot collide with a database that is already running.
POSTGRES_DSN ?= postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens
REDIS_URL ?= redis://127.0.0.1:56379/0

.PHONY: bootstrap build test race integration bench loadtest lint fmt vet memory-check run docker db-up db-down fixture clean

bootstrap:
	go mod download
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/queuelens


test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

# The integration suite needs a real PostgreSQL and a real Redis. The BullMQ
# fixture additionally needs Node and its dependencies.
integration:
	QUEUELENS_TEST_DATABASE_URL=$(POSTGRES_DSN) QUEUELENS_TEST_REDIS_URL=$(REDIS_URL) go test -count=1 -v ./tests/integration

fixture:
	cd tests/integration/bullmq && npm install

# MemGuard is a static analyser for JavaScript memory and resource problems.
# It is pinned as a development dependency of the BullMQ fixture and runs in
# CI. It reads source; it does not observe runtime behaviour.
memory-check:
	cd tests/integration/bullmq && npm run memory:check

bench:
	QUEUELENS_TEST_DATABASE_URL=$(POSTGRES_DSN) go test ./internal/store -bench WriteBatch1000 -benchtime 20x -run XXX

# The five minute ingest run from docs/benchmarks/m1-ingest-5000eps.md.
loadtest: build
	./bin/$(BINARY) loadgen --url http://127.0.0.1:8080 --rate 5000 --duration 5m --workers 4 --batch 100

lint: fmt vet
	golangci-lint run ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

run: build
	./bin/$(BINARY) serve

docker:
	docker build -t queuelens:$(VERSION) .

db-up:
	docker compose -f deploy/docker-compose.yml up -d postgres redis

db-down:
	docker compose -f deploy/docker-compose.yml down -v

clean:
	go clean
	rm -rf bin
