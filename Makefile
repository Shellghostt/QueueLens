BINARY := queuelens
VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Local infrastructure. The compose file maps PostgreSQL to 55433 and Redis to
# 56379 so they cannot collide with a database that is already running.
POSTGRES_DSN ?= postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens
REDIS_URL ?= redis://127.0.0.1:56379/0

.PHONY: bootstrap build test race integration bench loadtest m3-load m4-bench lint fmt vet memory-check run docker db-up db-down fixture sdk-test sdk-bench web-install web-test web-build web-check clean

bootstrap:
	go mod download
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

build: web-build
	go build -tags dashboard -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/queuelens

web-install:
	cd web && npm ci

web-test: web-install
	cd web && npm run lint && npm run typecheck && npm test && npm run memory:check

web-build: web-install
	cd web && npm run build && npm run check:assets

web-check: web-test web-build
	cd web && npm audit


test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

# The integration suite needs a real PostgreSQL and a real Redis. The BullMQ
# fixture additionally needs Node and its dependencies.
integration: sdk-test
	go test -count=1 -v ./tests/integration
integration: export QUEUELENS_TEST_DATABASE_URL = $(POSTGRES_DSN)
integration: export QUEUELENS_TEST_REDIS_URL = $(REDIS_URL)

fixture:
	cd tests/integration/bullmq && npm install

sdk-test:
	cd sdk/typescript && npm ci && npm test
	go test -count=1 ./sdk/go ./pkg/protocol

sdk-bench:
	cd sdk/typescript && npm ci && npm run bench
	go test -count=1 -timeout=120s -v ./tests/bench
sdk-bench: export QUEUELENS_RUN_BENCH = 1

# MemGuard is a static analyser for JavaScript memory and resource problems.
# It is pinned as a development dependency of the BullMQ fixture and runs in
# CI. It reads source; it does not observe runtime behaviour.
memory-check:
	cd tests/integration/bullmq && npm run memory:check

bench:
	go test ./internal/store -bench WriteBatch1000 -benchtime 20x -run XXX
bench: export QUEUELENS_TEST_DATABASE_URL = $(POSTGRES_DSN)

# The five minute ingest run from docs/benchmarks/m1-ingest-5000eps.md.
loadtest: build
	./bin/$(BINARY) loadgen --url http://127.0.0.1:8080 --rate 5000 --duration 5m --workers 4 --batch 100

# Isolated synthetic HTTP/storage/projection run; retains no user database rows.
m3-load: export QUEUELENS_RUN_M3_LOAD = 1
m3-load: export QUEUELENS_TEST_DATABASE_URL = $(POSTGRES_DSN)
m3-load:
	go test -count=1 -timeout=180s -v ./tests/integration -run "^TestLifecycleSustainedProjectionMemoryAndLag$$"

m4-bench: export QUEUELENS_RUN_M4_BENCH = 1
m4-bench: export QUEUELENS_TEST_DATABASE_URL = $(POSTGRES_DSN)
m4-bench:
	go test -count=1 -timeout=180s -v ./tests/integration -run "^TestDashboard(ThirtyDayQueryBudget|MetricWorkLimit)$$"

lint: fmt vet
	golangci-lint run ./...

fmt:
	go fmt ./...

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
