BASE_URL ?= http://localhost:8787
ADMIN_TOKEN ?= admin-dev-token
DATABASE_URL ?= postgres://seats:seats@127.0.0.1:5432/seats?sslmode=disable

.PHONY: build run test burst burst-small docker-up docker-down docker-burst fmt vet

build:
	go build -o bin/server ./cmd/server
	go build -o bin/burst ./cmd/burst

run:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/server

test:
	DATABASE_URL=$(DATABASE_URL) go test ./... -count=1 -race

## Reproduce the on-sale stampede against BASE_URL and print the outcome distribution.
##   make burst BASE_URL=https://your-live-url ADMIN_TOKEN=...
burst:
	ADMIN_TOKEN=$(ADMIN_TOKEN) go run ./cmd/burst -url $(BASE_URL) $(BURST_ARGS)

## Lighter variant for free-tier instances (fewer requests, lower concurrency).
burst-small:
	ADMIN_TOKEN=$(ADMIN_TOKEN) go run ./cmd/burst -url $(BASE_URL) -requests 3000 -concurrency 150 -hot-users 200 -users 500 -seats 300 $(BURST_ARGS)

docker-up:
	docker compose up --build -d
	@echo "waiting for readiness..."; for i in $$(seq 1 30); do curl -fsS localhost:8787/readyz >/dev/null 2>&1 && break; sleep 1; done; curl -s localhost:8787/readyz; echo

docker-down:
	docker compose down -v

## Run the burst from inside the container network (no Go toolchain needed).
docker-burst:
	docker compose run --rm --entrypoint /burst api -url http://api:8787 -admin-token $(ADMIN_TOKEN) $(BURST_ARGS)

fmt:
	gofmt -w ./cmd ./internal

vet:
	go vet ./...
