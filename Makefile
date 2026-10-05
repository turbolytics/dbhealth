.PHONY: build test test-short test-integration test-e2e fmt-check vet image release-test

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null)
LDFLAGS := -X github.com/turbolytics/dbhealth/internal/report.Version=$(VERSION) -X github.com/turbolytics/dbhealth/internal/report.Commit=$(COMMIT)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/dbhealth ./cmd/dbhealth

# Every test, the ones that start Postgres through testcontainers included.
test:
	go test -race ./...

# The unit layer: no Docker, and every package's tests finish inside ten
# seconds or the run fails.
test-short:
	go test -short -timeout 10s ./...

# One database kind's tests against a real one in testcontainers. KIND is
# the package under internal/: postgres today.
KIND ?= postgres
test-integration:
	go test -race -count=1 ./internal/$(KIND)/

# Against a real Postgres and a local control; see test/e2e/e2e_test.go
# for the DBHEALTH_E2E_* variables it needs.
test-e2e:
	DBHEALTH_E2E=1 go test -count=1 -v ./test/e2e/

fmt-check:
	@test -z "$$(gofmt -l cmd internal $(wildcard test))" || { echo "gofmt these:"; gofmt -l cmd internal $(wildcard test); exit 1; }

vet:
	go vet ./...

IMAGE ?= dbhealth:dev
image:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE) .

# The image, run against a real Postgres: validate, then a few intervals
# of run with StatsD, and the gauges have to say the probe answered and
# the tables were read.
release-test: image
	./scripts/release-test.sh $(IMAGE)
