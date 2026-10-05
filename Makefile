.PHONY: build test test-short

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null)
LDFLAGS := -X github.com/turbolytics/dbhealth/internal/report.Version=$(VERSION) -X github.com/turbolytics/dbhealth/internal/report.Commit=$(COMMIT)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/dbhealth ./cmd/dbhealth

test:
	go test -race ./...

test-short:
	go test -short ./...
