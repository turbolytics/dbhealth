.PHONY: build test test-short

build:
	go build -o bin/dbhealth ./cmd/dbhealth

test:
	go test -race ./...

test-short:
	go test -short ./...
