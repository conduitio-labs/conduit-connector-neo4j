.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-neo4j.version=${VERSION}'" -o conduit-connector-neo4j cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run --config .golangci.yml
