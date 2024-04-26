VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-neo4j.version=${VERSION}'" -o conduit-connector-neo4j cmd/connector/main.go

.PHONY: test
test:
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down --volumes; \
		exit $$ret

.PHONY: lint
lint:
	golangci-lint run

.PHONY: mockgen
mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
	mockgen -package mock -source source/source.go -destination source/mock/source.go

.PHONY: paramgen
paramgen:
	paramgen -path=./destination -output=destination_params.go Config
	paramgen -path=./source -output=source_params.go Config

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
