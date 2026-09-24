.PHONY: default

default: init

init:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2

linter:
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

test:
	go test ./... -bench . -benchmem

test-unit:
	go test -v -short ./...

tidy:
	go mod tidy

build:
	go build ./...

deps:
	go mod download
