.PHONY: proto build test lint clean

BINARY=embyr
MODULE=github.com/petereon/embyr

proto:
	buf generate

build:
	CGO_ENABLED=0 go build -o $(BINARY) ./cmd/embyr

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
	rm -rf gen/go
