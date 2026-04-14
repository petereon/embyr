.PHONY: proto build test lint clean

BINARY=firstyr
MODULE=github.com/firstyr/firstyr

proto:
	buf generate

build:
	CGO_ENABLED=0 go build -o $(BINARY) ./cmd/firstyr

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
	rm -rf gen/go
