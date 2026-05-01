FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /embyr ./cmd/embyr

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /embyr /usr/local/bin/embyr
COPY migrations /migrations
EXPOSE 8080 8081
ENTRYPOINT ["embyr", "--migrations-dir", "/migrations"]
