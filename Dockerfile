FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /firstyr ./cmd/firstyr

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /firstyr /usr/local/bin/firstyr
COPY migrations /migrations
EXPOSE 8080 8081
ENTRYPOINT ["firstyr", "--migrations-dir", "/migrations"]
