FROM golang:1.21 AS builder
WORKDIR /src
COPY common/ ./common/
COPY agents/data-collector/ ./agent/
WORKDIR /src/agent
RUN go mod tidy
RUN go build -o /app .
FROM debian:stable-slim
COPY --from=builder /app /app
CMD ["/app"]