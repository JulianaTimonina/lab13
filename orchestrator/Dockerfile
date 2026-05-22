FROM golang:1.21 AS builder
WORKDIR /src
COPY common/ ./common/
COPY orchestrator/ ./orchestrator/
WORKDIR /src/orchestrator
RUN go build -o /app .
FROM debian:stable-slim
COPY --from=builder /app /app
CMD ["/app"]