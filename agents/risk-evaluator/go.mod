module credit-scoring/risk-evaluator

go 1.21

require (
    credit-scoring/common v0.0.0
    github.com/nats-io/nats.go v1.31.0
    github.com/redis/go-redis/v9 v9.0.5
    go.opentelemetry.io/otel v1.21.0
)

replace credit-scoring/common => ../common