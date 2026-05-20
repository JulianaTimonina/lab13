module credit-scoring/income-analyzer

go 1.21

require (
    credit-scoring/common v0.0.0
    github.com/nats-io/nats.go v1.31.0
    go.opentelemetry.io/otel v1.21.0
)

replace credit-scoring/common => ../common