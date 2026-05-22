package main

import (
    "context"
    "encoding/json"
    "log"
    "github.com/nats-io/nats.go"
    "credit-scoring/common"
    "go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("decision-maker")

func main() {
    ctx := context.Background()
    tp, err := common.InitTracerProvider(ctx, "decision-maker")
    if err != nil {
        log.Fatal(err)
    }
    defer tp.Shutdown(ctx)

    nc, _, err := common.ConnectNATS()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    _, err = nc.QueueSubscribe("decision.make", "decision-workers", func(msg *nats.Msg) {
        log.Printf("Decision request received: %s", string(msg.Data))
		_, span := tracer.Start(context.Background(), "make-decision")
        defer span.End()

        var input struct {
            Income common.IncomeAnalysis `json:"income"`
            Risk   common.RiskAssessment `json:"risk"`
        }
        if err := json.Unmarshal(msg.Data, &input); err != nil {
            log.Printf("invalid input: %v", err)
            return
        }

        decision := common.Decision{
            Approved: input.Risk.RiskLevel == "low",
            Amount:   input.Income.ApprovedAmount,
            Interest: 5.5,
            Reason:   "Good risk profile",
        }
        resp, _ := json.Marshal(decision)
        msg.Respond(resp)
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Decision Maker started")
    select {}
}