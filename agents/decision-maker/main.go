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

		ctx := common.ExtractTrace(msg)

		ctx, span := tracer.Start(ctx, "make-decision")
		defer span.End()

		var input struct {
			Income common.IncomeAnalysis `json:"income"`
			Risk   common.RiskAssessment `json:"risk"`
		}

		if err := json.Unmarshal(msg.Data, &input); err != nil {
			return
		}

		decision := common.Decision{
			Approved: input.Risk.RiskLevel == "low",
			Amount:   input.Income.ApprovedAmount,
			Interest: 5.5,
			Reason:   "Good risk profile",
		}

		resp, _ := json.Marshal(decision)

		reply := &nats.Msg{
			Subject: msg.Reply,
			Data:    resp,
		}

		common.InjectTrace(ctx, reply)

		nc.PublishMsg(reply)
	})
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Decision Maker started")
    select {}
}