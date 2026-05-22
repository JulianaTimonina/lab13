package main

import (
    "context"
    "encoding/json"
    "log"
    "math/rand"
    "os"
    "sync/atomic"
    "github.com/nats-io/nats.go"
    "credit-scoring/common"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("income-analyzer")
var currentLoad int64

func main() {
    ctx := context.Background()
    tp, err := common.InitTracerProvider(ctx, "income-analyzer")
    if err != nil {
        log.Fatal(err)
    }
    defer tp.Shutdown(ctx)

    nc, _, err := common.ConnectNATS()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    // Аукционная подписка (оставлена для демонстрации, но оркестратор её сейчас не использует)
    nc.Subscribe("auction.income.analyze", func(msg *nats.Msg) {
        load := float64(atomic.LoadInt64(&currentLoad)) / 10.0
        cost := 0.5 + rand.Float64()*0.3
        bid := common.Bid{
            AgentID: "income-analyzer-" + getHostname(),
            Cost:    cost,
            Load:    load,
        }
        bidBytes, _ := json.Marshal(bid)
        nc.Publish(msg.Reply, bidBytes)
    })

    _, err = nc.QueueSubscribe("income.analyze.do", "income-workers", func(msg *nats.Msg) {

		atomic.AddInt64(&currentLoad, 1)
		defer atomic.AddInt64(&currentLoad, -1)

		ctx := common.ExtractTrace(msg)

		ctx, span := tracer.Start(ctx, "process-income-analysis")
		defer span.End()

		var data common.ClientData

		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return
		}

		span.SetAttributes(
			attribute.String("client_id", data.ClientID),
		)

		analysis := common.IncomeAnalysis{
			StabilityScore: 0.85,
			DebtToIncome:   data.Income * 0.3 / 5000,
			ApprovedAmount: data.Income * 3,
		}

		resp, _ := json.Marshal(analysis)

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

    log.Println("Income Analyzer started")
    select {}
}

func getHostname() string {
    h, _ := os.Hostname()
    return h
}