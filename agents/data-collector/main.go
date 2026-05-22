package main

import (
    "context"
    "encoding/json"
    "log"
    "github.com/nats-io/nats.go"
    "credit-scoring/common"
    "go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("data-collector")

func main() {
    ctx := context.Background()
    tp, err := common.InitTracerProvider(ctx, "data-collector")
    if err != nil {
        log.Fatal(err)
    }
    defer tp.Shutdown(ctx)

    nc, _, err := common.ConnectNATS()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    _, err = nc.Subscribe("data.collect", func(msg *nats.Msg) {
        _, span := tracer.Start(ctx, "handle-data-collect")
        defer span.End()

        var req struct {
            ClientID string `json:"client_id"`
        }
        if err := json.Unmarshal(msg.Data, &req); err != nil {
            nc.Publish(msg.Reply, []byte(`{"error": "invalid request"}`))
            return
        }

        data := common.ClientData{
            ClientID:       req.ClientID,
            Age:            35,
            Income:         5500.0,
            EmploymentType: "full-time",
            CreditHistory:  "good",
        }
        resp, _ := json.Marshal(data)
        msg.Respond(resp)
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Data Collector started")
    select {}
}