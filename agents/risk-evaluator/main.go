package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "sync"
    "time"
    "github.com/nats-io/nats.go"
    "github.com/redis/go-redis/v9"
    "credit-scoring/common"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("risk-evaluator")
var rdb *redis.Client
var stateMutex sync.Mutex
var state RiskState

type RiskState struct {
    TotalEvaluated int64             `json:"total_evaluated"`
    LastEvaluated  time.Time         `json:"last_evaluated"`
    ModelCache     map[string]string `json:"model_cache"`
}

func main() {
    ctx := context.Background()
    tp, err := common.InitTracerProvider(ctx, "risk-evaluator")
    if err != nil {
        log.Fatal(err)
    }
    defer tp.Shutdown(ctx)

    rdb = common.NewRedisClient()
    loadState(ctx)

    nc, _, err := common.ConnectNATS()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    // Аукционная подписка (не используется оркестратором, но оставлена)
    nc.Subscribe("auction.risk.analyze", func(msg *nats.Msg) {
        bid := common.Bid{
            AgentID: "risk-evaluator-" + getHostname(),
            Cost:    0.7,
            Load:    float64(state.TotalEvaluated%10) / 10.0,
        }
        b, _ := json.Marshal(bid)
        nc.Publish(msg.Reply, b)
    })

    // Рабочая очередь
    _, err = nc.QueueSubscribe("risk.analyze.do", "risk-workers", func(msg *nats.Msg) {
		log.Printf("Received risk request: %s", string(msg.Data))
        _, span := tracer.Start(context.Background(), "process-risk-evaluation")
        defer span.End()

        var data common.ClientData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			log.Printf("Unmarshal error: %v", err)
			return
		}
		log.Printf("Parsed client: %+v", data)
        span.SetAttributes(attribute.String("client_id", data.ClientID))

        assessment := common.RiskAssessment{
            RiskScore: 0.2,
            RiskLevel: "low",
            Factors:   []string{"good credit history", "stable income"},
        }
        resp, _ := json.Marshal(assessment)
		log.Printf("Sending assessment: %+v", assessment)
        msg.Respond(resp)

        stateMutex.Lock()
        state.TotalEvaluated++
        state.LastEvaluated = time.Now()
        stateMutex.Unlock()
        saveState(ctx)
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Risk Evaluator started")
    select {}
}

func loadState(ctx context.Context) {
    val, err := rdb.Get(ctx, "risk_evaluator_state").Result()
    if err == nil {
        json.Unmarshal([]byte(val), &state)
    } else {
        state = RiskState{ModelCache: make(map[string]string)}
    }
}

func saveState(ctx context.Context) {
    stateMutex.Lock()
    defer stateMutex.Unlock()
    data, _ := json.Marshal(state)
    rdb.Set(ctx, "risk_evaluator_state", data, 0)
}

func getHostname() string {
    h, _ := os.Hostname()
    return h
}