package main

import (
	"context"
	"credit-scoring/common"
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
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

	hostname := getHostname()
	agentID := "risk-evaluator-" + hostname

	nc.Subscribe("auction.risk.analyze", func(msg *nats.Msg) {
		load := math.Min(float64(state.TotalEvaluated%10)/10.0, 1.0)
		skill, availability := riskBidFactors(msg.Data, load)
		cost := 0.45 + load*0.5 + (1.0-skill)*0.12

		bid := common.Bid{
			AgentID:      agentID,
			Cost:         cost,
			Load:         load,
			Skill:        skill,
			Availability: availability,
		}
		b, _ := json.Marshal(bid)
		if msg.Reply != "" {
			nc.Publish(msg.Reply, b)
		}
	})

	agentSubject := "scoring.risk.agent." + hostname
	_, err = nc.Subscribe(agentSubject, func(msg *nats.Msg) {
		handleRiskWork(nc, msg)
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = nc.QueueSubscribe("scoring.risk.do", "risk-workers", func(msg *nats.Msg) {
		handleRiskWork(nc, msg)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Risk Evaluator started (agent=%s, subject=%s)", agentID, agentSubject)
	select {}
}

func riskBidFactors(payload []byte, load float64) (skill, availability float64) {
	skill = 0.78
	availability = math.Max(0, 1.0-load*0.9)

	var task common.AuctionRequest
	if err := json.Unmarshal(payload, &task); err != nil {
		return skill, availability
	}
	raw, err := json.Marshal(task.Data)
	if err != nil {
		return skill, availability
	}
	var client common.ClientData
	if err := json.Unmarshal(raw, &client); err != nil {
		return skill, availability
	}

	switch client.CreditHistory {
	case "good":
		skill += 0.15
	case "average":
		skill += 0.05
	default:
		skill -= 0.05
	}
	if client.Age >= 25 && client.Age <= 55 {
		skill += 0.05
	}
	if skill > 1 {
		skill = 1
	}
	if skill < 0.3 {
		skill = 0.3
	}
	return skill, availability
}

func handleRiskWork(nc *nats.Conn, msg *nats.Msg) {
	time.Sleep(2 * time.Second)

	ctx := common.ExtractTrace(msg)
	ctx, span := tracer.Start(ctx, "process-risk-evaluation")
	defer span.End()

	var data common.ClientData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return
	}

	span.SetAttributes(attribute.String("client_id", data.ClientID))

	assessment := common.RiskAssessment{
		RiskScore: 0.2,
		RiskLevel: "low",
		Factors: []string{
			"good credit history",
			"stable income",
		},
	}

	resp, _ := json.Marshal(assessment)
	reply := &nats.Msg{Subject: msg.Reply, Data: resp}
	common.InjectTrace(ctx, reply)
	nc.PublishMsg(reply)

	stateMutex.Lock()
	state.TotalEvaluated++
	state.LastEvaluated = time.Now()
	stateMutex.Unlock()
	saveState(ctx)
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
