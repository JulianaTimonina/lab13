package main

import (
	"context"
	"credit-scoring/common"
	"encoding/json"
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
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

	hostname := getHostname()
	agentID := "income-analyzer-" + hostname

	nc.Subscribe("auction.income.analyze", func(msg *nats.Msg) {
		load := math.Min(float64(atomic.LoadInt64(&currentLoad))/10.0, 1.0)
		skill, availability := incomeBidFactors(msg.Data, load)
		cost := 0.35 + load*0.55 + (1.0-skill)*0.15

		bid := common.Bid{
			AgentID:      agentID,
			Cost:         cost,
			Load:         load,
			Skill:        skill,
			Availability: availability,
		}
		bidBytes, _ := json.Marshal(bid)
		if msg.Reply != "" {
			nc.Publish(msg.Reply, bidBytes)
		}
	})

	agentSubject := "scoring.income.agent." + hostname
	_, err = nc.Subscribe(agentSubject, func(msg *nats.Msg) {
		handleIncomeWork(nc, msg)
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = nc.QueueSubscribe("scoring.income.do", "income-workers", func(msg *nats.Msg) {
		handleIncomeWork(nc, msg)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Income Analyzer started (agent=%s, subject=%s)", agentID, agentSubject)
	select {}
}

func incomeBidFactors(payload []byte, load float64) (skill, availability float64) {
	skill = 0.75
	availability = math.Max(0, 1.0-load)

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

	skill = 0.72
	switch client.EmploymentType {
	case "full-time", "full_time", "government":
		skill += 0.18
	case "contract", "part-time":
		skill += 0.08
	default:
		skill += 0.02
	}
	if client.Income >= 5000 {
		skill += 0.05
	}
	if client.CreditHistory == "good" {
		skill += 0.05
	}
	if skill > 1 {
		skill = 1
	}
	return skill, availability
}

func handleIncomeWork(nc *nats.Conn, msg *nats.Msg) {
	time.Sleep(2 * time.Second)

	atomic.AddInt64(&currentLoad, 1)
	defer atomic.AddInt64(&currentLoad, -1)

	ctx := common.ExtractTrace(msg)
	ctx, span := tracer.Start(ctx, "process-income-analysis")
	defer span.End()

	var data common.ClientData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return
	}

	span.SetAttributes(attribute.String("client_id", data.ClientID))

	analysis := common.IncomeAnalysis{
		StabilityScore: 0.85,
		DebtToIncome:   data.Income * 0.3 / 5000,
		ApprovedAmount: data.Income * 3,
	}

	resp, _ := json.Marshal(analysis)
	reply := &nats.Msg{Subject: msg.Reply, Data: resp}
	common.InjectTrace(ctx, reply)
	nc.PublishMsg(reply)
}

func getHostname() string {
	h, _ := os.Hostname()
	return h
}
