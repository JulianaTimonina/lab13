package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/nats-io/nats.go"
    "credit-scoring/common"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("orchestrator")

func main() {
    ctx := context.Background()
    tp, err := common.InitTracerProvider(ctx, "orchestrator")
    if err != nil {
        log.Fatal(err)
    }
    defer tp.Shutdown(ctx)

    nc, _, err := common.ConnectNATS()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    nc.Subscribe("scoring.request", func(msg *nats.Msg) {
        ctx, span := tracer.Start(ctx, "orchestrator-pipeline")
        defer span.End()

        var req struct {
            ClientID string `json:"client_id"`
        }
        if err := json.Unmarshal(msg.Data, &req); err != nil {
            resp, _ := json.Marshal(map[string]string{"error": "invalid request"})
            nc.Publish(msg.Reply, resp)
            return
        }
        span.SetAttributes(attribute.String("client_id", req.ClientID))

        clientData, err := collectData(ctx, nc, req.ClientID)
        if err != nil {
            nc.Publish(msg.Reply, []byte(`{"error":"data collection failed"}`))
            return
        }

        incomeMap, err := directStep(ctx, nc, "income", clientData)
        if err != nil {
            nc.Publish(msg.Reply, []byte(`{"error":"income analysis failed"}`))
            return
        }
        var incomeAnalysis common.IncomeAnalysis
        mapToStruct(incomeMap, &incomeAnalysis)

        riskMap, err := directStep(ctx, nc, "risk", clientData)
        if err != nil {
            nc.Publish(msg.Reply, []byte(`{"error":"risk assessment failed"}`))
            return
        }
        var riskAssessment common.RiskAssessment
        mapToStruct(riskMap, &riskAssessment)

        decision, err := makeDecision(ctx, nc, incomeAnalysis, riskAssessment)
        if err != nil {
            nc.Publish(msg.Reply, []byte(`{"error":"decision failed"}`))
            return
        }

        explanation, err := requestExplanation(ctx, nc, decision, clientData)
        if err != nil {
            log.Printf("LLM explanation failed: %v", err)
            explanation = "No explanation available"
        }

        result := common.ScoringResult{
            Client:      *clientData,
            Income:      incomeAnalysis,
            Risk:        riskAssessment,
            Decision:    *decision,
            Explanation: explanation,
        }
        resultBytes, _ := json.Marshal(result)
        nc.Publish(msg.Reply, resultBytes)
        saveResult(ctx, result)
    })

    go scalerLoop(ctx, nc)

    log.Println("Orchestrator started")
    select {}
}

func collectData(ctx context.Context, nc *nats.Conn, clientID string) (*common.ClientData, error) {
    ctx, span := tracer.Start(ctx, "collectData")
    defer span.End()
    req, _ := json.Marshal(map[string]string{"client_id": clientID})
    resp, err := nc.Request("data.collect", req, 5*time.Second)
    if err != nil {
        return nil, err
    }
    var data common.ClientData
    err = json.Unmarshal(resp.Data, &data)
    return &data, err
}

func directStep(ctx context.Context, nc *nats.Conn, stepType string, data interface{}) (map[string]interface{}, error) {
    ctx, span := tracer.Start(ctx, "directStep-"+stepType)
    defer span.End()

    doSubject := stepType + ".analyze.do"
    workReq, _ := json.Marshal(data)

    resp, err := nc.Request(doSubject, workReq, 10*time.Second)
    if err != nil {
        return nil, err
    }
    var result map[string]interface{}
    json.Unmarshal(resp.Data, &result)
    return result, nil
}

func makeDecision(ctx context.Context, nc *nats.Conn, income common.IncomeAnalysis, risk common.RiskAssessment) (*common.Decision, error) {
    ctx, span := tracer.Start(ctx, "makeDecision")
    defer span.End()
    input, _ := json.Marshal(map[string]interface{}{"income": income, "risk": risk})
    resp, err := nc.Request("decision.make", input, 5*time.Second)
    if err != nil {
        return nil, err
    }
    var decision common.Decision
    err = json.Unmarshal(resp.Data, &decision)
    return &decision, err
}

func requestExplanation(ctx context.Context, nc *nats.Conn, decision *common.Decision, client *common.ClientData) (string, error) {
    ctx, span := tracer.Start(ctx, "requestExplanation")
    defer span.End()
    input, _ := json.Marshal(map[string]interface{}{"decision": decision, "client": client})
    resp, err := nc.Request("llm.explain.request", input, 15*time.Second)
    if err != nil {
        return "", err
    }
    var expl map[string]string
    json.Unmarshal(resp.Data, &expl)
    return expl["explanation"], nil
}

func saveResult(ctx context.Context, result common.ScoringResult) {
    rdb := common.NewRedisClient()
    data, _ := json.Marshal(result)
    rdb.Set(ctx, "result:"+result.Client.ClientID, data, 24*time.Hour)
    rdb.LPush(ctx, "recent_results", result.Client.ClientID)
    rdb.LTrim(ctx, "recent_results", 0, 19)
}

func scalerLoop(ctx context.Context, nc *nats.Conn) {
    js, _ := nc.JetStream()
    ticker := time.NewTicker(15 * time.Second)
    for range ticker.C {
        info, err := js.ConsumerInfo("SCORING", "income-workers")
        if err == nil {
            pending := info.NumPending
            if pending > 10 {
                log.Printf("Need to scale UP income-analyzer, pending=%d", pending)
            } else if pending < 2 {
                log.Printf("Can scale DOWN income-analyzer, pending=%d", pending)
            }
        }
        info, err = js.ConsumerInfo("SCORING", "risk-workers")
        if err == nil {
            if info.NumPending > 10 {
                log.Printf("Need to scale UP risk-evaluator, pending=%d", info.NumPending)
            } else if info.NumPending < 2 {
                log.Printf("Can scale DOWN risk-evaluator, pending=%d", info.NumPending)
            }
        }
    }
}

func mapToStruct(m map[string]interface{}, s interface{}) {
    data, _ := json.Marshal(m)
    json.Unmarshal(data, s)
}