package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"credit-scoring/common"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("orchestrator")

// Параметры масштабирования
const (
	maxIncomeReplicas = 5
	maxRiskReplicas   = 5
	incomeImage       = "lab13-income-analyzer:latest" // должно совпадать с build в compose
	riskImage         = "lab13-risk-evaluator:latest"
	networkName       = "scoring_net" // фиксированное имя сети (из docker-compose)
)

const (
    activeIncomeKey = "scaler:active_income"
    activeRiskKey   = "scaler:active_risk"
)

// Docker HTTP client
var dockerHTTP *http.Client

func init() {
	// Создаём клиент, общающийся через Unix-сокет Docker
	dockerHTTP = &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", "/var/run/docker.sock")
			},
		},
	}
}

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
	
	if err := ensureStream(nc); err != nil {
		log.Fatalf("Stream setup failed: %v", err)
	}

	// Запуск цикла автомасштабирования
	go scalerLoop(nc)
	// Запуск REST API для тестирования
	go startRESTServer(nc)

	// Основная подписка на задачи (без изменений)
	nc.Subscribe("scoring.request", func(msg *nats.Msg) {
		ctx := common.ExtractTrace(msg)
		ctx, span := tracer.Start(ctx, "orchestrator-pipeline")
		defer span.End()

		var req struct {
			ClientID string `json:"client_id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp, _ := json.Marshal(map[string]string{"error": "invalid request"})
			reply := &nats.Msg{Subject: msg.Reply, Data: resp}
			common.InjectTrace(ctx, reply)
			nc.PublishMsg(reply)
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
		if err := mapToStruct(incomeMap, &incomeAnalysis); err != nil {
			nc.Publish(msg.Reply, []byte(`{"error":"income parse failed"}`))
			return
		}

		riskMap, err := directStep(ctx, nc, "risk", clientData)
		if err != nil {
			nc.Publish(msg.Reply, []byte(`{"error":"risk assessment failed"}`))
			return
		}
		var riskAssessment common.RiskAssessment
		if err := mapToStruct(riskMap, &riskAssessment); err != nil {
			nc.Publish(msg.Reply, []byte(`{"error":"risk parse failed"}`))
			return
		}

		decision, err := makeDecision(ctx, nc, incomeAnalysis, riskAssessment)
		if err != nil {
			nc.Publish(msg.Reply, []byte(`{"error":"decision failed"}`))
			return
		}

		explanation, err := requestExplanation(ctx, nc, decision, clientData)
		if err != nil {
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
		reply := &nats.Msg{Subject: msg.Reply, Data: resultBytes}
		common.InjectTrace(ctx, reply)
		nc.PublishMsg(reply)
		saveResult(ctx, result)
	})

	log.Println("Orchestrator started")

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

// ---------- Функции пайплайна (без изменений) ----------
func collectData(ctx context.Context, nc *nats.Conn, clientID string) (*common.ClientData, error) {
	ctx, span := tracer.Start(ctx, "collectData")
	defer span.End()
	req, _ := json.Marshal(map[string]string{"client_id": clientID})
	msg := &nats.Msg{Subject: "data.collect", Data: req}
	common.InjectTrace(ctx, msg)
	resp, err := nc.RequestMsg(msg, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var data common.ClientData
	err = json.Unmarshal(resp.Data, &data)
	return &data, err
}

func directStep(
    ctx context.Context,
    nc *nats.Conn,
    stepType string,
    data interface{},
) (map[string]interface{}, error) {

    ctx, span := tracer.Start(ctx, "directStep-"+stepType)
    defer span.End()

    // ---------- Добавленный блок: учёт активных задач ----------
    rdb := common.NewRedisClient()
    activeKey := activeIncomeKey
    if stepType == "risk" {
        activeKey = activeRiskKey
    }
    // Увеличиваем счётчик – сейчас началась обработка
    rdb.Incr(ctx, activeKey)
    // Гарантируем, что после выхода из функции счётчик уменьшится
    defer rdb.Decr(ctx, activeKey)
    // -----------------------------------------------------------

    doSubject := stepType + ".analyze.do"
    workReq, _ := json.Marshal(data)

    msg := &nats.Msg{
        Subject: doSubject,
        Data:    workReq,
    }

    common.InjectTrace(ctx, msg)

    resp, err := nc.RequestMsg(msg, 10*time.Second)
    if err != nil {
        return nil, err
    }

    var result map[string]interface{}

    if err := json.Unmarshal(resp.Data, &result); err != nil {
        return nil, err
    }

    return result, nil
}

func makeDecision(ctx context.Context, nc *nats.Conn, income common.IncomeAnalysis, risk common.RiskAssessment) (*common.Decision, error) {
	ctx, span := tracer.Start(ctx, "makeDecision")
	defer span.End()
	input, _ := json.Marshal(map[string]interface{}{"income": income, "risk": risk})
	msg := &nats.Msg{Subject: "decision.make", Data: input}
	common.InjectTrace(ctx, msg)
	resp, err := nc.RequestMsg(msg, 5*time.Second)
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
	msg := &nats.Msg{Subject: "llm.explain.request", Data: input, Header: nats.Header{}}
	common.InjectTrace(ctx, msg)
	resp, err := nc.RequestMsg(msg, 15*time.Second)
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

func mapToStruct(m map[string]interface{}, s interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, s)
}

// ---------- Автомасштабирование через Docker HTTP API ----------
func scalerLoop(nc *nats.Conn) {
    rdb := common.NewRedisClient()
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        // income-analyzer
        valIncome, err := rdb.Get(context.Background(), activeIncomeKey).Int()
        if err != nil {
            valIncome = 0
        }
        currentIncome, _ := countContainers("income-analyzer")
        log.Printf("income-analyzer: active=%d, running=%d", valIncome, currentIncome)

        if valIncome > 10 && currentIncome < maxIncomeReplicas {
            if err := scaleUp("income-analyzer", incomeImage); err != nil {
                log.Printf("Scale UP income-analyzer failed: %v", err)
            }
        } else if valIncome < 2 && currentIncome > 1 {
            if err := scaleDown("income-analyzer"); err != nil {
                log.Printf("Scale DOWN income-analyzer failed: %v", err)
            }
        }

        // risk-evaluator
        valRisk, err := rdb.Get(context.Background(), activeRiskKey).Int()
        if err != nil {
            valRisk = 0
        }
        currentRisk, _ := countContainers("risk-evaluator")
        log.Printf("risk-evaluator: active=%d, running=%d", valRisk, currentRisk)

        if valRisk > 10 && currentRisk < maxRiskReplicas {
            if err := scaleUp("risk-evaluator", riskImage); err != nil {
                log.Printf("Scale UP risk-evaluator failed: %v", err)
            }
        } else if valRisk < 2 && currentRisk > 1 {
            if err := scaleDown("risk-evaluator"); err != nil {
                log.Printf("Scale DOWN risk-evaluator failed: %v", err)
            }
        }
    }
}

// countContainers возвращает количество работающих контейнеров с заданной меткой сервиса.
func countContainers(serviceName string) (int, error) {
	// Запрос к Docker API: GET /containers/json?all=true
	resp, err := dockerHTTP.Get("http://localhost/containers/json?all=true")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("docker API returned %d", resp.StatusCode)
	}
	var containers []struct {
		Labels map[string]string `json:"Labels"`
		State  string            `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return 0, err
	}
	count := 0
	for _, c := range containers {
		if c.Labels["com.docker.compose.service"] == serviceName && c.State == "running" {
			count++
		}
	}
	return count, nil
}

// scaleUp создаёт и запускает ещё один контейнер агента.
func scaleUp(serviceName, imageName string) error {
	log.Printf("Scaling UP %s", serviceName)
	containerName := fmt.Sprintf("%s-%d", serviceName, time.Now().UnixNano())

	createBody := map[string]interface{}{
		"Image": imageName,
		"Env": []string{
			"NATS_URL=nats://nats:4222",
			"REDIS_URL=redis:6379",
			"JAEGER_ENDPOINT=jaeger:4317",
		},
		"Labels": map[string]string{
			"com.docker.compose.service": serviceName,
		},
		"HostConfig": map[string]interface{}{
			"NetworkMode": networkName,
			"AutoRemove":  true,
		},
	}

	jsonBody, _ := json.Marshal(createBody)

	// Создание контейнера: POST /containers/create?name=...
	url := fmt.Sprintf("http://localhost/containers/create?name=%s", containerName)
	resp, err := dockerHTTP.Post(url, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create container failed: %s", body)
	}

	var createResp struct {
		ID string `json:"Id"`
	}
	json.NewDecoder(resp.Body).Decode(&createResp)

	// Запуск контейнера: POST /containers/{id}/start
	startURL := fmt.Sprintf("http://localhost/containers/%s/start", createResp.ID)
	startResp, err := dockerHTTP.Post(startURL, "application/json", nil)
	if err != nil {
		return fmt.Errorf("start container: %w", err)
	}
	startResp.Body.Close()
	if startResp.StatusCode != http.StatusNoContent && startResp.StatusCode != http.StatusOK {
		return fmt.Errorf("start container failed: %d", startResp.StatusCode)
	}

	log.Printf("Started container %s (ID: %s)", containerName, createResp.ID[:12])
	return nil
}

// scaleDown останавливает и удаляет один лишний контейнер агента.
func scaleDown(serviceName string) error {
	containers, err := countContainersDetailed(serviceName)
	if err != nil {
		return err
	}
	if len(containers) == 0 {
		return fmt.Errorf("no running container for %s", serviceName)
	}
	// Останавливаем первый попавшийся
	id := containers[0]
	log.Printf("Scaling DOWN %s: stopping container %s", serviceName, id[:12])

	// POST /containers/{id}/stop
	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=10", id)
	stopResp, err := dockerHTTP.Post(stopURL, "application/json", nil)
	if err != nil {
		return fmt.Errorf("stop container: %w", err)
	}
	stopResp.Body.Close()
	if stopResp.StatusCode != http.StatusNoContent && stopResp.StatusCode != http.StatusOK {
		return fmt.Errorf("stop container failed: %d", stopResp.StatusCode)
	}
	// Контейнер автоматически удалится из-за AutoRemove = true
	return nil
}

// countContainersDetailed возвращает ID всех работающих контейнеров сервиса.
func countContainersDetailed(serviceName string) ([]string, error) {
	resp, err := dockerHTTP.Get("http://localhost/containers/json?all=true")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("docker API returned %d", resp.StatusCode)
	}
	var containers []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
		State  string            `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, err
	}
	var ids []string
	for _, c := range containers {
		if c.Labels["com.docker.compose.service"] == serviceName && c.State == "running" {
			ids = append(ids, c.ID)
		}
	}
	return ids, nil
}

func startRESTServer(nc *nats.Conn) {
    http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "POST only", http.StatusMethodNotAllowed)
            return
        }
        body, _ := io.ReadAll(r.Body)
        log.Printf("REST received body: %s", string(body))
        var req struct{ ClientID string `json:"client_id"` }
        if err := json.Unmarshal(body, &req); err != nil {
            log.Printf("JSON parse error: %v", err)
            http.Error(w, "invalid JSON", http.StatusBadRequest)
            return
        }
        if req.ClientID == "" {
            log.Printf("client_id empty")
            http.Error(w, "client_id required", http.StatusBadRequest)
            return
        }
        taskID := fmt.Sprintf("%d", time.Now().UnixNano())
        task := map[string]string{"client_id": req.ClientID}
        data, _ := json.Marshal(task)
        nc.Publish("scoring.request", data)
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]string{"task_id": taskID})
    })
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func ensureStream(nc *nats.Conn) error {
    js, err := nc.JetStream()
    if err != nil {
        return fmt.Errorf("failed to get JetStream context: %w", err)
    }

    // Создаём стрим, если его нет
    _, err = js.StreamInfo("SCORING")
    if err != nil {
        streamConfig := &nats.StreamConfig{
            Name:      "SCORING",
            Subjects:  []string{"scoring.>"},
            Retention: nats.LimitsPolicy,
            MaxMsgs:   10000,
            MaxBytes:  -1,
            Discard:   nats.DiscardOld,
            Storage:   nats.FileStorage,
        }
        _, err = js.AddStream(streamConfig)
        if err != nil {
            return fmt.Errorf("failed to create stream: %w", err)
        }
        log.Println("Stream SCORING created")
    } else {
        log.Println("Stream SCORING already exists")
    }

    // Создаём durable-консьюмеров, если их нет
    consumers := map[string]string{
        "income-workers": "income.analyze.do",
        "risk-workers":   "risk.analyze.do",
    }

    for consumerName, filterSubject := range consumers {
        _, err := js.ConsumerInfo("SCORING", consumerName)
        if err != nil {
            // Консьюмер не найден – создаём
            _, err = js.AddConsumer("SCORING", &nats.ConsumerConfig{
                Durable:        consumerName,
                FilterSubject:  filterSubject,
                AckPolicy:      nats.AckExplicitPolicy,
                DeliverPolicy:  nats.DeliverAllPolicy,
                ReplayPolicy:   nats.ReplayInstantPolicy,
                MaxDeliver:     -1,
                AckWait:        30 * time.Second,
            })
            if err != nil {
                return fmt.Errorf("failed to create consumer %s: %w", consumerName, err)
            }
            log.Printf("Consumer %s created", consumerName)
        } else {
            log.Printf("Consumer %s already exists", consumerName)
        }
    }

    return nil
}