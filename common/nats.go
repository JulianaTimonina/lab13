package common

import (
    "github.com/nats-io/nats.go"
)

func ConnectNATS() (*nats.Conn, nats.JetStreamContext, error) {
    nc, err := nats.Connect("nats://nats:4222")
    if err != nil {
        return nil, nil, err
    }
    js, err := nc.JetStream()
    if err != nil {
        return nil, nil, err
    }
    // Удаляем старый поток, если он существует (ошибку игнорируем)
    js.DeleteStream("SCORING")
    // Создаём поток с актуальными subject'ами
    _, err = js.AddStream(&nats.StreamConfig{
        Name:      "SCORING",
        Subjects:  []string{"income.analyze.do", "risk.analyze.do", "decision.make", "llm.explain"},
        Retention: nats.WorkQueuePolicy,
    })
    if err != nil {
        return nil, nil, err
    }
    return nc, js, nil
}