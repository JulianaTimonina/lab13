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
    // Проверяем существует ли stream
	_, err = js.StreamInfo("SCORING")

	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name: "SCORING",
			Subjects: []string{
				"events.>",
			},
			Retention: nats.WorkQueuePolicy,
		})

		if err != nil {
			return nil, nil, err
		}
	}
    return nc, js, nil
}