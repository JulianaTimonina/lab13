package common

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
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

func InjectTrace(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	otel.GetTextMapPropagator().Inject(
		ctx,
		propagation.HeaderCarrier(msg.Header),
	)
}

func ExtractTrace(msg *nats.Msg) context.Context {
	if msg.Header == nil {
		return context.Background()
	}

	return otel.GetTextMapPropagator().Extract(
		context.Background(),
		propagation.HeaderCarrier(msg.Header),
	)
}