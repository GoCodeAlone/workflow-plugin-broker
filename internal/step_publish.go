package internal

import (
	"context"
	"encoding/json"
	"fmt"
)

// PublishStep implements step.broker_publish.
// Config: topic (string), payload (string or map serialized to JSON).
type PublishStep struct {
	broker Publisher
}

// NewPublishStep creates a step.broker_publish executor.
func NewPublishStep(broker Publisher) *PublishStep {
	return &PublishStep{broker: broker}
}

// Execute publishes data to the broker topic extracted from params.
// Params:
//
//	topic   (string, required) — NATS subject to publish to.
//	payload (string|map|any)  — message body; maps are JSON-encoded.
func (s *PublishStep) Execute(ctx context.Context, params map[string]any) (map[string]any, error) {
	topic, _ := params["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("step.broker_publish: topic is required")
	}

	var data []byte
	switch v := params["payload"].(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		if params["payload"] == nil {
			data = []byte("{}")
		} else {
			var err error
			data, err = json.Marshal(params["payload"])
			if err != nil {
				return nil, fmt.Errorf("step.broker_publish: marshal payload: %w", err)
			}
		}
	}

	if err := s.broker.Publish(topic, data); err != nil {
		return nil, fmt.Errorf("step.broker_publish: %w", err)
	}

	return map[string]any{
		"topic":    topic,
		"bytes":    len(data),
		"published": true,
	}, nil
}
