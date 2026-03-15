package internal

import (
	"context"
	"fmt"
)

// SubscribeStep implements step.broker_subscribe.
// It is a trigger-mode step: starts a durable JetStream subscription and
// invokes the handler callback for each inbound message.
type SubscribeStep struct {
	broker Subscriber
}

// NewSubscribeStep creates a step.broker_subscribe executor.
func NewSubscribeStep(broker Subscriber) *SubscribeStep {
	return &SubscribeStep{broker: broker}
}

// Subscribe sets up a durable subscription for the given topic and consumer name.
// Each message is delivered to handler as raw bytes.
// Params:
//
//	topic         (string, required) — NATS subject filter (wildcards supported).
//	consumer_name (string, required) — durable consumer name for JetStream replay.
func (s *SubscribeStep) Subscribe(
	ctx context.Context,
	params map[string]any,
	handler func(data []byte),
) error {
	topic, _ := params["topic"].(string)
	if topic == "" {
		return fmt.Errorf("step.broker_subscribe: topic is required")
	}
	consumerName, _ := params["consumer_name"].(string)
	if consumerName == "" {
		return fmt.Errorf("step.broker_subscribe: consumer_name is required")
	}
	return s.broker.SubscribeDurable(topic, consumerName, handler)
}
