// Package internal provides the NATS JetStream broker module for
// cross-instance game event delivery.
package internal

import "context"

// Publisher publishes a message to the given NATS subject.
type Publisher interface {
	Publish(subject string, data []byte) error
}

// Subscriber subscribes to a NATS subject.
type Subscriber interface {
	Subscribe(subject string, handler func([]byte)) error
	SubscribeDurable(subject, consumerName string, handler func([]byte)) error
}

// Broker combines Publisher and Subscriber.
type Broker interface {
	Publisher
	Subscriber
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}
