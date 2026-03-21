// Package workflowpluginbroker provides the NATS broker workflow plugin.
package workflowpluginbroker

import (
	"context"

	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
)

// Broker is the public interface for the NATS broker.
type Broker interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Publish(subject string, data []byte) error
	Subscribe(subject string, handler func([]byte)) error
	SubscribeWithSubject(subject string, handler func(subject string, data []byte)) error
	SubscribeDurable(subject, consumerName string, handler func([]byte)) error
}

// NewBrokerPlugin creates a new NATS broker plugin instance from the given config.
// Config keys: url (string), stream (string), jetstream (bool).
func NewBrokerPlugin(name string, cfg map[string]any) Broker {
	return internal.NewPlugin(name, cfg)
}
