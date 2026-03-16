// Package workflowpluginbroker provides the NATS broker plugin for
// cross-instance game event delivery in the workflow engine.
package workflowpluginbroker

import "github.com/GoCodeAlone/workflow-plugin-broker/internal"

// Broker combines Publisher and Subscriber interfaces.
type Broker = internal.Broker

// NewBrokerPlugin creates a broker plugin from config.
// Config keys: url (string), stream (string), jetstream (bool).
func NewBrokerPlugin(name string, cfg map[string]any) Broker {
	return internal.NewPlugin(name, cfg)
}
