package internal

import (
	"context"
	"fmt"
)

// Plugin wires together the NATSModule with the publish/subscribe steps.
// A single Plugin instance is created per workflow-server process.
type Plugin struct {
	module *NATSModule
	pub    *PublishStep
	sub    *SubscribeStep
}

// NewPlugin creates a broker plugin from config.
// Config keys: url (string), stream (string), jetstream (bool).
func NewPlugin(name string, cfg map[string]any) *Plugin {
	mod := NewNATSModule(name, cfg)
	return &Plugin{
		module: mod,
		pub:    NewPublishStep(mod),
		sub:    NewSubscribeStep(mod),
	}
}

// Start initialises the NATS connection.
func (p *Plugin) Start(ctx context.Context) error {
	return p.module.Start(ctx)
}

// Stop drains subscriptions and closes the NATS connection.
func (p *Plugin) Stop(ctx context.Context) error {
	return p.module.Stop(ctx)
}

// Publish sends data to the given subject.
func (p *Plugin) Publish(subject string, data []byte) error {
	return p.module.Publish(subject, data)
}

// Subscribe creates a non-durable subscription.
func (p *Plugin) Subscribe(subject string, handler func([]byte)) error {
	return p.module.Subscribe(subject, handler)
}

// SubscribeDurable creates a durable JetStream subscription.
func (p *Plugin) SubscribeDurable(subject, consumerName string, handler func([]byte)) error {
	return p.module.SubscribeDurable(subject, consumerName, handler)
}

// PublishStep returns the step.broker_publish executor.
func (p *Plugin) PublishStep() *PublishStep { return p.pub }

// SubscribeStep returns the step.broker_subscribe executor.
func (p *Plugin) SubscribeStep() *SubscribeStep { return p.sub }

// Name returns the module name.
func (p *Plugin) Name() string { return p.module.name }

// Ensure Plugin satisfies Broker at compile time.
var _ Broker = (*Plugin)(nil)

// NATSModuleConfig holds typed configuration for the NATS broker module.
type NATSModuleConfig struct {
	// URL is the NATS server URL (default: nats://localhost:4222).
	URL string `yaml:"url" mapstructure:"url"`
	// Stream is the JetStream stream name (default: GAME_EVENTS).
	Stream string `yaml:"stream" mapstructure:"stream"`
	// JetStream enables JetStream persistence (default: false for plain pub/sub).
	JetStream bool `yaml:"jetstream" mapstructure:"jetstream"`
}

// Validate checks that required fields are present.
func (c NATSModuleConfig) Validate() error {
	if c.Stream == "" {
		return fmt.Errorf("broker.nats: stream name is required")
	}
	return nil
}
