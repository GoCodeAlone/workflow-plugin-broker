package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// BrokerProvider implements the workflow external plugin SDK interfaces so that
// the broker can be served as a proper gRPC plugin via sdk.Serve.
//
// Implemented interfaces:
//   - sdk.PluginProvider  (Manifest)
//   - sdk.ModuleProvider  (ModuleTypes, CreateModule)
//   - sdk.StepProvider    (StepTypes, CreateStep)
//   - sdk.SchemaProvider  (ModuleSchemas)
type BrokerProvider struct{}

// Manifest returns the plugin metadata.
func (p *BrokerProvider) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-broker",
		Version:     Version,
		Author:      "GoCodeAlone",
		Description: "NATS JetStream broker module for cross-instance message delivery in workflow pipelines",
	}
}

// ModuleTypes returns the module type names provided by this plugin.
func (p *BrokerProvider) ModuleTypes() []string {
	return []string{"broker.nats"}
}

// CreateModule creates a NATS broker module instance of the given type.
func (p *BrokerProvider) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	if typeName != "broker.nats" {
		return nil, fmt.Errorf("broker plugin: unknown module type %q", typeName)
	}
	return &brokerModuleInstance{
		module: NewNATSModule(name, config),
	}, nil
}

// StepTypes returns the step type names provided by this plugin.
func (p *BrokerProvider) StepTypes() []string {
	return []string{"step.broker_publish", "step.broker_subscribe"}
}

// CreateStep creates a step instance of the given type.
// The step's broker connection is created lazily on first Execute call using
// the config fields (url, stream).
func (p *BrokerProvider) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.broker_publish":
		return &publishStepInstance{name: name, config: config}, nil
	case "step.broker_subscribe":
		return &subscribeStepInstance{name: name, config: config}, nil
	default:
		return nil, fmt.Errorf("broker plugin: unknown step type %q", typeName)
	}
}

// ModuleSchemas returns typed schema descriptions for all module types.
func (p *BrokerProvider) ModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "broker.nats",
			Label:       "NATS JetStream Broker",
			Category:    "messaging",
			Description: "Connects to a NATS server and uses JetStream for durable message delivery across instances.",
			ConfigFields: []sdk.ConfigField{
				{
					Name:         "url",
					Type:         "string",
					Description:  "NATS server URL",
					DefaultValue: "nats://localhost:4222",
					Required:     false,
				},
				{
					Name:         "stream",
					Type:         "string",
					Description:  "JetStream stream name",
					DefaultValue: "GAME_EVENTS",
					Required:     false,
				},
			},
		},
	}
}

// --- brokerModuleInstance ---

// brokerModuleInstance wraps NATSModule and implements sdk.ModuleInstance.
type brokerModuleInstance struct {
	module *NATSModule
}

// Init is a no-op; all initialisation happens in Start.
func (m *brokerModuleInstance) Init() error { return nil }

// Start connects to NATS and ensures the JetStream stream exists.
func (m *brokerModuleInstance) Start(ctx context.Context) error {
	return m.module.Start(ctx)
}

// Stop drains subscriptions and closes the NATS connection.
func (m *brokerModuleInstance) Stop(ctx context.Context) error {
	return m.module.Stop(ctx)
}

// --- publishStepInstance ---

// publishStepInstance implements sdk.StepInstance for step.broker_publish.
// The static config from CreateStep is used as the base; runtime config and
// current values are merged on top at Execute time.
type publishStepInstance struct {
	name   string
	config map[string]any
}

// Execute publishes a message to the broker topic.
//
// Config keys (merged from static CreateStep config, runtime config, and current):
//
//	topic   (string, required) — NATS subject to publish to.
//
// Current/input keys:
//
//	payload (string|map|any)  — message body; maps are JSON-encoded.
func (s *publishStepInstance) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	// Merge static config, runtime config, and current into params.
	params := make(map[string]any, len(s.config)+len(config)+len(current))
	for k, v := range s.config {
		params[k] = v
	}
	for k, v := range config {
		params[k] = v
	}
	for k, v := range current {
		params[k] = v
	}

	// Validate required params before establishing any network connection.
	topic, _ := params["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("step.broker_publish: topic is required")
	}

	mod := NewNATSModule(s.name, params)
	if err := mod.Start(ctx); err != nil {
		return nil, fmt.Errorf("step.broker_publish: connect broker: %w", err)
	}
	defer mod.Stop(ctx) //nolint:errcheck

	step := NewPublishStep(mod)
	out, err := step.Execute(ctx, params)
	if err != nil {
		return nil, err
	}
	return &sdk.StepResult{Output: out}, nil
}

// --- subscribeStepInstance ---

// subscribeStepInstance implements sdk.StepInstance for step.broker_subscribe.
// The static config from CreateStep is used as the base; runtime config and
// current values are merged on top at Execute time.
// Execute blocks until the first message arrives or the context is cancelled.
type subscribeStepInstance struct {
	name   string
	config map[string]any
}

// Execute sets up a durable JetStream subscription and blocks until one message
// is received or the context is cancelled.
//
// Config keys (merged from static CreateStep config, runtime config, and current):
//
//	topic         (string, required) — NATS subject filter.
//	consumer_name (string, required) — durable consumer name for JetStream replay.
func (s *subscribeStepInstance) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	// Merge static config, runtime config, and current into params.
	params := make(map[string]any, len(s.config)+len(config)+len(current))
	for k, v := range s.config {
		params[k] = v
	}
	for k, v := range config {
		params[k] = v
	}
	for k, v := range current {
		params[k] = v
	}

	// Validate required params before establishing any network connection.
	topic, _ := params["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("step.broker_subscribe: topic is required")
	}
	consumerName, _ := params["consumer_name"].(string)
	if consumerName == "" {
		return nil, fmt.Errorf("step.broker_subscribe: consumer_name is required")
	}

	mod := NewNATSModule(s.name, params)
	if err := mod.Start(ctx); err != nil {
		return nil, fmt.Errorf("step.broker_subscribe: connect broker: %w", err)
	}

	received := make(chan []byte, 1)
	if err := mod.SubscribeDurable(topic, consumerName, func(data []byte) {
		select {
		case received <- data:
		default:
		}
	}); err != nil {
		_ = mod.Stop(ctx)
		return nil, fmt.Errorf("step.broker_subscribe: %w", err)
	}

	// Block until the first message arrives or the context is cancelled.
	var payload map[string]any
	select {
	case data := <-received:
		_ = mod.Stop(ctx)
		if err := json.Unmarshal(data, &payload); err != nil {
			// Return raw bytes as a string if not valid JSON.
			payload = map[string]any{"data": string(data)}
		}
	case <-ctx.Done():
		_ = mod.Stop(ctx)
		return nil, ctx.Err()
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"topic":    topic,
			"consumer": consumerName,
			"payload":  payload,
		},
	}, nil
}
