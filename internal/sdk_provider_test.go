package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TestBrokerProvider_Manifest verifies the manifest fields.
func TestBrokerProvider_Manifest(t *testing.T) {
	p := &internal.BrokerProvider{}
	m := p.Manifest()
	if m.Name == "" {
		t.Error("Manifest.Name must not be empty")
	}
	if m.Version == "" {
		t.Error("Manifest.Version must not be empty")
	}
	if m.Author == "" {
		t.Error("Manifest.Author must not be empty")
	}
	if m.Description == "" {
		t.Error("Manifest.Description must not be empty")
	}
}

// TestBrokerProvider_ModuleTypes verifies that exactly the expected module types are advertised.
func TestBrokerProvider_ModuleTypes(t *testing.T) {
	p := &internal.BrokerProvider{}
	types := p.ModuleTypes()
	if len(types) != 1 || types[0] != "broker.nats" {
		t.Errorf("ModuleTypes = %v, want [broker.nats]", types)
	}
}

// TestBrokerProvider_StepTypes verifies that exactly the expected step types are advertised.
func TestBrokerProvider_StepTypes(t *testing.T) {
	p := &internal.BrokerProvider{}
	types := p.StepTypes()
	want := map[string]bool{
		"step.broker_publish":   true,
		"step.broker_subscribe": true,
	}
	for _, got := range types {
		if !want[got] {
			t.Errorf("unexpected step type %q", got)
		}
		delete(want, got)
	}
	for missing := range want {
		t.Errorf("expected step type %q not advertised", missing)
	}
}

// TestBrokerProvider_CreateModule_ValidType verifies that CreateModule returns a
// non-nil ModuleInstance for the supported type.
func TestBrokerProvider_CreateModule_ValidType(t *testing.T) {
	p := &internal.BrokerProvider{}
	inst, err := p.CreateModule("broker.nats", "test-broker", map[string]any{
		"url":    "nats://localhost:4222",
		"stream": "TEST",
	})
	if err != nil {
		t.Fatalf("CreateModule returned error: %v", err)
	}
	if inst == nil {
		t.Fatal("CreateModule returned nil instance")
	}
}

// TestBrokerProvider_CreateModule_UnknownType verifies that an error is returned
// for an unknown module type.
func TestBrokerProvider_CreateModule_UnknownType(t *testing.T) {
	p := &internal.BrokerProvider{}
	_, err := p.CreateModule("broker.unknown", "x", nil)
	if err == nil {
		t.Error("expected error for unknown module type")
	}
}

// TestBrokerProvider_CreateStep_ValidTypes verifies that both step types can be created.
func TestBrokerProvider_CreateStep_ValidTypes(t *testing.T) {
	p := &internal.BrokerProvider{}
	for _, typeName := range []string{"step.broker_publish", "step.broker_subscribe"} {
		inst, err := p.CreateStep(typeName, "s", nil)
		if err != nil {
			t.Errorf("CreateStep(%q) returned error: %v", typeName, err)
		}
		if inst == nil {
			t.Errorf("CreateStep(%q) returned nil instance", typeName)
		}
	}
}

// TestBrokerProvider_CreateStep_UnknownType verifies that an error is returned
// for an unknown step type.
func TestBrokerProvider_CreateStep_UnknownType(t *testing.T) {
	p := &internal.BrokerProvider{}
	_, err := p.CreateStep("step.unknown", "x", nil)
	if err == nil {
		t.Error("expected error for unknown step type")
	}
}

// TestBrokerProvider_ModuleSchemas verifies that schema metadata is provided for
// the broker.nats module type.
func TestBrokerProvider_ModuleSchemas(t *testing.T) {
	p := &internal.BrokerProvider{}
	schemas := p.ModuleSchemas()
	if len(schemas) == 0 {
		t.Fatal("ModuleSchemas returned no schemas")
	}

	found := false
	for _, s := range schemas {
		if s.Type == "broker.nats" {
			found = true
			if s.Label == "" {
				t.Error("broker.nats schema: Label must not be empty")
			}
			if s.Category == "" {
				t.Error("broker.nats schema: Category must not be empty")
			}
			if s.Description == "" {
				t.Error("broker.nats schema: Description must not be empty")
			}
			if len(s.ConfigFields) == 0 {
				t.Error("broker.nats schema: ConfigFields must not be empty")
			}
		}
	}
	if !found {
		t.Error("ModuleSchemas: schema for broker.nats not found")
	}
}

// TestBrokerProvider_ModuleSchemas_NoJetstreamField verifies that the schema
// does not advertise the defunct jetstream flag (the module always uses JetStream).
func TestBrokerProvider_ModuleSchemas_NoJetstreamField(t *testing.T) {
	p := &internal.BrokerProvider{}
	for _, s := range p.ModuleSchemas() {
		if s.Type != "broker.nats" {
			continue
		}
		for _, f := range s.ConfigFields {
			if f.Name == "jetstream" {
				t.Error("broker.nats schema must not advertise the defunct jetstream field")
			}
		}
	}
}

// executeStep is a test helper that calls inst.Execute with nil for all
// positional arguments except config, keeping call sites concise.
func executeStep(t *testing.T, inst sdk.StepInstance, config map[string]any) error {
	t.Helper()
	_, err := inst.Execute(context.Background(), nil, nil, nil, nil, config)
	return err
}

// TestBrokerProvider_PublishStep_MissingTopic verifies that Execute returns an
// error when the topic is absent, without attempting a network connection.
func TestBrokerProvider_PublishStep_MissingTopic(t *testing.T) {
	p := &internal.BrokerProvider{}
	inst, err := p.CreateStep("step.broker_publish", "pub", map[string]any{
		"url": "nats://localhost:4222",
	})
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	if err := executeStep(t, inst, map[string]any{}); err == nil {
		t.Error("expected error when topic is missing")
	}
}

// TestBrokerProvider_SubscribeStep_MissingTopic verifies that Execute returns an
// error when the topic is absent, without attempting a network connection.
func TestBrokerProvider_SubscribeStep_MissingTopic(t *testing.T) {
	p := &internal.BrokerProvider{}
	inst, err := p.CreateStep("step.broker_subscribe", "sub", map[string]any{
		"url": "nats://localhost:4222",
	})
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	if err := executeStep(t, inst, map[string]any{"consumer_name": "c1"}); err == nil {
		t.Error("expected error when topic is missing")
	}
}

// TestBrokerProvider_SubscribeStep_MissingConsumerName verifies that Execute
// returns an error when consumer_name is absent, without attempting a network connection.
func TestBrokerProvider_SubscribeStep_MissingConsumerName(t *testing.T) {
	p := &internal.BrokerProvider{}
	inst, err := p.CreateStep("step.broker_subscribe", "sub", map[string]any{
		"url": "nats://localhost:4222",
	})
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	if err := executeStep(t, inst, map[string]any{"topic": "events.>"}); err == nil {
		t.Error("expected error when consumer_name is missing")
	}
}

// TestBrokerProvider_ImplementsSDKInterfaces asserts at compile time that
// BrokerProvider satisfies all expected SDK provider interfaces.
func TestBrokerProvider_ImplementsSDKInterfaces(t *testing.T) {
	var _ sdk.PluginProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.ModuleProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.StepProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.SchemaProvider = (*internal.BrokerProvider)(nil)
}

// TestBrokerProvider_PublishSubscribe_Integration publishes via the SDK publish
// step and then retrieves the message via the SDK subscribe step against an
// embedded NATS server, exercising the full SDK provider wiring end-to-end.
func TestBrokerProvider_PublishSubscribe_Integration(t *testing.T) {
	srv := startEmbeddedNATS(t)

	p := &internal.BrokerProvider{}
	natsCfg := map[string]any{
		"url":    srv.ClientURL(),
		"stream": "INT_TEST",
	}

	// Publish a JSON object message.
	pubInst, err := p.CreateStep("step.broker_publish", "pub", natsCfg)
	if err != nil {
		t.Fatalf("CreateStep publish: %v", err)
	}
	pubResult, err := pubInst.Execute(
		context.Background(), nil, nil,
		map[string]any{"payload": map[string]any{"hello": "world"}},
		nil,
		map[string]any{"topic": "INT_TEST.events"},
	)
	if err != nil {
		t.Fatalf("publish Execute: %v", err)
	}
	if pubResult == nil || pubResult.Output["published"] != true {
		t.Fatalf("publish step did not report success: %v", pubResult)
	}

	// Subscribe with a 5-second deadline so the test doesn't hang on failure.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subInst, err := p.CreateStep("step.broker_subscribe", "sub", natsCfg)
	if err != nil {
		t.Fatalf("CreateStep subscribe: %v", err)
	}
	subResult, err := subInst.Execute(
		ctx, nil, nil, nil, nil,
		map[string]any{
			"topic":         "INT_TEST.events",
			"consumer_name": "int-test-consumer",
		},
	)
	if err != nil {
		t.Fatalf("subscribe Execute: %v", err)
	}
	if subResult == nil || subResult.Output == nil {
		t.Fatal("subscribe Execute returned nil result")
	}
	if subResult.Output["topic"] != "INT_TEST.events" {
		t.Errorf("topic = %v, want INT_TEST.events", subResult.Output["topic"])
	}
	if subResult.Output["consumer"] != "int-test-consumer" {
		t.Errorf("consumer = %v, want int-test-consumer", subResult.Output["consumer"])
	}
	// Payload should be the published JSON object.
	payload, ok := subResult.Output["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map[string]any", subResult.Output["payload"])
	}
	if payload["hello"] != "world" {
		t.Errorf("payload[hello] = %v, want world", payload["hello"])
	}
}

// TestBrokerProvider_SubscribeStep_NonObjectJSON verifies that a non-object JSON
// payload (array) is preserved as-is in the output rather than wrapped.
func TestBrokerProvider_SubscribeStep_NonObjectJSON(t *testing.T) {
	srv := startEmbeddedNATS(t)

	p := &internal.BrokerProvider{}
	natsCfg := map[string]any{
		"url":    srv.ClientURL(),
		"stream": "ARRAY_TEST",
	}

	// Publish a JSON array (non-object JSON) as a native Go slice; the publish
	// step JSON-encodes it, and the subscribe step must decode it as []any, not
	// wrap it as map[string]any{"data": "..."}.
	pubInst, err := p.CreateStep("step.broker_publish", "pub", natsCfg)
	if err != nil {
		t.Fatalf("CreateStep publish: %v", err)
	}
	if _, err := pubInst.Execute(
		context.Background(), nil, nil,
		map[string]any{"payload": []any{1, 2, 3}},
		nil,
		map[string]any{"topic": "ARRAY_TEST.items"},
	); err != nil {
		t.Fatalf("publish Execute: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subInst, err := p.CreateStep("step.broker_subscribe", "sub", natsCfg)
	if err != nil {
		t.Fatalf("CreateStep subscribe: %v", err)
	}
	subResult, err := subInst.Execute(
		ctx, nil, nil, nil, nil,
		map[string]any{
			"topic":         "ARRAY_TEST.items",
			"consumer_name": "array-consumer",
		},
	)
	if err != nil {
		t.Fatalf("subscribe Execute: %v", err)
	}
	// Payload must be the decoded array, not map[string]any{"data": "[1,2,3]"}.
	payload, ok := subResult.Output["payload"].([]any)
	if !ok {
		t.Fatalf("payload type = %T, want []any (JSON array)", subResult.Output["payload"])
	}
	if len(payload) != 3 {
		t.Errorf("payload length = %d, want 3", len(payload))
	}
}
