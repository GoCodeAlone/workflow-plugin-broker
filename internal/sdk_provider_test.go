package internal_test

import (
	"testing"

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

// TestBrokerProvider_ImplementsSDKInterfaces asserts at compile time that
// BrokerProvider satisfies all expected SDK provider interfaces.
func TestBrokerProvider_ImplementsSDKInterfaces(t *testing.T) {
	var _ sdk.PluginProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.ModuleProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.StepProvider = (*internal.BrokerProvider)(nil)
	var _ sdk.SchemaProvider = (*internal.BrokerProvider)(nil)
}
