package internal_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
)

func TestContractRegistry_HasAllSurfaces(t *testing.T) {
	p := &internal.BrokerProvider{}
	reg := p.ContractRegistry()

	if reg == nil {
		t.Fatal("ContractRegistry() returned nil")
	}

	// Expect 3 contracts: 1 module + 2 steps
	if len(reg.Contracts) != 3 {
		t.Fatalf("expected 3 contracts, got %d", len(reg.Contracts))
	}

	type want struct {
		kind       pb.ContractKind
		moduleType string
		stepType   string
	}
	wantContracts := []want{
		{kind: pb.ContractKind_CONTRACT_KIND_MODULE, moduleType: "broker.nats"},
		{kind: pb.ContractKind_CONTRACT_KIND_STEP, stepType: "step.broker_publish"},
		{kind: pb.ContractKind_CONTRACT_KIND_STEP, stepType: "step.broker_subscribe"},
	}

	for i, w := range wantContracts {
		c := reg.Contracts[i]
		if c.Kind != w.kind {
			t.Errorf("contract[%d]: want kind %v, got %v", i, w.kind, c.Kind)
		}
		if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
			t.Errorf("contract[%d]: want STRICT_PROTO mode, got %v", i, c.Mode)
		}
		if w.moduleType != "" && c.ModuleType != w.moduleType {
			t.Errorf("contract[%d]: want module_type %q, got %q", i, w.moduleType, c.ModuleType)
		}
		if w.stepType != "" && c.StepType != w.stepType {
			t.Errorf("contract[%d]: want step_type %q, got %q", i, w.stepType, c.StepType)
		}
	}
}

func TestContractRegistry_FileDescriptorSetIncludesBrokerAndStruct(t *testing.T) {
	p := &internal.BrokerProvider{}
	reg := p.ContractRegistry()

	if reg.FileDescriptorSet == nil {
		t.Fatal("FileDescriptorSet is nil")
	}

	var foundBroker, foundStruct bool
	for _, fd := range reg.FileDescriptorSet.File {
		name := fd.GetName()
		if name == "broker.proto" {
			foundBroker = true
		}
		if name == "google/protobuf/struct.proto" {
			foundStruct = true
		}
	}

	if !foundBroker {
		t.Error("FileDescriptorSet missing broker.proto")
	}
	if !foundStruct {
		t.Error("FileDescriptorSet missing google/protobuf/struct.proto")
	}
}

func TestContractRegistry_AllContractsHaveConfigMessage(t *testing.T) {
	p := &internal.BrokerProvider{}
	reg := p.ContractRegistry()

	for i, c := range reg.Contracts {
		if c.ConfigMessage == "" {
			t.Errorf("contract[%d] (%v): ConfigMessage is empty", i, c.Kind)
		}
	}
}

func TestContractRegistry_StepContractsHaveInputAndOutput(t *testing.T) {
	p := &internal.BrokerProvider{}
	reg := p.ContractRegistry()

	for i, c := range reg.Contracts {
		if c.Kind != pb.ContractKind_CONTRACT_KIND_STEP {
			continue
		}
		if c.InputMessage == "" {
			t.Errorf("step contract[%d] (%s): InputMessage is empty", i, c.StepType)
		}
		if c.OutputMessage == "" {
			t.Errorf("step contract[%d] (%s): OutputMessage is empty", i, c.StepType)
		}
	}
}
