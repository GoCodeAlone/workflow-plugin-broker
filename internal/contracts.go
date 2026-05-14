package internal

import (
	brokerv1 "github.com/GoCodeAlone/workflow-plugin-broker/gen"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ContractRegistry returns the typed contract descriptors for all broker
// module and step types. The workflow engine calls this via the
// sdk.ContractProvider interface for strict validation.
func (p *BrokerProvider) ContractRegistry() *pb.ContractRegistry {
	return brokerContractRegistry
}

// brokerProtoPkg is the proto package prefix for all broker typed messages.
const brokerProtoPkg = "workflow.plugin.broker.v1."

// brokerContractRegistry declares STRICT_PROTO contracts for the broker.nats
// module and the step.broker_publish and step.broker_subscribe step types.
var brokerContractRegistry = &pb.ContractRegistry{
	FileDescriptorSet: &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
			protodesc.ToFileDescriptorProto(brokerv1.File_broker_proto),
		},
	},
	Contracts: []*pb.ContractDescriptor{
		// ── module: broker.nats ──────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "broker.nats",
			ConfigMessage: brokerProtoPkg + "NATSModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── step.broker_publish ───────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.broker_publish",
			ConfigMessage: brokerProtoPkg + "BrokerPublishConfig",
			InputMessage:  brokerProtoPkg + "BrokerPublishInput",
			OutputMessage: brokerProtoPkg + "BrokerPublishOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── step.broker_subscribe ─────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.broker_subscribe",
			ConfigMessage: brokerProtoPkg + "BrokerSubscribeConfig",
			InputMessage:  brokerProtoPkg + "BrokerSubscribeInput",
			OutputMessage: brokerProtoPkg + "BrokerSubscribeOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
	},
}

// Compile-time assertion: BrokerProvider implements sdk.ContractProvider.
var _ interface{ ContractRegistry() *pb.ContractRegistry } = (*BrokerProvider)(nil)
