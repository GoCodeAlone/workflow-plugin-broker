// Command workflow-plugin-broker is a standalone gRPC plugin binary that
// exposes a NATS JetStream broker module to the workflow engine.
//
// It implements the Workflow external-plugin gRPC contract via sdk.Serve so
// that the workflow engine can load it with workflow-plugin-loader.
//
// Usage:
//
//	workflow-plugin-broker
//
// The binary is launched by the workflow engine via the plugin loader and
// communicates over stdin/stdout using the go-plugin gRPC protocol.
package main

import (
	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func main() {
	sdk.Serve(&internal.BrokerProvider{})
}
