// Command workflow-plugin-broker is a standalone gRPC plugin binary that
// exposes a NATS JetStream broker module to the workflow engine.
//
// Usage:
//
//	workflow-plugin-broker [--config path/to/config.yaml]
//
// The broker is typically embedded directly into the game server binary
// rather than run as a separate process, but this binary provides the
// standalone gRPC plugin path for use with workflow-plugin-loader.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
)

func main() {
	url := flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	stream := flag.String("stream", "GAME_EVENTS", "JetStream stream name")
	flag.Parse()

	plugin := internal.NewPlugin("broker.nats", map[string]any{
		"url":       *url,
		"stream":    *stream,
		"jetstream": true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := plugin.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "broker start: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("workflow-plugin-broker started (stream=%s url=%s)\n", *stream, *url)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig

	fmt.Println("shutting down broker...")
	if err := plugin.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "broker stop: %v\n", err)
	}
}
