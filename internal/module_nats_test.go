package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-broker/internal"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// startEmbeddedNATS launches a local NATS server for tests.
func startEmbeddedNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("startEmbeddedNATS: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready in time")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func newNATSModule(name string, cfg map[string]any) *internal.NATSModule {
	return internal.NewNATSModule(name, cfg)
}

func TestNATSModule_PublishSubscribe(t *testing.T) {
	srv := startEmbeddedNATS(t)

	mod := newNATSModule("broker", map[string]any{
		"url":    srv.ClientURL(),
		"stream": "GAME_EVENTS",
	})
	if err := mod.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mod.Stop(context.Background()) //nolint:errcheck

	received := make(chan []byte, 1)
	if err := mod.Subscribe("GAME_EVENTS.state.changed", func(data []byte) {
		received <- data
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := mod.Publish("GAME_EVENTS.state.changed", []byte(`{"gameId":"g1"}`)); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if string(msg) != `{"gameId":"g1"}` {
			t.Errorf("unexpected message: %s", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestNATSModule_JetStreamPersistence(t *testing.T) {
	srv := startEmbeddedNATS(t)

	mod := newNATSModule("broker", map[string]any{
		"url":       srv.ClientURL(),
		"stream":    "GAME_EVENTS",
		"jetstream": true,
	})
	if err := mod.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mod.Stop(context.Background()) //nolint:errcheck

	// Publish before subscriber connects
	if err := mod.Publish("GAME_EVENTS.node.drain", []byte(`{"nodeId":"node-a"}`)); err != nil {
		t.Fatalf("Publish before subscriber: %v", err)
	}

	// New subscriber connects after publish — JetStream should deliver from beginning
	received := make(chan []byte, 1)
	if err := mod.SubscribeDurable("GAME_EVENTS.node.drain", "drain-consumer", func(data []byte) {
		select {
		case received <- data:
		default:
		}
	}); err != nil {
		t.Fatalf("SubscribeDurable: %v", err)
	}

	select {
	case msg := <-received:
		if string(msg) != `{"nodeId":"node-a"}` {
			t.Fatalf("expected drain message, got %s", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("JetStream message not delivered to late subscriber")
	}
}

func TestNATSModule_MultipleSubscribers(t *testing.T) {
	srv := startEmbeddedNATS(t)

	mod := newNATSModule("broker", map[string]any{
		"url":    srv.ClientURL(),
		"stream": "GAME_EVENTS",
	})
	mod.Start(context.Background()) //nolint:errcheck
	defer mod.Stop(context.Background()) //nolint:errcheck

	ch1 := make(chan []byte, 1)
	ch2 := make(chan []byte, 1)
	mod.Subscribe("GAME_EVENTS.ping", func(d []byte) { ch1 <- d }) //nolint:errcheck
	mod.Subscribe("GAME_EVENTS.ping", func(d []byte) { ch2 <- d }) //nolint:errcheck

	mod.Publish("GAME_EVENTS.ping", []byte("hello")) //nolint:errcheck

	timeout := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-ch1:
		case <-ch2:
		case <-timeout:
			t.Fatal("timeout: not all subscribers received message")
		}
	}
}

func TestNATSModule_Stop(t *testing.T) {
	srv := startEmbeddedNATS(t)

	mod := newNATSModule("broker", map[string]any{
		"url":    srv.ClientURL(),
		"stream": "GAME_EVENTS",
	})
	mod.Start(context.Background()) //nolint:errcheck

	if err := mod.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Publish after stop should fail gracefully
	err := mod.Publish("GAME_EVENTS.test", []byte("data"))
	if err == nil {
		t.Log("note: publish after stop succeeded (connection may buffer)")
	}
}
