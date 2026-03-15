package internal

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

// NATSModule implements Broker using NATS JetStream.
// Thread-safe; multiple goroutines may call Publish/Subscribe concurrently.
type NATSModule struct {
	mu        sync.RWMutex
	name      string
	url       string
	stream    string
	jetstream bool

	nc   *nats.Conn
	js   nats.JetStreamContext
	subs []*nats.Subscription
}

// NewNATSModule constructs a NATSModule from config map.
// Recognised keys: url (string), stream (string), jetstream (bool).
func NewNATSModule(name string, cfg map[string]any) *NATSModule {
	m := &NATSModule{
		name:   name,
		url:    nats.DefaultURL,
		stream: "GAME_EVENTS",
	}
	if v, ok := cfg["url"].(string); ok && v != "" {
		m.url = v
	}
	if v, ok := cfg["stream"].(string); ok && v != "" {
		m.stream = v
	}
	if v, ok := cfg["jetstream"].(bool); ok {
		m.jetstream = v
	}
	return m
}

// Start connects to NATS and (if jetstream=true) ensures the stream exists.
func (m *NATSModule) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	nc, err := nats.Connect(m.url,
		nats.Name(m.name),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return fmt.Errorf("nats_module.Start: connect %s: %w", m.url, err)
	}
	m.nc = nc

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return fmt.Errorf("nats_module.Start: jetstream context: %w", err)
	}
	m.js = js

	// Ensure stream exists with a wildcard subject matching this stream name.
	if err := m.ensureStream(); err != nil {
		nc.Close()
		return err
	}

	return nil
}

// ensureStream creates the JetStream stream if it does not already exist.
// Must be called with m.mu held.
func (m *NATSModule) ensureStream() error {
	_, err := m.js.StreamInfo(m.stream)
	if err == nil {
		return nil // already exists
	}

	_, err = m.js.AddStream(&nats.StreamConfig{
		Name:      m.stream,
		Subjects:  []string{m.stream + ".>"},
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
		Replicas:  1,
		MaxAge:    0, // no expiry by default
	})
	if err != nil {
		return fmt.Errorf("nats_module.ensureStream %s: %w", m.stream, err)
	}
	return nil
}

// Stop drains subscriptions and closes the NATS connection.
func (m *NATSModule) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, sub := range m.subs {
		_ = sub.Unsubscribe()
	}
	m.subs = nil

	if m.nc != nil {
		m.nc.Close()
		m.nc = nil
	}
	return nil
}

// Publish sends data to the given NATS subject via JetStream.
func (m *NATSModule) Publish(subject string, data []byte) error {
	m.mu.RLock()
	js := m.js
	m.mu.RUnlock()

	if js == nil {
		return fmt.Errorf("nats_module.Publish: not started")
	}
	if _, err := js.Publish(subject, data); err != nil {
		return fmt.Errorf("nats_module.Publish %s: %w", subject, err)
	}
	return nil
}

// Subscribe creates a non-durable core NATS subscription.
// The handler is called on each message in a goroutine managed by the NATS client.
func (m *NATSModule) Subscribe(subject string, handler func([]byte)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nc == nil {
		return fmt.Errorf("nats_module.Subscribe: not started")
	}

	sub, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("nats_module.Subscribe %s: %w", subject, err)
	}
	m.subs = append(m.subs, sub)
	return nil
}

// SubscribeDurable creates a durable JetStream push-subscribe consumer.
// Late subscribers receive messages published before they connected (from sequence 1).
func (m *NATSModule) SubscribeDurable(subject, consumerName string, handler func([]byte)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.js == nil {
		return fmt.Errorf("nats_module.SubscribeDurable: not started")
	}

	sub, err := m.js.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
		_ = msg.Ack()
	},
		nats.Durable(consumerName),
		nats.DeliverAll(),
		nats.AckExplicit(),
		nats.BindStream(m.stream),
	)
	if err != nil {
		return fmt.Errorf("nats_module.SubscribeDurable %s/%s: %w", subject, consumerName, err)
	}
	m.subs = append(m.subs, sub)
	return nil
}
