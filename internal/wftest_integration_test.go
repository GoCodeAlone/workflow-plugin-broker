package internal_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow/wftest"
)

func TestBroker_PublishPipeline(t *testing.T) {
	pubRec := wftest.RecordStep("step.broker_publish")
	pubRec.WithOutput(map[string]any{"published": true, "topic": "game.events", "bytes": 42})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  publish-event:
    trigger:
      type: manual
    steps:
      - name: publish
        type: step.broker_publish
        config:
          topic: game.events
`), pubRec)

	result := h.ExecutePipeline("publish-event", map[string]any{
		"event":     "player.joined",
		"player_id": "player123",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if pubRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.broker_publish, got %d", pubRec.CallCount())
	}

	calls := pubRec.Calls()
	if calls[0].Config["topic"] != "game.events" {
		t.Errorf("expected config topic=game.events, got %v", calls[0].Config["topic"])
	}
}

func TestBroker_PublishMultipleEvents(t *testing.T) {
	pubRec := wftest.RecordStep("step.broker_publish")
	pubRec.WithOutput(map[string]any{"published": true})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  publish-state-change:
    trigger:
      type: manual
    steps:
      - name: publish-start
        type: step.broker_publish
        config:
          topic: game.state.changed
      - name: publish-notify
        type: step.broker_publish
        config:
          topic: game.notifications
`), pubRec)

	result := h.ExecutePipeline("publish-state-change", map[string]any{
		"game_id": "game-42",
		"state":   "in_progress",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if pubRec.CallCount() != 2 {
		t.Errorf("expected 2 calls to step.broker_publish, got %d", pubRec.CallCount())
	}

	calls := pubRec.Calls()
	if calls[0].Config["topic"] != "game.state.changed" {
		t.Errorf("first call: expected topic=game.state.changed, got %v", calls[0].Config["topic"])
	}
	if calls[1].Config["topic"] != "game.notifications" {
		t.Errorf("second call: expected topic=game.notifications, got %v", calls[1].Config["topic"])
	}
}

func TestBroker_SubscribePipeline(t *testing.T) {
	subRec := wftest.RecordStep("step.broker_subscribe")
	subRec.WithOutput(map[string]any{"subscribed": true, "topic": "game.events", "consumer": "game-consumer"})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  subscribe-events:
    trigger:
      type: manual
    steps:
      - name: subscribe
        type: step.broker_subscribe
        config:
          topic: game.events
          consumer_name: game-consumer
`), subRec)

	result := h.ExecutePipeline("subscribe-events", map[string]any{
		"game_id": "game-42",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if subRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.broker_subscribe, got %d", subRec.CallCount())
	}

	calls := subRec.Calls()
	if calls[0].Config["topic"] != "game.events" {
		t.Errorf("expected config topic=game.events, got %v", calls[0].Config["topic"])
	}
	if calls[0].Config["consumer_name"] != "game-consumer" {
		t.Errorf("expected config consumer_name=game-consumer, got %v", calls[0].Config["consumer_name"])
	}
}
