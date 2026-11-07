package natsbus

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestWakeHandlerSignalsOnlyForDueJobsWithoutBlocking(t *testing.T) {
	wake := make(chan struct{}, 1)
	handle := wakeHandler(wake)

	handle(&nats.Msg{Data: []byte(`{"occurred_at":"2026-11-07T10:00:00Z","job":{"available_at":"2026-11-07T10:00:05Z"}}`)})
	if len(wake) != 0 {
		t.Fatal("delayed job must not wake workers")
	}
	handle(&nats.Msg{Data: []byte(`not json`)})
	if len(wake) != 0 {
		t.Fatal("malformed event must not wake workers")
	}
	handle(&nats.Msg{Data: []byte(`{"occurred_at":"2026-11-07T10:00:00Z","job":{"available_at":"2026-11-07T10:00:00Z"}}`)})
	if len(wake) != 1 {
		t.Fatal("due job must wake workers")
	}
	done := make(chan struct{})
	go func() {
		handle(&nats.Msg{Data: []byte(`{"occurred_at":"2026-11-07T10:00:00Z","job":{"available_at":"2026-11-07T09:59:00Z"}}`)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wake handler blocked on a full channel")
	}
}
