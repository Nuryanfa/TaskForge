package natsbus

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/outbox"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// integrationConn connects to TASKFORGE_TEST_NATS_URL and returns a unique
// stream name that is deleted when the test ends.
func integrationConn(t *testing.T) (*nats.Conn, string) {
	t.Helper()
	natsURL := os.Getenv("TASKFORGE_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("TASKFORGE_TEST_NATS_URL is not set")
	}
	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal("connect to integration NATS")
	}
	stream := "TF_TEST_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		if js, err := jetstream.New(nc); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = js.DeleteStream(ctx, stream)
			cancel()
		}
		nc.Close()
	})
	return nc, stream
}

func testEvent(id int64, eventType string) outbox.Event {
	now := time.Now()
	return outbox.Event{ID: id, JobID: uuid.NewString(), Type: eventType, CreatedAt: now,
		Payload: json.RawMessage(`{"queue":"default","available_at":"` + now.UTC().Format(time.RFC3339Nano) + `"}`)}
}

func TestPublisherEnsuresStreamAndDeduplicatesIntegration(t *testing.T) {
	nc, stream := integrationConn(t)
	// Isolate subjects per test stream so parallel packages do not overlap.
	prefix := "tftest." + strings.ToLower(stream) + "."
	publisher, err := NewPublisher(nc, StreamOptions{Name: stream, Subjects: []string{prefix + ">"},
		MaxAge: time.Hour, Duplicates: time.Minute}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	publisher.subjectPrefix = prefix
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := publisher.EnsureStream(ctx); err != nil {
			t.Fatalf("ensure stream attempt %d: %v", i+1, err)
		}
	}
	first := testEvent(7, "job.queued")
	for i := 0; i < 3; i++ {
		if err := publisher.Publish(ctx, first); err != nil {
			t.Fatal(err)
		}
	}
	if err := publisher.Publish(ctx, testEvent(8, "job.started")); err != nil {
		t.Fatal(err)
	}
	js, _ := jetstream.New(nc)
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream messages=%d want 2 (duplicates dropped)", info.State.Msgs)
	}
	msg, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Subject != prefix+"job.queued" || msg.Header.Get(jetstream.MsgIDHeader) != "taskforge-outbox-7" ||
		msg.Header.Get(outbox.HeaderEventType) != "job.queued" || msg.Header.Get(outbox.HeaderQueue) != "default" {
		t.Fatalf("message subject=%q headers=%v", msg.Subject, msg.Header)
	}
}

func TestPublishFailsWithoutStreamIntegration(t *testing.T) {
	nc, stream := integrationConn(t)
	publisher, err := NewPublisher(nc, StreamOptions{Name: stream, Subjects: []string{"tfnostream." + strings.ToLower(stream) + ".>"},
		MaxAge: time.Hour, Duplicates: time.Minute}, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	publisher.subjectPrefix = "tfnostream." + strings.ToLower(stream) + "."
	if err := publisher.Publish(context.Background(), testEvent(1, "job.queued")); err == nil {
		t.Fatal("publishing to a subject without a stream must fail (no ack)")
	}
}

func TestSubscribeWakeupsReceivesQueuedEventsIntegration(t *testing.T) {
	nc, _ := integrationConn(t)
	wake := make(chan struct{}, 1)
	sub, err := SubscribeWakeups(nc, wake)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	body, err := testEvent(1, "job.queued").Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(outbox.QueuedSubject, body); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up received")
	}
}
