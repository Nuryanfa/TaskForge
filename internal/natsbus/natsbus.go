// Package natsbus adapts the outbox event contract to NATS: JetStream
// publishing for the relay and core-NATS wake-up hints for workers.
package natsbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/outbox"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Connect opens a NATS connection that keeps retrying in the background, so
// a broker outage never blocks process startup.
func Connect(url, name string, timeout time.Duration) (*nats.Conn, error) {
	return nats.Connect(url, nats.Name(name), nats.Timeout(timeout),
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
}

type StreamOptions struct {
	Name       string
	Subjects   []string
	MaxAge     time.Duration
	Duplicates time.Duration
}

// Publisher publishes outbox events to a JetStream stream and waits for the
// stream acknowledgment of each message.
type Publisher struct {
	js            jetstream.JetStream
	stream        StreamOptions
	timeout       time.Duration
	subjectPrefix string
}

func NewPublisher(nc *nats.Conn, stream StreamOptions, publishTimeout time.Duration) (*Publisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	if len(stream.Subjects) == 0 {
		stream.Subjects = []string{outbox.StreamSubjects}
	}
	return &Publisher{js: js, stream: stream, timeout: publishTimeout, subjectPrefix: outbox.SubjectPrefix}, nil
}

// EnsureStream creates the stream or updates its limits; it is idempotent.
func (p *Publisher) EnsureStream(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        p.stream.Name,
		Description: "TaskForge job lifecycle events published from the PostgreSQL outbox",
		Subjects:    p.stream.Subjects,
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxAge:      p.stream.MaxAge,
		Duplicates:  p.stream.Duplicates,
	})
	if err != nil {
		return fmt.Errorf("ensure jetstream stream: %w", err)
	}
	return nil
}

// Publish sends one event with its stable Nats-Msg-Id. A duplicate
// acknowledgment (already stored within the window) counts as success.
func (p *Publisher) Publish(ctx context.Context, event outbox.Event) error {
	body, err := event.Encode()
	if err != nil {
		return err
	}
	msg := nats.NewMsg(p.subjectPrefix + event.Type)
	msg.Data = body
	msg.Header.Set(outbox.HeaderEventType, event.Type)
	if queue := event.Queue(); queue != "" {
		msg.Header.Set(outbox.HeaderQueue, queue)
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	ack, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(event.MsgID()))
	if err != nil {
		return fmt.Errorf("publish to jetstream: %w", err)
	}
	if ack == nil || ack.Stream != p.stream.Name {
		return errors.New("publish acknowledged by an unexpected stream")
	}
	return nil
}

// SubscribeWakeups signals wake for each job.queued event whose job is due.
// It is a plain core-NATS subscription: lost or duplicate hints are harmless
// because claims are always decided by PostgreSQL.
func SubscribeWakeups(nc *nats.Conn, wake chan<- struct{}) (*nats.Subscription, error) {
	return nc.Subscribe(outbox.QueuedSubject, wakeHandler(wake))
}

func wakeHandler(wake chan<- struct{}) nats.MsgHandler {
	return func(msg *nats.Msg) {
		if due, err := outbox.DueOnOccurrence(msg.Data); err != nil || !due {
			return
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
