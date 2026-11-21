package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/tracectx"
	"go.opentelemetry.io/otel/trace"
)

const (
	firstContext  = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	secondContext = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)

// completeWithContext completes a claimed job as if its attempt span carried
// traceparent (nil: no span context, as with tracing off).
func completeWithContext(t *testing.T, s *Store, claimed job.Claimed, traceparent *string) {
	t.Helper()
	ctx := context.Background()
	if traceparent != nil {
		spanContext, ok := tracectx.SpanContext(*traceparent)
		if !ok {
			t.Fatalf("invalid test traceparent %s", *traceparent)
		}
		ctx = trace.ContextWithRemoteSpanContext(ctx, spanContext)
	}
	if err := s.CompleteJob(ctx, claimed.Execution, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("complete %s: %v", claimed.Job.ID, err)
	}
}

func jobTraceContext(t *testing.T, s *Store, jobID string) (*string, []string) {
	t.Helper()
	var parent *string
	var links []string
	if err := s.pool.QueryRow(context.Background(), `SELECT trace_parent, trace_links FROM jobs WHERE id=$1`, jobID).Scan(&parent, &links); err != nil {
		t.Fatal(err)
	}
	return parent, links
}

// TestFanInDispatchStoresSiblingTraceLinksIntegration: the dependency whose
// completion dispatched the fan-in step is its parent; every other completed
// dependency becomes a link, never duplicated; compensations get no links.
func TestFanInDispatchStoresSiblingTraceLinksIntegration(t *testing.T) {
	s := workflowStore(t, 1)
	wf := submitWorkflow(t, s,
		withCompensation(step("a", "demo.echo"), "demo.echo"),
		withCompensation(step("b", "demo.echo"), "demo.echo"),
		step("c", "demo.fail", "a", "b"))
	first, second := claimNext(t, s, "w"), claimNext(t, s, "w")
	firstValue, secondValue := firstContext, secondContext
	completeWithContext(t, s, first, &firstValue)
	completeWithContext(t, s, second, &secondValue)

	fanIn := stepByName(t, getWorkflow(t, s, wf.ID), "c")
	parent, links := jobTraceContext(t, s, *fanIn.JobID)
	if parent == nil || *parent != secondContext {
		t.Fatalf("fan-in parent=%v, want the triggering dependency %s", parent, secondContext)
	}
	if len(links) != 1 || links[0] != firstContext {
		t.Fatalf("fan-in links=%v, want [%s]", links, firstContext)
	}

	driveNext(t, s, "w", failPermanent("DEMO_FAILURE"))
	for _, name := range []string{"a", "b"} {
		compensation := stepByName(t, getWorkflow(t, s, wf.ID), name).CompensationJobID
		if compensation == nil {
			t.Fatalf("step %s compensation not dispatched", name)
		}
		if _, links := jobTraceContext(t, s, *compensation); len(links) != 0 {
			t.Fatalf("compensation of %s has links %v, want none", name, links)
		}
	}
}

// TestFanInDispatchSkipsDependenciesWithoutContextIntegration: a dependency
// that ran without a span context is skipped; dispatch still succeeds.
func TestFanInDispatchSkipsDependenciesWithoutContextIntegration(t *testing.T) {
	s := workflowStore(t, 1)
	wf := submitWorkflow(t, s, step("a", "demo.echo"), step("b", "demo.echo"), step("c", "demo.echo", "a", "b"))
	first, second := claimNext(t, s, "w"), claimNext(t, s, "w")
	secondValue := secondContext
	completeWithContext(t, s, first, nil)
	completeWithContext(t, s, second, &secondValue)

	fanIn := stepByName(t, getWorkflow(t, s, wf.ID), "c")
	if fanIn.JobID == nil {
		t.Fatal("fan-in step was not dispatched")
	}
	if _, links := jobTraceContext(t, s, *fanIn.JobID); len(links) != 0 {
		t.Fatalf("links=%v, want none for a dependency without context", links)
	}
	if fanIn.Status != job.StepDispatched {
		t.Fatalf("fan-in status=%v", fanIn.Status)
	}
}

// TestFanInLinksCappedAt32Integration: a step with more completed
// dependencies than the 32-link bound still dispatches, keeping the first 32
// links (best effort) instead of failing the completing job forever.
func TestFanInLinksCappedAt32Integration(t *testing.T) {
	s := workflowStore(t, 1)
	const roots = 35
	steps := make([]job.WorkflowStepRequest, 0, roots+1)
	names := make([]string, 0, roots)
	for index := range roots {
		name := fmt.Sprintf("r%02d", index)
		names = append(names, name)
		steps = append(steps, step(name, "demo.echo"))
	}
	steps = append(steps, step("join", "demo.echo", names...))
	wf := submitWorkflow(t, s, steps...)
	for index := range roots {
		claimed := claimNext(t, s, "w")
		value := fmt.Sprintf("00-%032x-%016x-01", index+1, index+1)
		completeWithContext(t, s, claimed, &value)
	}
	join := stepByName(t, getWorkflow(t, s, wf.ID), "join")
	if join.JobID == nil {
		t.Fatal("fan-in step with 35 dependencies was not dispatched")
	}
	if _, links := jobTraceContext(t, s, *join.JobID); len(links) != 32 {
		t.Fatalf("links=%d, want the 32-link bound", len(links))
	}
}
