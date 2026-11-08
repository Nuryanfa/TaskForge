package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/tracectx"
	"go.opentelemetry.io/otel/trace"
)

func tracedContext() (context.Context, string) {
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	return ctx, *tracectx.FromContext(ctx)
}

func TestTraceParentStampedOnCreateIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx, want := tracedContext()
	newJob := func(runAt *time.Time, delay *int) job.Job {
		t.Helper()
		submission, err := job.NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{"message":"x"}`), 0, nil, runAt, delay, time.Now(), 1024)
		if err != nil {
			t.Fatal(err)
		}
		created, _, err := store.CreateJob(ctx, submission)
		if err != nil {
			t.Fatal(err)
		}
		return created
	}
	immediate := newJob(nil, nil)
	if immediate.TraceParent == nil || *immediate.TraceParent != want || immediate.TraceLink {
		t.Fatalf("immediate trace_parent=%v link=%v", immediate.TraceParent, immediate.TraceLink)
	}
	sixty := 60
	if delayed := newJob(nil, &sixty); !delayed.TraceLink || delayed.TraceParent == nil {
		t.Fatalf("delayed job link=%v parent=%v", delayed.TraceLink, delayed.TraceParent)
	}
	past := time.Now().Add(-time.Hour)
	if pastRunAt := newJob(&past, nil); pastRunAt.TraceLink {
		t.Fatal("a run_at in the past is ready immediately and must not link")
	}

	submission, err := job.NewWorkflowSubmission(nil, nil, []job.WorkflowStepRequest{step("a", "demo.echo")}, workflowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	wf, _, err := store.CreateWorkflow(ctx, submission)
	if err != nil {
		t.Fatal(err)
	}
	var workflowParent *string
	if err := store.pool.QueryRow(context.Background(), `SELECT trace_parent FROM workflows WHERE id=$1`, wf.ID).Scan(&workflowParent); err != nil || workflowParent == nil || *workflowParent != want {
		t.Fatalf("workflow trace_parent=%v err=%v", workflowParent, err)
	}
	root, err := store.GetJob(context.Background(), *wf.Steps[0].JobID)
	if err != nil || root.TraceParent == nil || *root.TraceParent != want || root.TraceLink {
		t.Fatalf("root job trace_parent=%v link=%v err=%v", root.TraceParent, root.TraceLink, err)
	}

	untraced, _, err := store.CreateJob(context.Background(), submission2(t))
	if err != nil || untraced.TraceParent != nil {
		t.Fatalf("background ctx stored trace_parent=%v err=%v", untraced.TraceParent, err)
	}
}

func submission2(t *testing.T) job.Submission {
	t.Helper()
	value, err := job.NewSubmission("default", "demo.echo", json.RawMessage(`{"message":"plain"}`), 0, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
