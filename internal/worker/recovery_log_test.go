package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRecoveryLogsPartialFailuresWithJobIDs: an operator must be able to find
// the jobs whose recovery was rolled back, and the jobs that were recovered in
// the same batch must still be reported.
func TestRecoveryLogsPartialFailuresWithJobIDs(t *testing.T) {
	store := &workerStore{recovered: 2, recoverErr: &postgres.RecoveryError{
		JobIDs: []string{"6f1c5c0e-0000-4000-8000-000000000001"},
		Err:    errors.New("record step outcome: state conflict"),
	}}
	logs := &syncBuffer{}
	runner := New(store, NewRegistry(1024), slog.New(slog.NewJSONHandler(logs, nil)), testOptions())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	for _, want := range []string{`"msg":"worker_jobs_recovered"`, `"count":2`, `"msg":"worker_recovery_partial"`, "6f1c5c0e-0000-4000-8000-000000000001"} {
		if !strings.Contains(output, want) {
			t.Fatalf("log output missing %s:\n%s", want, output)
		}
	}
	if strings.Contains(output, "state conflict") {
		t.Fatal("raw store error text was logged")
	}
}
