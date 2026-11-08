package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

// simulateDemoHandler mirrors the semantics of the built-in demo handlers
// (internal/worker cannot be imported here: it imports this package).
func simulateDemoHandler(claimed job.Claimed) (json.RawMessage, *job.Failure) {
	var payload struct {
		FailAttempts int `json:"fail_attempts"`
		MS           int `json:"ms"`
	}
	_ = json.Unmarshal(claimed.Job.Payload, &payload)
	switch claimed.Job.Kind {
	case "demo.fail":
		return nil, &job.Failure{Code: "DEMO_FAILURE"}
	case "demo.flaky":
		if claimed.Execution.Attempt <= payload.FailAttempts {
			return nil, &job.Failure{Code: "DEMO_TRANSIENT", Retryable: true}
		}
	case "demo.sleep":
		time.Sleep(time.Duration(payload.MS) * time.Millisecond)
	}
	return json.RawMessage(`{"ok":true}`), nil
}

func randomWorkflow(random *rand.Rand) []job.WorkflowStepRequest {
	count := 2 + random.IntN(11)
	steps := make([]job.WorkflowStepRequest, 0, count)
	for i := range count {
		name := fmt.Sprintf("s%02d", i)
		var deps []string
		if i > 0 {
			chosen := map[int]bool{}
			for range random.IntN(min(i, 3) + 1) {
				chosen[random.IntN(i)] = true
			}
			for index := range chosen {
				deps = append(deps, fmt.Sprintf("s%02d", index))
			}
		}
		request := job.WorkflowStepRequest{Name: name, DependsOn: deps}
		switch roll := random.IntN(100); {
		case roll < 60:
			request.Kind, request.Payload = "demo.echo", json.RawMessage(`{"message":"`+name+`"}`)
		case roll < 75:
			request.Kind, request.Payload = "demo.fail", json.RawMessage(`{}`)
		case roll < 90:
			request.Kind, request.Payload = "demo.flaky", json.RawMessage(fmt.Sprintf(`{"fail_attempts":%d}`, random.IntN(3)))
		default:
			request.Kind, request.Payload = "demo.sleep", json.RawMessage(fmt.Sprintf(`{"ms":%d}`, random.IntN(51)))
		}
		if random.IntN(100) < 70 {
			compensation := &job.CompensationRequest{Kind: "demo.echo", Payload: json.RawMessage(`{"message":"undo"}`)}
			switch roll := random.IntN(100); {
			case roll < 10:
				compensation.Kind, compensation.Payload = "demo.flaky", json.RawMessage(`{"fail_attempts":1}`)
			case roll < 13:
				compensation.Kind, compensation.Payload = "demo.fail", json.RawMessage(`{}`)
			}
			request.Compensation = compensation
		}
		steps = append(steps, request)
	}
	return steps
}

// TestWorkflowSoakIntegration runs random DAGs with random failures, random
// cancels, four concurrent workers that randomly crash mid-job, and a recovery
// loop, then checks every invariant on every workflow. Reproduce a failure
// with TASKFORGE_SOAK_SEED=<seed printed in the log>.
func TestWorkflowSoakIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}
	seed := uint64(time.Now().UnixNano())
	if raw := os.Getenv("TASKFORGE_SOAK_SEED"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid TASKFORGE_SOAK_SEED: %v", err)
		}
		seed = parsed
	}
	t.Logf("soak seed=%d (rerun with TASKFORGE_SOAK_SEED=%d)", seed, seed)
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	store := workflowStore(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const workflows = 25
	ids := make([]string, 0, workflows)
	cancelAfter := map[string]time.Duration{}
	for range workflows {
		wf := submitWorkflow(t, store, randomWorkflow(random)...)
		ids = append(ids, wf.ID)
		if random.IntN(100) < 20 {
			cancelAfter[wf.ID] = time.Duration(random.IntN(300)) * time.Millisecond
		}
	}

	var background sync.WaitGroup
	failures := make(chan error, 64)
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	var crashes, claims atomic.Int64
	for i := range 4 {
		workerRandom := rand.New(rand.NewPCG(seed+uint64(i)+1, seed))
		background.Add(1)
		go func() {
			defer background.Done()
			workerID := fmt.Sprintf("soak-%d", i)
			for ctx.Err() == nil {
				claimed, err := store.ClaimNextJob(ctx, workerID, 500*time.Millisecond)
				if errors.Is(err, ErrNotFound) {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						report(fmt.Errorf("claim: %w", err))
					}
					return
				}
				claims.Add(1)
				if workerRandom.IntN(100) < 5 {
					crashes.Add(1) // crash: walk away without heartbeat or outcome
					continue
				}
				result, failure := simulateDemoHandler(claimed)
				if failure == nil {
					err = store.CompleteJob(ctx, claimed.Execution, result)
				} else {
					err = store.FailJob(ctx, claimed.Execution, *failure)
				}
				if err != nil && !errors.Is(err, ErrOwnershipLost) && ctx.Err() == nil {
					report(fmt.Errorf("outcome for job %s: %w", claimed.Job.ID, err))
					return
				}
			}
		}()
	}
	background.Add(1)
	go func() {
		defer background.Done()
		for ctx.Err() == nil {
			if _, err := store.RecoverExpiredJobs(ctx, 50); err != nil && ctx.Err() == nil {
				report(fmt.Errorf("recovery: %w", err))
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for id, delay := range cancelAfter {
		background.Add(1)
		go func() {
			defer background.Done()
			time.Sleep(delay)
			if _, err := store.CancelWorkflow(ctx, id); err != nil && !errors.Is(err, ErrStateConflict) && ctx.Err() == nil {
				report(fmt.Errorf("cancel %s: %w", id, err))
			}
		}()
	}

	// Wait for quiescence: every workflow terminal and no workflow job live
	// (compensation_failed lets in-flight work finish after it is terminal).
	for {
		select {
		case err := <-failures:
			cancel()
			background.Wait()
			t.Fatalf("seed=%d: %v", seed, err)
		default:
		}
		var open int
		if err := store.pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM workflows WHERE status IN ('running','compensating'))
			+ (SELECT count(*) FROM jobs WHERE workflow_id IS NOT NULL AND status IN ('queued','running'))`).Scan(&open); err != nil {
			cancel()
			background.Wait()
			t.Fatalf("seed=%d: soak did not settle: %v", seed, err)
		}
		if open == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	background.Wait()

	statuses := map[job.WorkflowStatus]int{}
	for _, id := range ids {
		statuses[getWorkflow(t, store, id).Status]++
		for _, violation := range workflowInvariantViolations(t, store, id) {
			t.Errorf("seed=%d workflow %s: %s", seed, id, violation)
		}
	}
	t.Logf("soak seed=%d outcomes=%v claims=%d crashes=%d", seed, statuses, claims.Load(), crashes.Load())
}
