package job

import (
	"reflect"
	"testing"
)

func state(name string, status StepStatus, comp CompensationStatus, deps ...string) StepState {
	return StepState{Name: name, DependsOn: deps, Status: status, Compensation: comp}
}

func TestDependents(t *testing.T) {
	steps := []StepState{state("a", StepPending, CompensationNone), state("c", StepPending, CompensationNone, "a"), state("b", StepPending, CompensationNone, "a")}
	if got := Dependents(steps, "a"); !reflect.DeepEqual(got, []string{"b", "c"}) {
		t.Fatalf("Dependents = %v", got)
	}
	if got := Dependents(steps, "b"); len(got) != 0 {
		t.Fatalf("leaf dependents = %v", got)
	}
}

func TestReadyCompensationsReverseDiamond(t *testing.T) {
	steps := []StepState{
		state("a", StepSucceeded, CompensationWaiting),
		state("b", StepSucceeded, CompensationWaiting, "a"),
		state("c", StepSucceeded, CompensationWaiting, "a"),
		state("d", StepSucceeded, CompensationNotDefined, "b", "c"),
	}
	if got := ReadyCompensations(steps); !reflect.DeepEqual(got, []string{"b", "c"}) {
		t.Fatalf("first wave = %v", got)
	}
	steps[1].Compensation = CompensationDispatched
	if got := ReadyCompensations(steps); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("with b dispatched = %v", got)
	}
	steps[1].Compensation = CompensationSucceeded
	steps[2].Compensation = CompensationSucceeded
	if got := ReadyCompensations(steps); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("second wave = %v", got)
	}
}

func TestReadyCompensationsWaitsForInFlightDependent(t *testing.T) {
	steps := []StepState{state("a", StepSucceeded, CompensationWaiting), state("b", StepDispatched, CompensationNone, "a")}
	if got := ReadyCompensations(steps); len(got) != 0 {
		t.Fatalf("ready = %v, want none while b is in flight", got)
	}
}

func TestReadyCompensationsIgnoresFailedDependents(t *testing.T) {
	steps := []StepState{state("a", StepSucceeded, CompensationWaiting), state("b", StepFailed, CompensationNone, "a"), state("c", StepSkipped, CompensationNone, "a")}
	if got := ReadyCompensations(steps); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("ready = %v", got)
	}
}

func TestSettle(t *testing.T) {
	failed, canceled := AbortStepFailed, AbortCanceled
	tests := []struct {
		name   string
		status WorkflowStatus
		reason *AbortReason
		steps  []StepState
		want   WorkflowStatus
		ok     bool
	}{
		{"running all succeeded", WorkflowRunning, nil, []StepState{state("a", StepSucceeded, CompensationNotDefined)}, WorkflowSucceeded, true},
		{"running in flight", WorkflowRunning, nil, []StepState{state("a", StepSucceeded, CompensationWaiting), state("b", StepDispatched, CompensationNone, "a")}, "", false},
		{"compensating waiting", WorkflowCompensating, &failed, []StepState{state("a", StepSucceeded, CompensationWaiting), state("b", StepFailed, CompensationNone, "a")}, "", false},
		{"compensating dispatched", WorkflowCompensating, &failed, []StepState{state("a", StepSucceeded, CompensationDispatched)}, "", false},
		{"compensating step in flight", WorkflowCompensating, &canceled, []StepState{state("a", StepDispatched, CompensationNone)}, "", false},
		{"compensated", WorkflowCompensating, &failed, []StepState{state("a", StepSucceeded, CompensationSucceeded), state("b", StepFailed, CompensationNone, "a"), state("c", StepSkipped, CompensationNone, "b")}, WorkflowCompensated, true},
		{"canceled", WorkflowCompensating, &canceled, []StepState{state("a", StepSucceeded, CompensationNotDefined), state("b", StepCanceled, CompensationNone, "a")}, WorkflowCanceled, true},
		{"terminal stays", WorkflowCompensationFailed, &failed, []StepState{state("a", StepSucceeded, CompensationFailed)}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Settle(tt.status, tt.reason, tt.steps)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("Settle = %q, %v; want %q, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}
