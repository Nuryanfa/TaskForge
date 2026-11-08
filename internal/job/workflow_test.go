package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var testLimits = WorkflowLimits{MaxSteps: 32, MaxPayloadBytes: 1024}

func req(name, kind string, deps ...string) WorkflowStepRequest {
	return WorkflowStepRequest{Name: name, Kind: kind, Payload: json.RawMessage(`{"message":"` + name + `"}`), DependsOn: deps}
}

func diamond() []WorkflowStepRequest {
	return []WorkflowStepRequest{req("a", "demo.echo"), req("b", "demo.echo", "a"), req("c", "demo.echo", "a"), req("d", "demo.echo", "c", "b")}
}

func TestNewWorkflowSubmissionValidation(t *testing.T) {
	valid, err := NewWorkflowSubmission(nil, nil, diamond(), testLimits)
	if err != nil {
		t.Fatalf("valid diamond: %v", err)
	}
	if len(valid.Steps) != 4 || strings.Join(valid.Steps[3].DependsOn, ",") != "b,c" {
		t.Fatalf("unexpected steps %+v", valid.Steps)
	}

	tooMany := make([]WorkflowStepRequest, 33)
	for i := range tooMany {
		tooMany[i] = req(fmt.Sprintf("s%d", i), "demo.echo")
	}
	spaces := "   "
	long := strings.Repeat("x", 129)
	badCode := req("a", "demo.echo")
	badCode.Compensation = &CompensationRequest{Kind: "", Payload: json.RawMessage(`{}`)}
	highPriority := req("a", "demo.echo")
	highPriority.Priority = 101
	nonObject := req("a", "demo.echo")
	nonObject.Payload = json.RawMessage(`[]`)
	oversized := req("a", "demo.echo")
	oversized.Payload = json.RawMessage(`{"message":"` + strings.Repeat("x", 2000) + `"}`)

	tests := []struct {
		name  string
		label *string
		steps []WorkflowStepRequest
		want  string
	}{
		{name: "zero steps", steps: nil, want: "steps"},
		{name: "too many steps", steps: tooMany, want: "steps"},
		{name: "duplicate name", steps: []WorkflowStepRequest{req("a", "demo.echo"), req("a", "demo.echo")}, want: "duplicate"},
		{name: "bad name", steps: []WorkflowStepRequest{req("Bad", "demo.echo")}, want: "name"},
		{name: "unknown dependency", steps: []WorkflowStepRequest{req("a", "demo.echo", "zzz")}, want: "unknown"},
		{name: "self dependency", steps: []WorkflowStepRequest{req("a", "demo.echo", "a")}, want: "itself"},
		{name: "duplicate dependency", steps: []WorkflowStepRequest{req("a", "demo.echo"), req("b", "demo.echo", "a", "a")}, want: "duplicate"},
		{name: "cycle", steps: []WorkflowStepRequest{req("a", "demo.echo", "c"), req("b", "demo.echo", "a"), req("c", "demo.echo", "b")}, want: "cycle"},
		{name: "non object payload", steps: []WorkflowStepRequest{nonObject}, want: "payload"},
		{name: "oversized payload", steps: []WorkflowStepRequest{oversized}, want: "payload"},
		{name: "compensation without kind", steps: []WorkflowStepRequest{badCode}, want: "kind"},
		{name: "priority too high", steps: []WorkflowStepRequest{highPriority}, want: "priority"},
		{name: "blank label", label: &spaces, steps: diamond(), want: "label"},
		{name: "long label", label: &long, steps: diamond(), want: "label"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWorkflowSubmission(tt.label, nil, tt.steps, testLimits)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestWorkflowCycleErrorNamesAStep(t *testing.T) {
	_, err := NewWorkflowSubmission(nil, nil, []WorkflowStepRequest{req("x", "demo.echo"), req("a", "demo.echo", "c"), req("b", "demo.echo", "a"), req("c", "demo.echo", "b")}, testLimits)
	if err == nil || !strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("expected cycle error naming \"a\", got %v", err)
	}
}

func TestWorkflowFingerprintIsOrderAndFormattingIndependent(t *testing.T) {
	label := "order-1"
	first, err := NewWorkflowSubmission(&label, nil, diamond(), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	reordered := []WorkflowStepRequest{req("d", "demo.echo", "b", "c"), req("c", "demo.echo", "a"), req("a", "demo.echo"), req("b", "demo.echo", "a")}
	reordered[0].Payload = json.RawMessage("{ \"message\" :  \"d\" }")
	second, err := NewWorkflowSubmission(&label, nil, reordered, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Fingerprint[:], second.Fingerprint[:]) {
		t.Fatal("equivalent workflows produced different fingerprints")
	}

	changedLabel := "order-2"
	third, _ := NewWorkflowSubmission(&changedLabel, nil, diamond(), testLimits)
	if bytes.Equal(first.Fingerprint[:], third.Fingerprint[:]) {
		t.Fatal("label change did not change the fingerprint")
	}
	withComp := diamond()
	withComp[0].Compensation = &CompensationRequest{Kind: "demo.echo", Payload: json.RawMessage(`{"message":"undo"}`)}
	fourth, _ := NewWorkflowSubmission(&label, nil, withComp, testLimits)
	withComp[0].Compensation.Payload = json.RawMessage(`{"message":"undo-2"}`)
	fifth, _ := NewWorkflowSubmission(&label, nil, withComp, testLimits)
	if bytes.Equal(fourth.Fingerprint[:], fifth.Fingerprint[:]) {
		t.Fatal("compensation payload change did not change the fingerprint")
	}
}

func TestWorkflowDefaultsAreFilled(t *testing.T) {
	steps := []WorkflowStepRequest{req("a", "demo.echo")}
	steps[0].Priority = 7
	steps[0].Queue = "billing"
	steps[0].Compensation = &CompensationRequest{Kind: "demo.echo", Payload: json.RawMessage(`{}`)}
	steps = append(steps, req("b", "demo.echo", "a"))
	sub, err := NewWorkflowSubmission(nil, nil, steps, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	comp := sub.Steps[0].Compensation
	if comp == nil || comp.Queue != "billing" || comp.Priority != 7 {
		t.Fatalf("compensation defaults not inherited: %+v", comp)
	}
	if sub.Steps[1].Queue != DefaultQueue || sub.Steps[1].Compensation != nil {
		t.Fatalf("step defaults wrong: %+v", sub.Steps[1])
	}
}
