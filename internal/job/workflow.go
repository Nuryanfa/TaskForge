package job

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type WorkflowStatus string

const (
	WorkflowRunning            WorkflowStatus = "running"
	WorkflowCompensating       WorkflowStatus = "compensating"
	WorkflowSucceeded          WorkflowStatus = "succeeded"
	WorkflowCompensated        WorkflowStatus = "compensated"
	WorkflowCanceled           WorkflowStatus = "canceled"
	WorkflowCompensationFailed WorkflowStatus = "compensation_failed"
)

// Terminal reports whether no further transitions are possible.
func (s WorkflowStatus) Terminal() bool {
	return s == WorkflowSucceeded || s == WorkflowCompensated || s == WorkflowCanceled || s == WorkflowCompensationFailed
}

type StepStatus string

const (
	StepPending    StepStatus = "pending"
	StepDispatched StepStatus = "dispatched"
	StepSucceeded  StepStatus = "succeeded"
	StepFailed     StepStatus = "failed"
	StepCanceled   StepStatus = "canceled"
	StepSkipped    StepStatus = "skipped"
)

type CompensationStatus string

const (
	CompensationNone       CompensationStatus = "none"
	CompensationWaiting    CompensationStatus = "waiting"
	CompensationDispatched CompensationStatus = "dispatched"
	CompensationSucceeded  CompensationStatus = "succeeded"
	CompensationFailed     CompensationStatus = "failed"
	CompensationNotDefined CompensationStatus = "not_defined"
)

type AbortReason string

const (
	AbortStepFailed AbortReason = "step_failed"
	AbortCanceled   AbortReason = "canceled"
)

const (
	RoleAction       = "action"
	RoleCompensation = "compensation"
)

type WorkflowLimits struct {
	MaxSteps        int
	MaxPayloadBytes int64
}

// CompensationRequest and WorkflowStepRequest are the JSON shapes accepted by
// POST /v1/workflows.
type CompensationRequest struct {
	Queue    string          `json:"queue"`
	Kind     string          `json:"kind"`
	Priority *int            `json:"priority"`
	Payload  json.RawMessage `json:"payload"`
}

type WorkflowStepRequest struct {
	Name         string               `json:"name"`
	Queue        string               `json:"queue"`
	Kind         string               `json:"kind"`
	Priority     int                  `json:"priority"`
	Payload      json.RawMessage      `json:"payload"`
	DependsOn    []string             `json:"depends_on"`
	Compensation *CompensationRequest `json:"compensation"`
}

type CompensationSpec struct {
	Queue    string          `json:"queue"`
	Kind     string          `json:"kind"`
	Priority int16           `json:"priority"`
	Payload  json.RawMessage `json:"payload"`
}

type StepSpec struct {
	Name         string            `json:"name"`
	Queue        string            `json:"queue"`
	Kind         string            `json:"kind"`
	Priority     int16             `json:"priority"`
	Payload      json.RawMessage   `json:"payload"`
	DependsOn    []string          `json:"depends_on"`
	Compensation *CompensationSpec `json:"compensation"`
}

type WorkflowSubmission struct {
	Label          *string
	IdempotencyKey *string
	Steps          []StepSpec
	Fingerprint    [sha256.Size]byte
}

type Workflow struct {
	ID          string
	Label       *string
	Status      WorkflowStatus
	AbortReason *AbortReason
	FailedStep  *string
	ErrorCode   *string
	StepCount   int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
	Steps       []WorkflowStep
}

type WorkflowStep struct {
	ID                 int64
	Position           int
	Name               string
	Queue              string
	Kind               string
	DependsOn          []string
	Status             StepStatus
	JobID              *string
	CompensationKind   *string
	CompensationStatus CompensationStatus
	CompensationJobID  *string
}

func NewWorkflowSubmission(label, idempotencyKey *string, steps []WorkflowStepRequest, limits WorkflowLimits) (WorkflowSubmission, error) {
	if label != nil {
		if err := validateLabel(*label); err != nil {
			return WorkflowSubmission{}, err
		}
	}
	if idempotencyKey != nil {
		if err := validateIdempotencyKey(*idempotencyKey); err != nil {
			return WorkflowSubmission{}, err
		}
	}
	if len(steps) == 0 || len(steps) > limits.MaxSteps {
		return WorkflowSubmission{}, fmt.Errorf("steps must contain between 1 and %d entries", limits.MaxSteps)
	}
	specs := make([]StepSpec, 0, len(steps))
	names := make(map[string]bool, len(steps))
	for _, request := range steps {
		spec, err := newStepSpec(request, limits.MaxPayloadBytes)
		if err != nil {
			return WorkflowSubmission{}, err
		}
		if names[spec.Name] {
			return WorkflowSubmission{}, fmt.Errorf("duplicate step name %q", spec.Name)
		}
		names[spec.Name] = true
		specs = append(specs, spec)
	}
	for _, spec := range specs {
		for _, dependency := range spec.DependsOn {
			if !names[dependency] {
				return WorkflowSubmission{}, fmt.Errorf("step %q depends on unknown step %q", spec.Name, dependency)
			}
		}
	}
	if err := checkAcyclic(specs); err != nil {
		return WorkflowSubmission{}, err
	}
	fingerprint, err := workflowFingerprint(label, specs)
	if err != nil {
		return WorkflowSubmission{}, err
	}
	return WorkflowSubmission{Label: label, IdempotencyKey: idempotencyKey, Steps: specs, Fingerprint: fingerprint}, nil
}

func validateLabel(value string) error {
	if len(value) < 1 || len(value) > 128 || strings.TrimSpace(value) == "" {
		return errors.New("label must be 1-128 printable characters and not only spaces")
	}
	for _, r := range value {
		if r < 0x20 || r > 0x7e {
			return errors.New("label must be 1-128 printable characters and not only spaces")
		}
	}
	return nil
}

func newStepSpec(request WorkflowStepRequest, maxPayloadBytes int64) (StepSpec, error) {
	if !identifierPattern.MatchString(request.Name) {
		return StepSpec{}, fmt.Errorf("step name %q must be a 1-64 character lowercase identifier", request.Name)
	}
	queue := request.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	if !identifierPattern.MatchString(queue) {
		return StepSpec{}, fmt.Errorf("step %q: queue must be a 1-64 character lowercase identifier", request.Name)
	}
	if !identifierPattern.MatchString(request.Kind) {
		return StepSpec{}, fmt.Errorf("step %q: kind must be a 1-64 character lowercase identifier", request.Name)
	}
	if request.Priority < MinPriority || request.Priority > MaxPriority {
		return StepSpec{}, fmt.Errorf("step %q: priority must be between %d and %d", request.Name, MinPriority, MaxPriority)
	}
	payload, err := canonicalObject(request.Payload, maxPayloadBytes)
	if err != nil {
		return StepSpec{}, fmt.Errorf("step %q: payload: %w", request.Name, err)
	}
	dependencies := make([]string, 0, len(request.DependsOn))
	seen := make(map[string]bool, len(request.DependsOn))
	for _, dependency := range request.DependsOn {
		if dependency == request.Name {
			return StepSpec{}, fmt.Errorf("step %q cannot depend on itself", request.Name)
		}
		if seen[dependency] {
			return StepSpec{}, fmt.Errorf("step %q has duplicate dependency %q", request.Name, dependency)
		}
		seen[dependency] = true
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	spec := StepSpec{Name: request.Name, Queue: queue, Kind: request.Kind, Priority: int16(request.Priority), Payload: payload, DependsOn: dependencies}
	if request.Compensation != nil {
		compensation, err := newCompensationSpec(spec, *request.Compensation, maxPayloadBytes)
		if err != nil {
			return StepSpec{}, err
		}
		spec.Compensation = &compensation
	}
	return spec, nil
}

func newCompensationSpec(step StepSpec, request CompensationRequest, maxPayloadBytes int64) (CompensationSpec, error) {
	queue := request.Queue
	if queue == "" {
		queue = step.Queue
	}
	if !identifierPattern.MatchString(queue) {
		return CompensationSpec{}, fmt.Errorf("step %q: compensation queue must be a 1-64 character lowercase identifier", step.Name)
	}
	if !identifierPattern.MatchString(request.Kind) {
		return CompensationSpec{}, fmt.Errorf("step %q: compensation kind must be a 1-64 character lowercase identifier", step.Name)
	}
	priority := int(step.Priority)
	if request.Priority != nil {
		priority = *request.Priority
	}
	if priority < MinPriority || priority > MaxPriority {
		return CompensationSpec{}, fmt.Errorf("step %q: compensation priority must be between %d and %d", step.Name, MinPriority, MaxPriority)
	}
	payload, err := canonicalObject(request.Payload, maxPayloadBytes)
	if err != nil {
		return CompensationSpec{}, fmt.Errorf("step %q: compensation payload: %w", step.Name, err)
	}
	return CompensationSpec{Queue: queue, Kind: request.Kind, Priority: int16(priority), Payload: payload}, nil
}

// checkAcyclic runs Kahn's algorithm and names the lexicographically smallest
// step that is part of (or blocked by) a cycle.
func checkAcyclic(specs []StepSpec) error {
	remaining := make(map[string]int, len(specs))
	dependents := make(map[string][]string, len(specs))
	for _, spec := range specs {
		remaining[spec.Name] = len(spec.DependsOn)
		for _, dependency := range spec.DependsOn {
			dependents[dependency] = append(dependents[dependency], spec.Name)
		}
	}
	ready := make([]string, 0, len(specs))
	for name, count := range remaining {
		if count == 0 {
			ready = append(ready, name)
		}
	}
	visited := 0
	for len(ready) > 0 {
		name := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++
		for _, dependent := range dependents[name] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if visited == len(specs) {
		return nil
	}
	blocked := make([]string, 0)
	for name, count := range remaining {
		if count > 0 {
			blocked = append(blocked, name)
		}
	}
	sort.Strings(blocked)
	return fmt.Errorf("steps contain a dependency cycle involving %q", blocked[0])
}

func workflowFingerprint(label *string, specs []StepSpec) ([sha256.Size]byte, error) {
	sorted := append([]StepSpec(nil), specs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	encoded, err := json.Marshal(struct {
		Label *string    `json:"label"`
		Steps []StepSpec `json:"steps"`
	}{label, sorted})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode workflow fingerprint: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
