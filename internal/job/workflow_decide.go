package job

import "sort"

// StepState is the engine's view of one step, independent of storage.
type StepState struct {
	Name         string
	DependsOn    []string
	Status       StepStatus
	Compensation CompensationStatus
}

// Dependents returns the sorted names of steps that directly depend on name.
func Dependents(steps []StepState, name string) []string {
	var result []string
	for _, step := range steps {
		for _, dependency := range step.DependsOn {
			if dependency == name {
				result = append(result, step.Name)
				break
			}
		}
	}
	sort.Strings(result)
	return result
}

// ReadyCompensations returns the sorted names of waiting compensations whose
// direct dependents are settled: none still running its action, and every
// succeeded dependent already compensated (or with nothing to compensate).
// This walks the succeeded subgraph in reverse dependency order.
func ReadyCompensations(steps []StepState) []string {
	byName := make(map[string]StepState, len(steps))
	for _, step := range steps {
		byName[step.Name] = step
	}
	var ready []string
	for _, step := range steps {
		if step.Compensation != CompensationWaiting {
			continue
		}
		blocked := false
		for _, name := range Dependents(steps, step.Name) {
			dependent := byName[name]
			if dependent.Status == StepDispatched {
				blocked = true
				break
			}
			if dependent.Status == StepSucceeded &&
				dependent.Compensation != CompensationSucceeded && dependent.Compensation != CompensationNotDefined {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, step.Name)
		}
	}
	sort.Strings(ready)
	return ready
}

// Settle returns the terminal status a workflow should move to, or ok=false
// if it must stay where it is. compensation_failed is decided by the engine
// directly, never here.
func Settle(status WorkflowStatus, reason *AbortReason, steps []StepState) (WorkflowStatus, bool) {
	switch status {
	case WorkflowRunning:
		for _, step := range steps {
			if step.Status != StepSucceeded {
				return "", false
			}
		}
		return WorkflowSucceeded, true
	case WorkflowCompensating:
		for _, step := range steps {
			if step.Status == StepDispatched ||
				step.Compensation == CompensationWaiting || step.Compensation == CompensationDispatched {
				return "", false
			}
		}
		if reason != nil && *reason == AbortCanceled {
			return WorkflowCanceled, true
		}
		return WorkflowCompensated, true
	default:
		return "", false
	}
}
