package worker

import (
	"slices"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

// TestRegisteredKindsMatchRegistry keeps the API's accepted kinds and the
// worker's handlers in one source of truth: if they diverge, a submitted job
// could fail at run time with the platform error UNKNOWN_JOB_KIND.
func TestRegisteredKindsMatchRegistry(t *testing.T) {
	registry := NewRegistry(1024)
	var handled []string
	for kind := range registry.handlers {
		handled = append(handled, kind)
	}
	slices.Sort(handled)
	declared := slices.Clone(job.RegisteredKinds)
	slices.Sort(declared)
	if !slices.Equal(handled, declared) {
		t.Fatalf("registry handles %v but job.RegisteredKinds is %v", handled, declared)
	}
}
