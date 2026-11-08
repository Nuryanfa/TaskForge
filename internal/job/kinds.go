package job

import "slices"

// RegisteredKinds is the single source of truth for the job kinds this build
// can execute. The API rejects every other kind at submission, the worker
// registry must handle exactly these (enforced by a test), and metrics use it
// to bound the kind label.
var RegisteredKinds = []string{"demo.echo", "demo.fail", "demo.flaky", "demo.sleep"}

// IsRegisteredKind reports whether kind has a handler in this build.
func IsRegisteredKind(kind string) bool {
	return slices.Contains(RegisteredKinds, kind)
}
