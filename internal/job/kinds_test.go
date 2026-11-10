package job

import "testing"

func TestIsRegisteredKind(t *testing.T) {
	for _, kind := range []string{"demo.echo", "demo.fail", "demo.flaky", "demo.sleep"} {
		if !IsRegisteredKind(kind) {
			t.Fatalf("%s should be registered", kind)
		}
	}
	for _, kind := range []string{"report.generate", "", "demo"} {
		if IsRegisteredKind(kind) {
			t.Fatalf("%q should not be registered", kind)
		}
	}
}
