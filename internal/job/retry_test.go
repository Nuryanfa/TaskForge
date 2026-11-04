package job

import (
	"testing"
	"time"
)

func TestRetryDelayGrowthCapAndDeterministicJitter(t *testing.T) {
	if got := RetryDelay("job", 1, time.Second, 5*time.Second, 0); got != time.Second {
		t.Fatalf("first=%s", got)
	}
	if got := RetryDelay("job", 3, time.Second, 5*time.Second, 0); got != 4*time.Second {
		t.Fatalf("third=%s", got)
	}
	if got := RetryDelay("job", 100, time.Second, 5*time.Second, 0); got != 5*time.Second {
		t.Fatalf("capped=%s", got)
	}
	first := RetryDelay("job", 2, time.Second, 5*time.Second, 20)
	second := RetryDelay("job", 2, time.Second, 5*time.Second, 20)
	if first != second || first < 1600*time.Millisecond || first > 2400*time.Millisecond {
		t.Fatalf("jitter=%s/%s", first, second)
	}
	if got := RetryDelay("job", 100, time.Second, 5*time.Second, 100); got > 5*time.Second {
		t.Fatalf("jitter exceeded cap: %s", got)
	}
}
