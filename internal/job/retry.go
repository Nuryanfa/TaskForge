package job

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"time"
)

// RetryDelay returns capped exponential backoff with deterministic bounded
// jitter. failedAttempt is one-based; attempt 1 receives the initial delay.
func RetryDelay(jobID string, failedAttempt int, initial, maximum time.Duration, jitterPercent int) time.Duration {
	delay := initial
	for step := 1; step < failedAttempt && delay < maximum; step++ {
		if delay > maximum/2 {
			delay = maximum
		} else {
			delay *= 2
		}
	}
	if delay > maximum {
		delay = maximum
	}
	if jitterPercent <= 0 {
		return delay
	}
	span := delay * time.Duration(jitterPercent) / 100
	if span <= 0 {
		return delay
	}
	hash := sha256.Sum256([]byte(jobID + ":" + strconv.Itoa(failedAttempt)))
	width := uint64(span)*2 + 1
	offset := time.Duration(binary.BigEndian.Uint64(hash[:8])%width) - span
	result := delay + offset
	if result > maximum {
		return maximum
	}
	if result < 0 {
		return 0
	}
	return result
}
