// Package loadgen submits a weighted mix of demo jobs at a fixed rate. It
// drives the SLO burn demo (docs/SLO.md); it is a development tool, not part
// of the platform.
package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Entry is one kind of job in the mix with its relative weight.
type Entry struct {
	Kind    string
	Weight  int
	Payload json.RawMessage
}

// Mix is a weighted list of entries.
type Mix []Entry

// TotalWeight is the sum of all weights.
func (m Mix) TotalWeight() int {
	total := 0
	for _, entry := range m {
		total += entry.Weight
	}
	return total
}

// pick returns the entry a value in [0, TotalWeight) falls into.
func (m Mix) pick(value int) Entry {
	for _, entry := range m {
		if value < entry.Weight {
			return entry
		}
		value -= entry.Weight
	}
	return m[len(m)-1]
}

// ParseMix parses "echo=60,sleep:8000=20,fail=10,flaky:2=10": echo, fail,
// sleep:<ms> (0..60000), and flaky:<fail_attempts> (0..10), each =<weight>.
func ParseMix(spec string) (Mix, error) {
	var mix Mix
	for _, part := range strings.Split(spec, ",") {
		name, weightText, ok := strings.Cut(strings.TrimSpace(part), "=")
		weight, err := strconv.Atoi(weightText)
		if !ok || err != nil || weight <= 0 {
			return nil, fmt.Errorf("mix entry %q must be <kind>=<positive weight>", part)
		}
		kind, argument, hasArgument := strings.Cut(name, ":")
		entry := Entry{Weight: weight}
		switch kind {
		case "echo":
			entry.Kind, entry.Payload = "demo.echo", json.RawMessage(`{"message":"loadgen"}`)
		case "fail":
			entry.Kind, entry.Payload = "demo.fail", json.RawMessage(`{}`)
		case "sleep":
			ms, err := boundedArgument(argument, hasArgument, 1000, 60000)
			if err != nil {
				return nil, fmt.Errorf("mix entry %q: sleep milliseconds %w", part, err)
			}
			entry.Kind, entry.Payload = "demo.sleep", json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms))
		case "flaky":
			failures, err := boundedArgument(argument, hasArgument, 1, 10)
			if err != nil {
				return nil, fmt.Errorf("mix entry %q: flaky failures %w", part, err)
			}
			entry.Kind, entry.Payload = "demo.flaky", json.RawMessage(fmt.Sprintf(`{"fail_attempts":%d}`, failures))
		default:
			return nil, fmt.Errorf("mix entry %q: unknown kind (use echo, fail, sleep:<ms>, flaky:<n>)", part)
		}
		mix = append(mix, entry)
	}
	return mix, nil
}

func boundedArgument(text string, present bool, fallback, maximum int) (int, error) {
	if !present {
		return fallback, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < 0 || value > maximum {
		return 0, fmt.Errorf("must be between 0 and %d", maximum)
	}
	return value, nil
}

// Stats counts submissions.
type Stats struct {
	Submitted int
	Failed    int
}

// Run submits one job per 1/rate seconds until ctx ends. pick returns a value
// in [0, mix.TotalWeight()) choosing the entry for each submission.
func Run(ctx context.Context, client *http.Client, baseURL string, rate float64, mix Mix, pick func() int) (Stats, error) {
	if rate <= 0 || len(mix) == 0 {
		return Stats{}, fmt.Errorf("rate must be positive and the mix non-empty")
	}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer ticker.Stop()
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/jobs"
	var stats Stats
	for {
		select {
		case <-ctx.Done():
			return stats, nil
		case <-ticker.C:
		}
		entry := mix.pick(pick())
		body, _ := json.Marshal(map[string]any{"kind": entry.Kind, "payload": entry.Payload})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return stats, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return stats, nil
			}
			stats.Failed++
			continue
		}
		response.Body.Close()
		if response.StatusCode == http.StatusCreated {
			stats.Submitted++
		} else {
			stats.Failed++
		}
	}
}
