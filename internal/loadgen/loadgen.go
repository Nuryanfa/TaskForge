// Package loadgen submits a weighted mix of demo jobs and workflows at a fixed rate. It
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

// Entry is one kind of submission in the mix with its relative weight: Body
// is the complete JSON request body posted to Path (/v1/jobs or
// /v1/workflows).
type Entry struct {
	Name   string
	Weight int
	Path   string
	Body   json.RawMessage
}

// Workflow bodies. "workflow" is a diamond (a, then b and c, then d) so the
// fan-in step d exercises span links; "workflow:fail" is a saga whose second
// step fails and compensates the first.
const (
	workflowOK = `{"steps":[` +
		`{"name":"a","kind":"demo.echo","payload":{"message":"a"}},` +
		`{"name":"b","kind":"demo.echo","payload":{"message":"b"},"depends_on":["a"]},` +
		`{"name":"c","kind":"demo.echo","payload":{"message":"c"},"depends_on":["a"]},` +
		`{"name":"d","kind":"demo.echo","payload":{"message":"d"},"depends_on":["b","c"]}]}`
	workflowFail = `{"steps":[` +
		`{"name":"reserve","kind":"demo.echo","payload":{"message":"reserve"},` +
		`"compensation":{"kind":"demo.echo","payload":{"message":"release"}}},` +
		`{"name":"ship","kind":"demo.fail","payload":{},"depends_on":["reserve"]}]}`
)

func jobEntry(kind, payload string) (string, json.RawMessage) {
	return "/v1/jobs", json.RawMessage(fmt.Sprintf(`{"kind":%q,"payload":%s}`, kind, payload))
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
// sleep:<ms> (0..60000), flaky:<fail_attempts> (0..10), workflow (alias
// workflow:ok), and workflow:fail, each =<weight>.
func ParseMix(spec string) (Mix, error) {
	var mix Mix
	for _, part := range strings.Split(spec, ",") {
		name, weightText, ok := strings.Cut(strings.TrimSpace(part), "=")
		weight, err := strconv.Atoi(weightText)
		if !ok || err != nil || weight <= 0 {
			return nil, fmt.Errorf("mix entry %q must be <kind>=<positive weight>", part)
		}
		kind, argument, hasArgument := strings.Cut(name, ":")
		entry := Entry{Name: name, Weight: weight}
		switch kind {
		case "echo":
			entry.Path, entry.Body = jobEntry("demo.echo", `{"message":"loadgen"}`)
		case "fail":
			entry.Path, entry.Body = jobEntry("demo.fail", `{}`)
		case "sleep":
			ms, err := boundedArgument(argument, hasArgument, 1000, 60000)
			if err != nil {
				return nil, fmt.Errorf("mix entry %q: sleep milliseconds %w", part, err)
			}
			entry.Path, entry.Body = jobEntry("demo.sleep", fmt.Sprintf(`{"ms":%d}`, ms))
		case "flaky":
			failures, err := boundedArgument(argument, hasArgument, 1, 10)
			if err != nil {
				return nil, fmt.Errorf("mix entry %q: flaky failures %w", part, err)
			}
			entry.Path, entry.Body = jobEntry("demo.flaky", fmt.Sprintf(`{"fail_attempts":%d}`, failures))
		case "workflow":
			switch {
			case !hasArgument || argument == "ok":
				entry.Path, entry.Body = "/v1/workflows", json.RawMessage(workflowOK)
			case argument == "fail":
				entry.Path, entry.Body = "/v1/workflows", json.RawMessage(workflowFail)
			default:
				return nil, fmt.Errorf("mix entry %q: unknown workflow %q (use workflow, workflow:ok, workflow:fail)", part, name)
			}
		default:
			return nil, fmt.Errorf("mix entry %q: unknown kind (use echo, fail, sleep:<ms>, flaky:<n>, workflow, workflow:fail)", part)
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

// Run submits one entry per 1/rate seconds until ctx ends. pick returns a value
// in [0, mix.TotalWeight()) choosing the entry for each submission.
func Run(ctx context.Context, client *http.Client, baseURL string, rate float64, mix Mix, pick func() int) (Stats, error) {
	if rate <= 0 || len(mix) == 0 {
		return Stats{}, fmt.Errorf("rate must be positive and the mix non-empty")
	}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer ticker.Stop()
	base := strings.TrimRight(baseURL, "/")
	var stats Stats
	for {
		select {
		case <-ctx.Done():
			return stats, nil
		case <-ticker.C:
		}
		entry := mix.pick(pick())
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+entry.Path, bytes.NewReader(entry.Body))
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
