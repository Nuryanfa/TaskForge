package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// roundTripFunc is an in-memory transport: Run is tested without opening a
// socket, so the tests pass in sandboxes that block loopback connections.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type recordedRequest struct {
	Path        string
	ContentType string
	Body        []byte
}

// recordingClient answers every request with status(path) and records it.
func recordingClient(status func(path string) int) (*http.Client, func() []recordedRequest) {
	var mu sync.Mutex
	var requests []recordedRequest
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, recordedRequest{Path: r.URL.Path, ContentType: r.Header.Get("Content-Type"), Body: body})
		mu.Unlock()
		return &http.Response{StatusCode: status(r.URL.Path), Body: io.NopCloser(bytes.NewReader(nil)), Header: http.Header{}}, nil
	})}
	return client, func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedRequest(nil), requests...)
	}
}

func TestParseMix(t *testing.T) {
	mix, err := ParseMix("echo=60,sleep:8000=20,fail=10,flaky:2=10")
	if err != nil {
		t.Fatal(err)
	}
	if len(mix) != 4 || mix.TotalWeight() != 100 {
		t.Fatalf("mix=%+v total=%d", mix, mix.TotalWeight())
	}
	if mix[1].Path != "/v1/jobs" || string(mix[1].Body) != `{"kind":"demo.sleep","payload":{"ms":8000}}` {
		t.Fatalf("sleep entry path=%s body=%s", mix[1].Path, mix[1].Body)
	}
	if string(mix[3].Body) != `{"kind":"demo.flaky","payload":{"fail_attempts":2}}` {
		t.Fatalf("flaky entry body=%s", mix[3].Body)
	}
	for _, bad := range []string{"", "report=10", "echo=0", "sleep:60001=1", "flaky:11=1", "echo", "echo=x"} {
		if _, err := ParseMix(bad); err == nil {
			t.Fatalf("ParseMix(%q) accepted an invalid mix", bad)
		}
	}
}

type workflowBody struct {
	Steps []struct {
		Name         string          `json:"name"`
		Kind         string          `json:"kind"`
		DependsOn    []string        `json:"depends_on"`
		Compensation json.RawMessage `json:"compensation"`
	} `json:"steps"`
}

func TestParseMixSupportsWorkflowsAndRejectsInvalid(t *testing.T) {
	mix, err := ParseMix("workflow=1,workflow:fail=1,echo=1,workflow:ok=1")
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{mix[0].Path, mix[1].Path, mix[2].Path, mix[3].Path}
	if strings.Join(paths, " ") != "/v1/workflows /v1/workflows /v1/jobs /v1/workflows" {
		t.Fatalf("paths=%v", paths)
	}
	var diamond workflowBody
	if err := json.Unmarshal(mix[0].Body, &diamond); err != nil || len(diamond.Steps) != 4 {
		t.Fatalf("workflow body=%s err=%v", mix[0].Body, err)
	}
	if diamond.Steps[3].Name != "d" || strings.Join(diamond.Steps[3].DependsOn, ",") != "b,c" {
		t.Fatalf("workflow is not a diamond ending in a fan-in: %s", mix[0].Body)
	}
	if string(mix[3].Body) != string(mix[0].Body) {
		t.Fatal("workflow:ok is not an alias of workflow")
	}
	var saga workflowBody
	if err := json.Unmarshal(mix[1].Body, &saga); err != nil || len(saga.Steps) != 2 ||
		saga.Steps[0].Name != "reserve" || saga.Steps[0].Compensation == nil ||
		saga.Steps[1].Kind != "demo.fail" || strings.Join(saga.Steps[1].DependsOn, ",") != "reserve" {
		t.Fatalf("workflow:fail body=%s err=%v", mix[1].Body, err)
	}
	if _, err := ParseMix("workflow:nope=1"); err == nil || !strings.Contains(err.Error(), "workflow:nope") {
		t.Fatalf("workflow:nope error=%v", err)
	}
}

func TestRunSubmitsWorkflowsAndJobs(t *testing.T) {
	client, requests := recordingClient(func(path string) int {
		if path == "/v1/workflows" {
			return http.StatusCreated
		}
		return http.StatusInternalServerError
	})
	mix, err := ParseMix("workflow=1,echo=1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	next := 0
	stats, err := Run(ctx, client, "http://taskforge.test/", 40, mix, func() int { next++; return next % mix.TotalWeight() })
	if err != nil {
		t.Fatal(err)
	}
	got := requests()
	if len(got) < 4 {
		t.Fatalf("only %d requests", len(got))
	}
	workflows := 0
	for _, request := range got {
		if request.ContentType != "application/json" {
			t.Fatalf("content type=%q", request.ContentType)
		}
		switch request.Path {
		case "/v1/workflows":
			workflows++
			if string(request.Body) != string(mix[0].Body) {
				t.Fatalf("workflow body=%s", request.Body)
			}
		case "/v1/jobs":
			if string(request.Body) != string(mix[1].Body) {
				t.Fatalf("job body=%s", request.Body)
			}
		default:
			t.Fatalf("unexpected path %s", request.Path)
		}
	}
	if stats.Submitted != workflows || stats.Failed != len(got)-workflows {
		t.Fatalf("stats=%+v workflows=%d requests=%d", stats, workflows, len(got))
	}
}

func TestRunPacesSubmissions(t *testing.T) {
	client, requests := recordingClient(func(string) int { return http.StatusCreated })
	mix, err := ParseMix("echo=1,fail=1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	next := 0
	stats, err := Run(ctx, client, "http://taskforge.test", 20, mix, func() int { next++; return next % mix.TotalWeight() })
	if err != nil {
		t.Fatal(err)
	}
	got := requests()
	if len(got) < 8 || len(got) > 12 || stats.Submitted != len(got) || stats.Failed != 0 {
		t.Fatalf("requests=%d stats=%+v", len(got), stats)
	}
	var joined strings.Builder
	for _, request := range got {
		joined.Write(request.Body)
	}
	if !strings.Contains(joined.String(), "demo.echo") || !strings.Contains(joined.String(), "demo.fail") {
		t.Fatalf("mix not honored: %s", joined.String())
	}
}
