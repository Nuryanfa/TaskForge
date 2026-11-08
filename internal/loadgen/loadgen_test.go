package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseMix(t *testing.T) {
	mix, err := ParseMix("echo=60,sleep:8000=20,fail=10,flaky:2=10")
	if err != nil {
		t.Fatal(err)
	}
	if len(mix) != 4 || mix.TotalWeight() != 100 {
		t.Fatalf("mix=%+v total=%d", mix, mix.TotalWeight())
	}
	if mix[1].Kind != "demo.sleep" || string(mix[1].Payload) != `{"ms":8000}` {
		t.Fatalf("sleep entry=%+v payload=%s", mix[1], mix[1].Payload)
	}
	if mix[3].Kind != "demo.flaky" || string(mix[3].Payload) != `{"fail_attempts":2}` {
		t.Fatalf("flaky entry payload=%s", mix[3].Payload)
	}
	for _, bad := range []string{"", "report=10", "echo=0", "sleep:60001=1", "flaky:11=1", "echo", "echo=x"} {
		if _, err := ParseMix(bad); err == nil {
			t.Fatalf("ParseMix(%q) accepted an invalid mix", bad)
		}
	}
}

func TestRunPacesSubmissions(t *testing.T) {
	var mu sync.Mutex
	var kinds []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		if r.URL.Path != "/v1/jobs" || json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		kinds = append(kinds, body.Kind)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	mix, err := ParseMix("echo=1,fail=1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	next := 0
	stats, err := Run(ctx, server.Client(), server.URL, 20, mix, func() int { next++; return next % mix.TotalWeight() })
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(kinds) < 8 || len(kinds) > 12 || stats.Submitted != len(kinds) || stats.Failed != 0 {
		t.Fatalf("requests=%d stats=%+v", len(kinds), stats)
	}
	joined := strings.Join(kinds, ",")
	if !strings.Contains(joined, "demo.echo") || !strings.Contains(joined, "demo.fail") {
		t.Fatalf("mix not honored: %s", joined)
	}
}
