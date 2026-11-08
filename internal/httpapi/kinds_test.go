package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

func TestSubmitRejectsUnregisteredKinds(t *testing.T) {
	store := &fakeStore{created: true, value: job.Job{ID: "00000000-0000-4000-8000-000000000001", Status: job.StatusQueued},
		workflow: sampleWorkflow(), workflowCreated: true,
		schedule: job.Schedule{ID: "00000000-0000-4000-8000-000000000002"}}
	handler := NewWithOptions(store, Options{PayloadMaxBytes: 4096, ReadinessTimeout: time.Second})
	cases := []struct {
		name, path, body, mention string
	}{
		{"job", "/v1/jobs", `{"kind":"report.generate","payload":{}}`, "report.generate"},
		{"schedule", "/v1/schedules", `{"name":"nightly","kind":"report.generate","payload":{},"cron_expr":"0 2 * * *"}`, "report.generate"},
		{"workflow step", "/v1/workflows", `{"steps":[{"name":"a","kind":"report.generate","payload":{}}]}`, `step \"a\"`},
		{"workflow compensation", "/v1/workflows", `{"steps":[{"name":"a","kind":"demo.echo","payload":{"message":"x"},"compensation":{"kind":"undo.report","payload":{}}}]}`, "undo.report"},
	}
	for _, tt := range cases {
		response := serve(handler, http.MethodPost, tt.path, tt.body)
		if response.Code != http.StatusBadRequest || errorCode(t, response) != "UNSUPPORTED_JOB_KIND" {
			t.Fatalf("%s: status=%d body=%s", tt.name, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Body.String(), tt.mention) {
			t.Fatalf("%s: message does not mention %s: %s", tt.name, tt.mention, response.Body.String())
		}
	}
	accepted := serve(handler, http.MethodPost, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"ok"}}`)
	if accepted.Code != http.StatusCreated {
		t.Fatalf("registered kind status=%d body=%s", accepted.Code, accepted.Body.String())
	}
}
