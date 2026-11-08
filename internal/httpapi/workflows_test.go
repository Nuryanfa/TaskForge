package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/google/uuid"
)

func (f *fakeStore) CreateWorkflow(_ context.Context, submission job.WorkflowSubmission) (job.Workflow, bool, error) {
	f.lastWorkflow = submission
	return f.workflow, f.workflowCreated, f.workflowErr
}
func (f *fakeStore) GetWorkflow(context.Context, string) (job.Workflow, error) {
	return f.workflow, f.workflowErr
}
func (f *fakeStore) ListWorkflows(context.Context, *time.Time, string, int) ([]job.Workflow, bool, error) {
	return f.workflows, f.hasMore, f.workflowErr
}
func (f *fakeStore) CancelWorkflow(context.Context, string) (job.Workflow, error) {
	return f.workflow, f.workflowErr
}

func sampleWorkflow() job.Workflow {
	jobID, compJobID, compKind := uuid.NewString(), uuid.NewString(), "demo.echo"
	reason, failed, code := job.AbortStepFailed, "ship", "DEMO_FAILURE"
	return job.Workflow{
		ID: uuid.NewString(), Status: job.WorkflowCompensating, AbortReason: &reason, FailedStep: &failed,
		ErrorCode: &code, StepCount: 2, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Steps: []job.WorkflowStep{
			{Name: "reserve", Kind: "demo.echo", Queue: "default", DependsOn: []string{}, Status: job.StepSucceeded,
				JobID: &jobID, CompensationKind: &compKind, CompensationStatus: job.CompensationDispatched, CompensationJobID: &compJobID},
			{Name: "ship", Kind: "demo.fail", Queue: "default", DependsOn: []string{"reserve"}, Status: job.StepFailed,
				CompensationStatus: job.CompensationNone},
		},
	}
}

const workflowBody = `{"label":"order-1","idempotency_key":"private-workflow-key","steps":[
	{"name":"reserve","kind":"demo.echo","payload":{"message":"secret-reserve"},
	 "compensation":{"kind":"demo.echo","payload":{"message":"secret-release"}}},
	{"name":"ship","kind":"demo.fail","payload":{},"depends_on":["reserve"]}]}`

func serve(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func errorCode(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("not an error envelope: %s", response.Body.String())
	}
	return envelope.Error.Code
}

func workflowHandler(store *fakeStore) http.Handler {
	return NewWithOptions(store, Options{PayloadMaxBytes: 1024, ReadinessTimeout: time.Second, PageSize: 50,
		WorkflowMaxSteps: 3, WorkflowMaxBytes: 2048, WorkflowPageSize: 10})
}

func TestWorkflowAPISubmitStatusesAndPrivacy(t *testing.T) {
	store := &fakeStore{workflow: sampleWorkflow(), workflowCreated: true}
	handler := workflowHandler(store)
	created := serve(handler, http.MethodPost, "/v1/workflows", workflowBody)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	if store.lastWorkflow.IdempotencyKey == nil || len(store.lastWorkflow.Steps) != 2 || *store.lastWorkflow.Label != "order-1" {
		t.Fatalf("submission=%+v", store.lastWorkflow)
	}
	for _, secret := range []string{"payload", "secret-reserve", "secret-release", "private-workflow-key"} {
		if strings.Contains(created.Body.String(), secret) {
			t.Fatalf("response exposed %q: %s", secret, created.Body.String())
		}
	}
	var body struct {
		Status      string `json:"status"`
		AbortReason string `json:"abort_reason"`
		FailedStep  string `json:"failed_step"`
		Steps       []struct {
			Name         string   `json:"name"`
			DependsOn    []string `json:"depends_on"`
			Status       string   `json:"status"`
			Compensation *struct {
				Kind   string `json:"kind"`
				Status string `json:"status"`
				JobID  string `json:"job_id"`
			} `json:"compensation"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "compensating" || body.AbortReason != "step_failed" || body.FailedStep != "ship" || len(body.Steps) != 2 ||
		body.Steps[0].Compensation == nil || body.Steps[0].Compensation.Status != "dispatched" || body.Steps[1].Compensation != nil ||
		body.Steps[1].DependsOn[0] != "reserve" || body.Steps[0].DependsOn == nil {
		t.Fatalf("body=%s", created.Body.String())
	}

	store.workflowCreated = false
	if replay := serve(handler, http.MethodPost, "/v1/workflows", workflowBody); replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d", replay.Code)
	}
	store.workflowErr = postgres.ErrIdempotencyConflict
	if conflict := serve(handler, http.MethodPost, "/v1/workflows", workflowBody); conflict.Code != http.StatusConflict || errorCode(t, conflict) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}

	invalid := map[string]struct {
		body   string
		status int
		code   string
	}{
		"cycle":              {`{"steps":[{"name":"a","kind":"demo.echo","payload":{},"depends_on":["b"]},{"name":"b","kind":"demo.echo","payload":{},"depends_on":["a"]}]}`, http.StatusBadRequest, "VALIDATION_FAILED"},
		"too many steps":     {`{"steps":[{"name":"a","kind":"x","payload":{}},{"name":"b","kind":"x","payload":{}},{"name":"c","kind":"x","payload":{}},{"name":"d","kind":"x","payload":{}}]}`, http.StatusBadRequest, "VALIDATION_FAILED"},
		"unknown step field": {`{"steps":[{"name":"a","kind":"x","payload":{},"run_at":"2026-01-01T00:00:00Z"}]}`, http.StatusBadRequest, "INVALID_JSON"},
		"body too large":     {`{"steps":[{"name":"a","kind":"x","payload":{"m":"` + strings.Repeat("x", 3000) + `"}}]}`, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE"},
		"payload too large":  {`{"steps":[{"name":"a","kind":"x","payload":{"m":"` + strings.Repeat("x", 1100) + `"}}]}`, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE"},
	}
	store.workflowErr = nil
	for name, tt := range invalid {
		response := serve(handler, http.MethodPost, "/v1/workflows", tt.body)
		if response.Code != tt.status || errorCode(t, response) != tt.code {
			t.Fatalf("%s: status=%d body=%s", name, response.Code, response.Body.String())
		}
	}
}

func TestWorkflowAPIGetListCancel(t *testing.T) {
	sample := sampleWorkflow()
	store := &fakeStore{workflow: sample, workflows: []job.Workflow{{ID: sample.ID, Status: job.WorkflowRunning, StepCount: 4, CreatedAt: time.Now(), UpdatedAt: time.Now()}}, hasMore: true}
	handler := workflowHandler(store)
	if got := serve(handler, http.MethodGet, "/v1/workflows/"+sample.ID, ""); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"steps"`) {
		t.Fatalf("get status=%d body=%s", got.Code, got.Body.String())
	}
	if bad := serve(handler, http.MethodGet, "/v1/workflows/not-a-uuid", ""); bad.Code != http.StatusBadRequest || errorCode(t, bad) != "INVALID_WORKFLOW_ID" {
		t.Fatalf("bad id status=%d", bad.Code)
	}
	list := serve(handler, http.MethodGet, "/v1/workflows?limit=1", "")
	var page struct {
		Items []struct {
			StepCount int             `json:"step_count"`
			Steps     json.RawMessage `json:"steps"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Items[0].StepCount != 4 || page.Items[0].Steps != nil || page.NextCursor == "" {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	if _, err := base64.RawURLEncoding.DecodeString(page.NextCursor); err != nil {
		t.Fatal(err)
	}
	if next := serve(handler, http.MethodGet, "/v1/workflows?cursor="+page.NextCursor, ""); next.Code != http.StatusOK {
		t.Fatalf("cursor round trip status=%d", next.Code)
	}
	if over := serve(handler, http.MethodGet, "/v1/workflows?limit=11", ""); over.Code != http.StatusBadRequest {
		t.Fatalf("limit above workflow page size status=%d", over.Code)
	}
	if canceled := serve(handler, http.MethodPost, "/v1/workflows/"+sample.ID+"/cancel", ""); canceled.Code != http.StatusOK {
		t.Fatalf("cancel status=%d", canceled.Code)
	}
	store.workflowErr = postgres.ErrStateConflict
	if conflict := serve(handler, http.MethodPost, "/v1/workflows/"+sample.ID+"/cancel", ""); conflict.Code != http.StatusConflict || errorCode(t, conflict) != "WORKFLOW_STATE_CONFLICT" {
		t.Fatalf("cancel conflict status=%d", conflict.Code)
	}
	store.workflowErr = postgres.ErrNotFound
	if missing := serve(handler, http.MethodGet, "/v1/workflows/"+uuid.NewString(), ""); missing.Code != http.StatusNotFound || errorCode(t, missing) != "WORKFLOW_NOT_FOUND" {
		t.Fatalf("missing status=%d", missing.Code)
	}
	if wrong := serve(handler, http.MethodDelete, "/v1/workflows/"+sample.ID, ""); wrong.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d", wrong.Code)
	}
}

func TestJobCancelWorkflowOwned409(t *testing.T) {
	handler := New(&fakeStore{err: postgres.ErrWorkflowOwnedJob}, 1024, time.Second)
	response := serve(handler, http.MethodPost, "/v1/jobs/"+uuid.NewString()+"/cancel", "")
	if response.Code != http.StatusConflict || errorCode(t, response) != "WORKFLOW_OWNED_JOB" {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestJobResponseIncludesWorkflowLink(t *testing.T) {
	workflowID, role := uuid.NewString(), job.RoleCompensation
	store := &fakeStore{value: job.Job{ID: uuid.NewString(), Status: job.StatusQueued, WorkflowID: &workflowID, WorkflowRole: &role}}
	response := serve(New(store, 1024, time.Second), http.MethodGet, "/v1/jobs/"+store.value.ID, "")
	if !strings.Contains(response.Body.String(), `"workflow_id":"`+workflowID+`"`) || !strings.Contains(response.Body.String(), `"workflow_role":"compensation"`) {
		t.Fatalf("body=%s", response.Body.String())
	}
}
