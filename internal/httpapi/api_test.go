package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/google/uuid"
)

type fakeStore struct {
	created   bool
	err       error
	value     job.Job
	letters   []job.DeadLetter
	schedules []job.Schedule
	schedule  job.Schedule
	hasMore   bool
	// Workflow responses; workflowErr is returned by every workflow method.
	workflow        job.Workflow
	workflows       []job.Workflow
	workflowCreated bool
	workflowErr     error
	lastWorkflow    job.WorkflowSubmission
	lastCtx         context.Context
}

func (f *fakeStore) CreateJob(ctx context.Context, _ job.Submission) (job.Job, bool, error) {
	f.lastCtx = ctx
	return f.value, f.created, f.err
}
func (f *fakeStore) GetJob(context.Context, string) (job.Job, error) { return f.value, f.err }
func (f *fakeStore) CancelQueuedJob(context.Context, string) (job.Job, error) {
	return f.value, f.err
}
func (f *fakeStore) Ping(context.Context) error { return f.err }
func (f *fakeStore) ListDeadLetters(context.Context, *time.Time, string, int) ([]job.DeadLetter, bool, error) {
	return f.letters, f.hasMore, f.err
}
func (f *fakeStore) GetDeadLetter(context.Context, string) (job.DeadLetter, error) {
	if len(f.letters) > 0 {
		return f.letters[0], f.err
	}
	return job.DeadLetter{}, f.err
}

func TestDeadLetterAPIPrivacyPaginationAndRedrive(t *testing.T) {
	id := uuid.NewString()
	now := time.Now().UTC()
	store := &fakeStore{created: true, value: job.Job{ID: uuid.NewString(), Queue: "default", Kind: "demo.echo", Status: job.StatusQueued, Payload: json.RawMessage(`{"secret":"payload"}`), ExecutionKey: "private-execution-key"}, letters: []job.DeadLetter{{JobID: id, Queue: "default", Kind: "demo.echo", FinalAttempt: 3, ErrorCode: "PERMANENT", DeadLetteredAt: now}}, hasMore: true}
	handler := New(store, 1024, time.Second, 10)
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/v1/dead-letters?limit=1", nil))
	if list.Code != 200 || !strings.Contains(list.Body.String(), "next_cursor") || strings.Contains(list.Body.String(), "secret") || strings.Contains(list.Body.String(), "private-execution-key") {
		t.Fatalf("unsafe list: %d %s", list.Code, list.Body.String())
	}
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest(http.MethodGet, "/v1/dead-letters?limit=11", nil))
	if bad.Code != 400 {
		t.Fatalf("invalid limit status=%d", bad.Code)
	}
	redrive := httptest.NewRecorder()
	handler.ServeHTTP(redrive, httptest.NewRequest(http.MethodPost, "/v1/dead-letters/"+id+"/redrive", nil))
	if redrive.Code != 201 || strings.Contains(redrive.Body.String(), "private-execution-key") || strings.Contains(redrive.Body.String(), "secret") {
		t.Fatalf("unsafe redrive: %d %s", redrive.Code, redrive.Body.String())
	}
}
func (f *fakeStore) RedriveDeadLetter(context.Context, string) (job.Job, bool, error) {
	return f.value, f.created, f.err
}
func (f *fakeStore) CreateSchedule(context.Context, job.ScheduleSubmission) (job.Schedule, bool, error) {
	return f.schedule, f.created, f.err
}
func (f *fakeStore) GetSchedule(context.Context, string) (job.Schedule, error) {
	return f.schedule, f.err
}
func (f *fakeStore) ListSchedules(context.Context, *time.Time, string, int) ([]job.Schedule, bool, error) {
	return f.schedules, f.hasMore, f.err
}
func (f *fakeStore) SetSchedulePaused(_ context.Context, _ string, paused bool) (job.Schedule, error) {
	out := f.schedule
	out.Paused = paused
	return out, f.err
}
func (f *fakeStore) DeleteSchedule(context.Context, string) error {
	return f.err
}

func TestScheduleAPICreateListPauseResumeDeleteAndPrivacy(t *testing.T) {
	id := uuid.NewString()
	now := time.Now().UTC()
	sched := job.Schedule{
		ID:        id,
		Name:      "hourly.echo",
		Queue:     "default",
		Kind:      "demo.echo",
		Payload:   json.RawMessage(`{"secret":"do-not-expose"}`),
		CronExpr:  "0 * * * *",
		Timezone:  "UTC",
		NextRunAt: now.Add(time.Hour),
		CreatedAt: now,
		UpdatedAt: now,
	}
	store := &fakeStore{
		created:   true,
		schedule:  sched,
		schedules: []job.Schedule{sched},
		hasMore:   true,
	}
	handler := New(store, 1024, time.Second, 10)

	createReq := httptest.NewRequest(http.MethodPost, "/v1/schedules", strings.NewReader(`{
		"name":"hourly.echo","kind":"demo.echo","payload":{"message":"hello"},"cron_expr":"0 * * * *","timezone":"UTC"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	handler.ServeHTTP(createResp, createReq)
	if createResp.Code != http.StatusCreated || strings.Contains(createResp.Body.String(), "do-not-expose") {
		t.Fatalf("create schedule status=%d body=%s", createResp.Code, createResp.Body.String())
	}

	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, httptest.NewRequest(http.MethodGet, "/v1/schedules?limit=1", nil))
	if listResp.Code != http.StatusOK || !strings.Contains(listResp.Body.String(), "next_cursor") || strings.Contains(listResp.Body.String(), "do-not-expose") {
		t.Fatalf("list schedules status=%d body=%s", listResp.Code, listResp.Body.String())
	}

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/v1/schedules/"+id, nil))
	if getResp.Code != http.StatusOK || strings.Contains(getResp.Body.String(), "do-not-expose") {
		t.Fatalf("get schedule status=%d body=%s", getResp.Code, getResp.Body.String())
	}

	pauseResp := httptest.NewRecorder()
	handler.ServeHTTP(pauseResp, httptest.NewRequest(http.MethodPost, "/v1/schedules/"+id+"/pause", nil))
	if pauseResp.Code != http.StatusOK || !strings.Contains(pauseResp.Body.String(), `"paused":true`) {
		t.Fatalf("pause schedule status=%d body=%s", pauseResp.Code, pauseResp.Body.String())
	}

	resumeResp := httptest.NewRecorder()
	handler.ServeHTTP(resumeResp, httptest.NewRequest(http.MethodPost, "/v1/schedules/"+id+"/resume", nil))
	if resumeResp.Code != http.StatusOK || !strings.Contains(resumeResp.Body.String(), `"paused":false`) {
		t.Fatalf("resume schedule status=%d body=%s", resumeResp.Code, resumeResp.Body.String())
	}

	delResp := httptest.NewRecorder()
	handler.ServeHTTP(delResp, httptest.NewRequest(http.MethodDelete, "/v1/schedules/"+id, nil))
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("delete schedule status=%d", delResp.Code)
	}
}

func TestSubmitStatusAndPrivacy(t *testing.T) {
	id := uuid.NewString()
	store := &fakeStore{created: true, value: job.Job{
		ID: id, Queue: "default", Kind: "demo.echo", Status: job.StatusQueued,
		Payload: json.RawMessage(`{"secret":"value"}`), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}}
	handler := New(store, 1024, time.Second)
	request := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{
        "kind":"demo.echo","payload":{"message":"hello"},"idempotency_key":"private-key"}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "private-key") {
		t.Fatal("response exposed sensitive submission data")
	}
}

func TestSubmitRejectsInvalidRequests(t *testing.T) {
	handler := New(&fakeStore{}, 32, time.Second)
	tests := []struct {
		name, contentType, contentEncoding, body string
		status                                   int
	}{
		{name: "missing content type", body: `{}`, status: http.StatusUnsupportedMediaType},
		{name: "unsupported charset", contentType: "application/json; charset=latin1", body: `{}`, status: http.StatusUnsupportedMediaType},
		{name: "content encoding", contentType: "application/json", contentEncoding: "gzip", body: `{}`, status: http.StatusUnsupportedMediaType},
		{name: "unknown field", contentType: "application/json", body: `{"kind":"demo.echo","payload":{},"extra":true}`, status: http.StatusBadRequest},
		{name: "multiple values", contentType: "application/json", body: `{} {}`, status: http.StatusBadRequest},
		{name: "both run_at and delay_seconds", contentType: "application/json", body: `{"kind":"demo.echo","payload":{},"run_at":"2026-11-05T10:00:00Z","delay_seconds":10}`, status: http.StatusBadRequest},
		{name: "oversized", contentType: "application/json", body: `{"kind":"demo.echo","payload":{"message":"` + strings.Repeat("x", 5000) + `"}}`, status: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(tt.body))
			if tt.contentType != "" {
				request.Header.Set("Content-Type", tt.contentType)
			}
			if tt.contentEncoding != "" {
				request.Header.Set("Content-Encoding", tt.contentEncoding)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, tt.status, response.Body.String())
			}
		})
	}
}

func TestStoreErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
	}{
		{err: postgres.ErrNotFound, status: http.StatusNotFound},
		{err: postgres.ErrStateConflict, status: http.StatusConflict},
		{err: postgres.ErrIdempotencyConflict, status: http.StatusConflict},
		{err: postgres.ErrUnavailable, status: http.StatusServiceUnavailable},
		{err: errors.New("private database detail"), status: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		store := &fakeStore{err: tt.err}
		handler := New(store, 1024, time.Second)
		request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.NewString(), nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tt.status {
			t.Fatalf("error %v mapped to %d, want %d", tt.err, response.Code, tt.status)
		}
		if strings.Contains(response.Body.String(), "private database detail") {
			t.Fatal("response leaked internal error")
		}
	}
}

func TestHealthAndReadiness(t *testing.T) {
	store := &fakeStore{err: errors.New("database down")}
	handler := New(store, 1024, time.Second)
	for path, status := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != status {
			t.Fatalf("%s returned %d, want %d", path, response.Code, status)
		}
	}
}

func TestRequestIDAndRoutingErrorsUseJSONEnvelope(t *testing.T) {
	handler := New(&fakeStore{}, 1024, time.Second)
	tests := []struct {
		name, method, path, requestID string
		status                        int
	}{
		{name: "invalid request id", method: http.MethodGet, path: "/healthz", requestID: "unsafe request id", status: http.StatusBadRequest},
		{name: "method not allowed", method: http.MethodPost, path: "/healthz", status: http.StatusMethodNotAllowed},
		{name: "route not found", method: http.MethodGet, path: "/missing", status: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.requestID != "" {
				request.Header.Set("X-Request-ID", tt.requestID)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tt.status || response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("status=%d content-type=%q body=%s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
			}
			var envelope errorEnvelope
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || envelope.Error.RequestID == "" {
				t.Fatalf("invalid error envelope: err=%v body=%s", err, response.Body.String())
			}
		})
	}
}
