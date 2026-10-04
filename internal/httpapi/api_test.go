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
	created bool
	err     error
	value   job.Job
}

func (f *fakeStore) CreateJob(context.Context, job.Submission) (job.Job, bool, error) {
	return f.value, f.created, f.err
}
func (f *fakeStore) GetJob(context.Context, string) (job.Job, error) { return f.value, f.err }
func (f *fakeStore) CancelQueuedJob(context.Context, string) (job.Job, error) {
	return f.value, f.err
}
func (f *fakeStore) Ping(context.Context) error { return f.err }

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
