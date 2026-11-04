package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/google/uuid"
)

type Store interface {
	CreateJob(context.Context, job.Submission) (job.Job, bool, error)
	GetJob(context.Context, string) (job.Job, error)
	CancelQueuedJob(context.Context, string) (job.Job, error)
	Ping(context.Context) error
	ListDeadLetters(context.Context, *time.Time, string, int) ([]job.DeadLetter, bool, error)
	GetDeadLetter(context.Context, string) (job.DeadLetter, error)
	RedriveDeadLetter(context.Context, string) (job.Job, bool, error)
}

type API struct {
	store              Store
	payloadMaxBytes    int64
	readinessTimeout   time.Duration
	deadLetterPageSize int
}

type errorEnvelope struct {
	Error publicError `json:"error"`
}

type publicError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type jobResponse struct {
	ID            string     `json:"id"`
	Queue         string     `json:"queue"`
	Kind          string     `json:"kind"`
	Status        job.Status `json:"status"`
	Priority      int16      `json:"priority"`
	AttemptCount  int        `json:"attempt_count"`
	AvailableAt   time.Time  `json:"available_at"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	LastErrorCode *string    `json:"last_error_code,omitempty"`
	HasResult     bool       `json:"has_result"`
	ResultBytes   int        `json:"result_bytes,omitempty"`
	MaxAttempts   int        `json:"max_attempts"`
	DeadLettered  bool       `json:"dead_lettered"`
}

type submitRequest struct {
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func New(store Store, payloadMaxBytes int64, readinessTimeout time.Duration, pageSizes ...int) http.Handler {
	pageSize := 50
	if len(pageSizes) > 0 {
		pageSize = pageSizes[0]
	}
	api := &API{store: store, payloadMaxBytes: payloadMaxBytes, readinessTimeout: readinessTimeout, deadLetterPageSize: pageSize}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /readyz", api.ready)
	mux.HandleFunc("POST /v1/jobs", api.submit)
	mux.HandleFunc("GET /v1/jobs/{id}", api.get)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", api.cancel)
	mux.HandleFunc("GET /v1/dead-letters", api.listDeadLetters)
	mux.HandleFunc("GET /v1/dead-letters/{id}", api.getDeadLetter)
	mux.HandleFunc("POST /v1/dead-letters/{id}/redrive", api.redriveDeadLetter)
	mux.HandleFunc("/healthz", api.methodNotAllowed)
	mux.HandleFunc("/readyz", api.methodNotAllowed)
	mux.HandleFunc("/v1/jobs", api.methodNotAllowed)
	mux.HandleFunc("/v1/jobs/{id}", api.methodNotAllowed)
	mux.HandleFunc("/v1/jobs/{id}/cancel", api.methodNotAllowed)
	mux.HandleFunc("/v1/dead-letters", api.methodNotAllowed)
	mux.HandleFunc("/v1/dead-letters/{id}", api.methodNotAllowed)
	mux.HandleFunc("/v1/dead-letters/{id}/redrive", api.methodNotAllowed)
	mux.HandleFunc("/", api.notFound)
	return api.withRequestID(mux)
}

func (a *API) methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "HTTP method is not allowed for this resource")
}

func (a *API) notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusNotFound, "ROUTE_NOT_FOUND", "route was not found")
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.readinessTimeout)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "database is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	if err := validateJSONHeaders(r); err != nil {
		writeError(w, r, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", err.Error())
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.payloadMaxBytes+4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request submitRequest
	if err := decoder.Decode(&request); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body exceeds the configured limit")
			return
		}
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must be one valid JSON object")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must contain exactly one JSON object")
		return
	}

	submission, err := job.NewSubmission(request.Queue, request.Kind, request.Payload, request.Priority, request.IdempotencyKey, a.payloadMaxBytes)
	if err != nil {
		status := http.StatusBadRequest
		code := "VALIDATION_FAILED"
		if strings.Contains(err.Error(), "configured limit") {
			status = http.StatusRequestEntityTooLarge
			code = "PAYLOAD_TOO_LARGE"
		}
		writeError(w, r, status, code, err.Error())
		return
	}
	createdJob, created, err := a.store.CreateJob(r.Context(), submission)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, responseForJob(createdJob))
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JOB_ID", "job ID must be a valid UUID")
		return
	}
	found, err := a.store.GetJob(r.Context(), id)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseForJob(found))
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JOB_ID", "job ID must be a valid UUID")
		return
	}
	canceled, err := a.store.CancelQueuedJob(r.Context(), id)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseForJob(canceled))
}

type deadLetterResponse struct {
	JobID          string     `json:"job_id"`
	Queue          string     `json:"queue"`
	Kind           string     `json:"kind"`
	FinalAttempt   int        `json:"final_attempt"`
	ErrorCode      string     `json:"error_code"`
	DeadLetteredAt time.Time  `json:"dead_lettered_at"`
	RedrivenJobID  *string    `json:"redriven_job_id,omitempty"`
	RedrivenAt     *time.Time `json:"redriven_at,omitempty"`
}

type deadLetterPage struct {
	Items      []deadLetterResponse `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

type pageCursor struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

func (a *API) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	for key := range r.URL.Query() {
		if key != "limit" && key != "cursor" {
			writeError(w, r, http.StatusBadRequest, "INVALID_QUERY", "only limit and cursor are supported")
			return
		}
		if len(r.URL.Query()[key]) != 1 {
			writeError(w, r, http.StatusBadRequest, "INVALID_QUERY", "query parameters must not be repeated")
			return
		}
	}
	limit := a.deadLetterPageSize
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > a.deadLetterPageSize {
			writeError(w, r, http.StatusBadRequest, "INVALID_QUERY", "limit is outside the allowed range")
			return
		}
		limit = value
	}
	var before *time.Time
	beforeID := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		var cursor pageCursor
		if err != nil || json.Unmarshal(decoded, &cursor) != nil || cursor.At.IsZero() {
			writeError(w, r, http.StatusBadRequest, "INVALID_CURSOR", "cursor is invalid")
			return
		}
		if _, err := uuid.Parse(cursor.ID); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_CURSOR", "cursor is invalid")
			return
		}
		before = &cursor.At
		beforeID = cursor.ID
	}
	items, more, err := a.store.ListDeadLetters(r.Context(), before, beforeID, limit)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	response := deadLetterPage{Items: make([]deadLetterResponse, 0, len(items))}
	for _, item := range items {
		response.Items = append(response.Items, deadLetterForResponse(item))
	}
	if more && len(items) > 0 {
		last := items[len(items)-1]
		encoded, _ := json.Marshal(pageCursor{At: last.DeadLetteredAt, ID: last.JobID})
		response.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) getDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JOB_ID", "job ID must be a valid UUID")
		return
	}
	item, err := a.store.GetDeadLetter(r.Context(), id)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, deadLetterForResponse(item))
}

func (a *API) redriveDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JOB_ID", "job ID must be a valid UUID")
		return
	}
	created, isNew, err := a.store.RedriveDeadLetter(r.Context(), id)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	status := http.StatusOK
	if isNew {
		status = http.StatusCreated
	}
	writeJSON(w, status, responseForJob(created))
}

func deadLetterForResponse(v job.DeadLetter) deadLetterResponse {
	return deadLetterResponse{
		JobID: v.JobID, Queue: v.Queue, Kind: v.Kind, FinalAttempt: v.FinalAttempt,
		ErrorCode: v.ErrorCode, DeadLetteredAt: v.DeadLetteredAt,
		RedrivenJobID: v.RedrivenJobID, RedrivenAt: v.RedrivenAt,
	}
}

func (a *API) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "JOB_NOT_FOUND", "job was not found")
	case errors.Is(err, postgres.ErrStateConflict):
		writeError(w, r, http.StatusConflict, "JOB_STATE_CONFLICT", "job state does not allow this operation")
	case errors.Is(err, postgres.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key was already used for a different request")
	case errors.Is(err, postgres.ErrUnavailable):
		writeError(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "database is unavailable")
	default:
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "an internal error occurred")
	}
}

func (a *API) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		} else if !requestIDPattern.MatchString(requestID) {
			requestID = uuid.NewString()
			w.Header().Set("X-Request-ID", requestID)
			ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
			writeError(w, r.WithContext(ctx), http.StatusBadRequest, "INVALID_REQUEST_ID", "X-Request-ID must be a bounded safe identifier")
			return
		}
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type requestIDContextKey struct{}

func validateJSONHeaders(r *http.Request) error {
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return errors.New("Content-Encoding is not supported")
	}
	mediaType, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("Content-Type must be application/json")
	}
	for key, value := range parameters {
		if !strings.EqualFold(key, "charset") || !strings.EqualFold(value, "utf-8") {
			return errors.New("only the UTF-8 application/json media type is supported")
		}
	}
	return nil
}

func responseForJob(value job.Job) jobResponse {
	return jobResponse{
		ID: value.ID, Queue: value.Queue, Kind: value.Kind, Status: value.Status,
		Priority: value.Priority, AttemptCount: value.AttemptCount,
		AvailableAt: value.AvailableAt, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		StartedAt: value.StartedAt, CompletedAt: value.CompletedAt,
		LastErrorCode: value.LastErrorCode, HasResult: len(value.Result) > 0, ResultBytes: len(value.Result),
		MaxAttempts: value.MaxAttempts, DeadLettered: value.DeadLetteredAt != nil,
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	requestID, _ := r.Context().Value(requestIDContextKey{}).(string)
	writeJSON(w, status, errorEnvelope{Error: publicError{Code: code, Message: message, RequestID: requestID}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
