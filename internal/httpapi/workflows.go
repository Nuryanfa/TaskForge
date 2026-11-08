package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/google/uuid"
)

type createWorkflowRequest struct {
	Label          *string                   `json:"label"`
	IdempotencyKey *string                   `json:"idempotency_key"`
	Steps          []job.WorkflowStepRequest `json:"steps"`
}

type compensationResponse struct {
	Kind   string                 `json:"kind"`
	Status job.CompensationStatus `json:"status"`
	JobID  *string                `json:"job_id,omitempty"`
}

type workflowStepResponse struct {
	Name         string                `json:"name"`
	Kind         string                `json:"kind"`
	Queue        string                `json:"queue"`
	DependsOn    []string              `json:"depends_on"`
	Status       job.StepStatus        `json:"status"`
	JobID        *string               `json:"job_id,omitempty"`
	Compensation *compensationResponse `json:"compensation"`
}

// workflowResponse never carries payloads, the idempotency key, or execution
// keys. Steps are omitted from list items.
type workflowResponse struct {
	ID          string                 `json:"id"`
	Label       *string                `json:"label,omitempty"`
	Status      job.WorkflowStatus     `json:"status"`
	AbortReason *job.AbortReason       `json:"abort_reason,omitempty"`
	FailedStep  *string                `json:"failed_step,omitempty"`
	ErrorCode   *string                `json:"error_code,omitempty"`
	StepCount   int                    `json:"step_count"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Steps       []workflowStepResponse `json:"steps,omitempty"`
}

type workflowPage struct {
	Items      []workflowResponse `json:"items"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

func responseForWorkflow(value job.Workflow) workflowResponse {
	response := workflowResponse{
		ID: value.ID, Label: value.Label, Status: value.Status, AbortReason: value.AbortReason,
		FailedStep: value.FailedStep, ErrorCode: value.ErrorCode, StepCount: value.StepCount,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CompletedAt: value.CompletedAt,
	}
	for _, item := range value.Steps {
		step := workflowStepResponse{Name: item.Name, Kind: item.Kind, Queue: item.Queue,
			DependsOn: item.DependsOn, Status: item.Status, JobID: item.JobID}
		if step.DependsOn == nil {
			step.DependsOn = []string{}
		}
		if item.CompensationKind != nil {
			step.Compensation = &compensationResponse{Kind: *item.CompensationKind,
				Status: item.CompensationStatus, JobID: item.CompensationJobID}
		}
		response.Steps = append(response.Steps, step)
	}
	return response
}

func (a *API) createWorkflow(w http.ResponseWriter, r *http.Request) {
	if err := validateJSONHeaders(r); err != nil {
		writeError(w, r, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", err.Error())
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.workflowMaxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request createWorkflowRequest
	if err := decoder.Decode(&request); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body exceeds the configured limit")
			return
		}
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must be one valid JSON object with known fields")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "request body must contain exactly one JSON object")
		return
	}
	submission, err := job.NewWorkflowSubmission(request.Label, request.IdempotencyKey, request.Steps,
		job.WorkflowLimits{MaxSteps: a.workflowMaxSteps, MaxPayloadBytes: a.payloadMaxBytes})
	if err != nil {
		status, code := http.StatusBadRequest, "VALIDATION_FAILED"
		if strings.Contains(err.Error(), "configured limit") {
			status, code = http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE"
		}
		writeError(w, r, status, code, err.Error())
		return
	}
	created, isNew, err := a.store.CreateWorkflow(r.Context(), submission)
	if err != nil {
		a.writeWorkflowStoreError(w, r, err)
		return
	}
	status := http.StatusOK
	if isNew {
		status = http.StatusCreated
	}
	writeJSON(w, status, responseForWorkflow(created))
}

func (a *API) listWorkflows(w http.ResponseWriter, r *http.Request) {
	limit, before, beforeID, ok := parsePaginationQueryUpTo(w, r, a.workflowPageSize)
	if !ok {
		return
	}
	items, more, err := a.store.ListWorkflows(r.Context(), before, beforeID, limit)
	if err != nil {
		a.writeWorkflowStoreError(w, r, err)
		return
	}
	response := workflowPage{Items: make([]workflowResponse, 0, len(items))}
	for _, item := range items {
		item.Steps = nil
		response.Items = append(response.Items, responseForWorkflow(item))
	}
	if more && len(items) > 0 {
		last := items[len(items)-1]
		encoded, _ := json.Marshal(pageCursor{At: last.CreatedAt, ID: last.ID})
		response.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) workflowID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID must be a valid UUID")
		return "", false
	}
	return id, true
}

func (a *API) getWorkflow(w http.ResponseWriter, r *http.Request) {
	id, ok := a.workflowID(w, r)
	if !ok {
		return
	}
	found, err := a.store.GetWorkflow(r.Context(), id)
	if err != nil {
		a.writeWorkflowStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseForWorkflow(found))
}

func (a *API) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	id, ok := a.workflowID(w, r)
	if !ok {
		return
	}
	canceled, err := a.store.CancelWorkflow(r.Context(), id)
	if err != nil {
		a.writeWorkflowStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseForWorkflow(canceled))
}

func (a *API) writeWorkflowStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "workflow was not found")
	case errors.Is(err, postgres.ErrStateConflict):
		writeError(w, r, http.StatusConflict, "WORKFLOW_STATE_CONFLICT", "workflow state does not allow this operation")
	default:
		a.writeStoreError(w, r, err)
	}
}
