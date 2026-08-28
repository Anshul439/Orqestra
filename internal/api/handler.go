package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Anshul439/Orqestra/internal/service"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)


type Handler struct {
	jobs      *service.JobService
	workflows *service.WorkflowService
}

func NewHandler(jobs *service.JobService, workflows *service.WorkflowService) *Handler {
	return &Handler{jobs: jobs, workflows: workflows}
}

func NewRouter(h *Handler, pool *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/jobs", h.submitJob)
	mux.HandleFunc("GET /api/v1/jobs", h.listJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.getJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", h.cancelJob)

	mux.HandleFunc("GET /api/v1/workflows", h.listWorkflows)
	mux.HandleFunc("POST /api/v1/workflows/{name}/trigger", h.triggerWorkflow)
	mux.HandleFunc("GET /api/v1/workflows/runs", h.listWorkflowRuns)
	mux.HandleFunc("GET /api/v1/workflows/runs/{id}", h.getWorkflowStatus)
	mux.HandleFunc("POST /api/v1/workflows/runs/{id}/cancel", h.cancelWorkflowRun)

	return authMiddleware(pool, mux)
}

func (h *Handler) submitJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type       string `json:"type"`
		Payload    string `json:"payload"`
		MaxRetries int    `json:"max_retries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	jobID, err := h.jobs.SubmitJob(r.Context(), req.MaxRetries, req.Type, req.Payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]int{"job_id": jobID})
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")

	jobs, err := h.jobs.ListJobs(r.Context(), status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if jobs == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	if err := h.jobs.CancelJob(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "status": "cancelled"})
}

func (h *Handler) listWorkflows(w http.ResponseWriter, r *http.Request) {
	wfs := h.workflows.ListWorkflows()
	type summary struct {
		Name      string `json:"name"`
		Schedule  string `json:"schedule,omitempty"`
		StepCount int    `json:"step_count"`
	}
	result := make([]summary, len(wfs))
	for i, wf := range wfs {
		result[i] = summary{Name: wf.Name, Schedule: wf.Schedule, StepCount: len(wf.Steps)}
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) listWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.workflows.ListWorkflowRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if runs == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) cancelWorkflowRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}

	if err := h.workflows.CancelWorkflowRun(r.Context(), id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow run not found or not running")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"run_id": id, "status": "cancelled"})
}

func (h *Handler) triggerWorkflow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	runID, err := h.workflows.TriggerWorkflow(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]int{"run_id": runID})
}

func (h *Handler) getWorkflowStatus(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}

	run, err := h.workflows.GetWorkflowStatus(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "workflow run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, run)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Default().Error("writeJSON: encode failed", slog.String("error", err.Error()))
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func pathInt(r *http.Request, key string) (int, error) {
	return strconv.Atoi(r.PathValue(key))
}
