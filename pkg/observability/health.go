package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// HealthChecker is implemented by components that can report their health status
type HealthChecker interface {
	Check(ctx context.Context) error
}

// healthResponse is the JSON response format for health endpoints
type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// livenessHandler returns 200 OK if the process is alive
func livenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok"})
	}
}

// readinessHandler checks all registered health checkers and returns aggregate status
func readinessHandler(checkers map[string]HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Use a 2s timeout per check to avoid blocking the endpoint
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		checks := make(map[string]string, len(checkers))
		allOK := true

		for name, checker := range checkers {
			if err := checker.Check(ctx); err != nil {
				checks[name] = err.Error()
				allOK = false
			} else {
				checks[name] = "ok"
			}
		}

		resp := healthResponse{Checks: checks}
		if allOK {
			resp.Status = "ok"
			w.WriteHeader(http.StatusOK)
		} else {
			resp.Status = "error"
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		_ = json.NewEncoder(w).Encode(resp)
	}
}
