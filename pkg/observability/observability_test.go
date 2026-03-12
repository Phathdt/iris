package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestNewPrometheusMetrics verifies all metrics register without panic
func TestNewPrometheusMetrics(t *testing.T) {
	m, reg := NewPrometheusMetrics()
	if m == nil {
		t.Fatal("NewPrometheusMetrics returned nil")
	}
	if reg == nil {
		t.Fatal("registry is nil")
	}

	// Verify metrics are registered by gathering
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}
	// Should have at least the 5 registered metrics (some may show 0 initially)
	_ = families
}

// TestPrometheusMetrics_Operations verifies metric operations don't panic
func TestPrometheusMetrics_Operations(t *testing.T) {
	m, reg := NewPrometheusMetrics()

	m.IncEventsProcessed("users", "create", "success")
	m.IncEventsProcessed("orders", "update", "error")
	m.SetReplicationLag(0.5)
	m.ObserveTransformDuration(0.001)
	m.ObserveSinkWriteDuration(0.01)
	m.IncPipelineErrors("transform", "retry_exhausted")
	m.IncPipelineErrors("sink", "dlq")

	// Verify metrics are populated
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}

	expected := []string{
		"iris_events_processed_total",
		"iris_replication_lag_seconds",
		"iris_transform_duration_seconds",
		"iris_sink_write_duration_seconds",
		"iris_pipeline_errors_total",
	}
	for _, name := range expected {
		if !names[name] {
			t.Errorf("metric %q not found in gathered families", name)
		}
	}
}

// TestNoopMetrics verifies noop implementation satisfies interface
func TestNoopMetrics(t *testing.T) {
	var m Metrics = NewNoopMetrics()

	// All calls should be no-ops (no panic)
	m.IncEventsProcessed("t", "o", "s")
	m.SetReplicationLag(1.0)
	m.ObserveTransformDuration(0.1)
	m.ObserveSinkWriteDuration(0.2)
	m.IncPipelineErrors("c", "e")
}

// TestRegistry returns custom registry, not global
func TestRegistry(t *testing.T) {
	m, reg := NewPrometheusMetrics()
	if m.Registry() != reg {
		t.Error("Registry() should return the same registry passed at creation")
	}
	if reg == prometheus.DefaultRegisterer {
		t.Error("should use custom registry, not default")
	}
}

// TestLivenessHandler always returns 200 OK
func TestLivenessHandler(t *testing.T) {
	handler := livenessHandler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("liveness: got %d, want %d", w.Code, http.StatusOK)
	}

	var resp healthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want %q", resp.Status, "ok")
	}
}

// TestReadinessHandler_AllHealthy returns 200 when all checkers pass
func TestReadinessHandler_AllHealthy(t *testing.T) {
	checkers := map[string]HealthChecker{
		"source": &mockChecker{nil},
		"sink":   &mockChecker{nil},
	}

	handler := readinessHandler(checkers)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("readiness: got %d, want %d", w.Code, http.StatusOK)
	}

	var resp healthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != "ok" {
		t.Errorf("status = %q, want %q", resp.Status, "ok")
	}
}

// TestReadinessHandler_Unhealthy returns 503 when a checker fails
func TestReadinessHandler_Unhealthy(t *testing.T) {
	checkers := map[string]HealthChecker{
		"source": &mockChecker{nil},
		"sink":   &mockChecker{fmt.Errorf("connection refused")},
	}

	handler := readinessHandler(checkers)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness: got %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	var resp healthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != "error" {
		t.Errorf("status = %q, want %q", resp.Status, "error")
	}
	if resp.Checks["sink"] != "connection refused" {
		t.Errorf("sink check = %q, want %q", resp.Checks["sink"], "connection refused")
	}
}

// TestReadinessHandler_NoCheckers returns 200 with empty checkers
func TestReadinessHandler_NoCheckers(t *testing.T) {
	handler := readinessHandler(map[string]HealthChecker{})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("readiness: got %d, want %d", w.Code, http.StatusOK)
	}
}

// mockChecker is a test HealthChecker that returns a fixed error
type mockChecker struct {
	err error
}

func (c *mockChecker) Check(_ context.Context) error {
	return c.err
}
