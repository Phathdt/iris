package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics defines the interface for pipeline observability metrics.
// Pipeline code depends on this interface, not Prometheus directly.
type Metrics interface {
	// IncEventsProcessed increments the events processed counter
	IncEventsProcessed(table, op, status string)
	// SetReplicationLag sets the current replication lag in seconds
	SetReplicationLag(seconds float64)
	// ObserveTransformDuration records a transform duration observation
	ObserveTransformDuration(seconds float64)
	// ObserveSinkWriteDuration records a sink write duration observation
	ObserveSinkWriteDuration(seconds float64)
	// IncPipelineErrors increments the pipeline error counter
	IncPipelineErrors(component, errorType string)
}

// prometheusMetrics implements Metrics using Prometheus client
type prometheusMetrics struct {
	registry          *prometheus.Registry
	eventsProcessed   *prometheus.CounterVec
	replicationLag    prometheus.Gauge
	transformDuration prometheus.Histogram
	sinkWriteDuration prometheus.Histogram
	pipelineErrors    *prometheus.CounterVec
}

// NewPrometheusMetrics creates a new Prometheus-backed Metrics implementation.
// Uses a custom registry to avoid global state pollution in tests.
func NewPrometheusMetrics() (*prometheusMetrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()

	m := &prometheusMetrics{
		registry: reg,
		eventsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "iris_events_processed_total",
				Help: "Total number of CDC events processed",
			},
			[]string{"table", "op", "status"},
		),
		replicationLag: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "iris_replication_lag_seconds",
				Help: "Estimated replication lag in seconds (event TS vs now)",
			},
		),
		transformDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "iris_transform_duration_seconds",
				Help:    "Duration of WASM transform processing (first attempt only)",
				Buckets: []float64{.0005, .001, .005, .01, .05, .1, .5, 1},
			},
		),
		sinkWriteDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "iris_sink_write_duration_seconds",
				Help:    "Duration of sink write operations",
				Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
			},
		),
		pipelineErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "iris_pipeline_errors_total",
				Help: "Total number of pipeline errors by component and type",
			},
			[]string{"component", "error_type"},
		),
	}

	reg.MustRegister(
		m.eventsProcessed,
		m.replicationLag,
		m.transformDuration,
		m.sinkWriteDuration,
		m.pipelineErrors,
	)

	return m, reg
}

// Registry returns the custom Prometheus registry for use with promhttp
func (m *prometheusMetrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *prometheusMetrics) IncEventsProcessed(table, op, status string) {
	m.eventsProcessed.WithLabelValues(table, op, status).Inc()
}

func (m *prometheusMetrics) SetReplicationLag(seconds float64) {
	m.replicationLag.Set(seconds)
}

func (m *prometheusMetrics) ObserveTransformDuration(seconds float64) {
	m.transformDuration.Observe(seconds)
}

func (m *prometheusMetrics) ObserveSinkWriteDuration(seconds float64) {
	m.sinkWriteDuration.Observe(seconds)
}

func (m *prometheusMetrics) IncPipelineErrors(component, errorType string) {
	m.pipelineErrors.WithLabelValues(component, errorType).Inc()
}

// noopMetrics implements Metrics with zero-cost no-op operations
type noopMetrics struct{}

// NewNoopMetrics creates a no-op Metrics implementation for when metrics are disabled
func NewNoopMetrics() Metrics {
	return &noopMetrics{}
}

func (m *noopMetrics) IncEventsProcessed(_, _, _ string)  {}
func (m *noopMetrics) SetReplicationLag(_ float64)        {}
func (m *noopMetrics) ObserveTransformDuration(_ float64) {}
func (m *noopMetrics) ObserveSinkWriteDuration(_ float64) {}
func (m *noopMetrics) IncPipelineErrors(_, _ string)      {}
