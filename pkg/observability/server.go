package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"iris/pkg/config"
	"iris/pkg/logger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server runs the observability HTTP server with /metrics, /healthz, /readyz
type Server struct {
	httpServer *http.Server
	logger     logger.Logger
}

// NewServer creates a new observability HTTP server
func NewServer(
	cfg config.MetricsConfig,
	registry *prometheus.Registry,
	checkers map[string]HealthChecker,
	log logger.Logger,
) *Server {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	// Kubernetes-style health probes
	mux.HandleFunc("/healthz", livenessHandler())
	mux.HandleFunc("/readyz", readinessHandler(checkers))

	addr := fmt.Sprintf("%s:%d", cfg.Bind, cfg.Port)

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		logger: log,
	}
}

// Start launches the HTTP server in a background goroutine.
// Returns immediately; call Shutdown to stop.
func (s *Server) Start() {
	s.logger.Info("starting observability server", "addr", s.httpServer.Addr)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("observability server error", "error", err)
		}
	}()
}

// Shutdown gracefully stops the HTTP server with a 5s timeout
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down observability server")
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
