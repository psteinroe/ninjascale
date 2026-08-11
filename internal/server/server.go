package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/psteinroe/ninjascale/internal/adapter"
)

const healthCacheTTL = 10 * time.Second

var (
	scaleEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ninjascale_scale_events_total",
		Help: "Total number of scale events",
	}, []string{"service", "direction"})

	currentScale = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_current_scale",
		Help: "Current instance count per service",
	}, []string{"service"})

	desiredScale = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_desired_scale",
		Help: "Desired instance count per service (before cooldown)",
	}, []string{"service"})

	reconcileLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ninjascale_reconcile_duration_seconds",
		Help:    "Time spent reconciling each service",
		Buckets: prometheus.DefBuckets,
	}, []string{"service"})

	metricValue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_metric_value",
		Help: "Latest metric values being tracked",
	}, []string{"service", "metric"})

	metricAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_metric_age_seconds",
		Help: "Age of the latest metric sample in seconds",
	}, []string{"service", "metric"})

	windowCompleteBuckets = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_window_complete_buckets",
		Help: "Number of expected completed buckets containing samples",
	}, []string{"service", "metric", "direction"})

	windowEvaluationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ninjascale_window_evaluations_total",
		Help: "Window evaluations by bounded result",
	}, []string{"service", "metric", "direction", "result"})

	cooldownActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_cooldown_active",
		Help: "Whether cooldown is active (1) or not (0)",
	}, []string{"service", "direction"})
)

// Server provides HTTP endpoints for health and metrics.
type Server struct {
	adapter adapter.Adapter
	server  *http.Server

	healthMu        sync.RWMutex
	healthCacheTime time.Time
	healthCacheErr  error
}

// New creates a new server.
func New(adp adapter.Adapter, address string) *Server {
	mux := http.NewServeMux()

	s := &Server{
		adapter: adp,
		server: &http.Server{
			Addr:    address,
			Handler: mux,
		},
	}

	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.Handler())

	return s
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "ok")
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	err := s.getCachedHealth(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "adapter unhealthy: %v", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "ok")
}

func (s *Server) getCachedHealth(ctx context.Context) error {
	s.healthMu.RLock()
	if time.Since(s.healthCacheTime) < healthCacheTTL {
		err := s.healthCacheErr
		s.healthMu.RUnlock()
		return err
	}
	s.healthMu.RUnlock()

	// Cache expired, refresh
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := s.adapter.HealthCheck(ctx)

	s.healthMu.Lock()
	s.healthCacheTime = time.Now()
	s.healthCacheErr = err
	s.healthMu.Unlock()

	return err
}

// RecordScaleEvent records a scale event metric.
func RecordScaleEvent(service, direction string) {
	scaleEventsTotal.WithLabelValues(service, direction).Inc()
}

// RecordCurrentScale records the current scale metric.
func RecordCurrentScale(service string, count int) {
	currentScale.WithLabelValues(service).Set(float64(count))
}

// RecordDesiredScale records the desired scale metric.
func RecordDesiredScale(service string, count int) {
	desiredScale.WithLabelValues(service).Set(float64(count))
}

// RecordReconcileLatency records reconcile latency.
func RecordReconcileLatency(service string, duration time.Duration) {
	reconcileLatency.WithLabelValues(service).Observe(duration.Seconds())
}

// RecordMetricValue records a metric value.
func RecordMetricValue(service, metric string, value float64) {
	metricValue.WithLabelValues(service, metric).Set(value)
}

// RecordMetricAge records the age of a metric's latest sample.
func RecordMetricAge(service, metric string, age time.Duration) {
	seconds := age.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	metricAge.WithLabelValues(service, metric).Set(seconds)
}

// RecordWindowCompleteBuckets records populated buckets in the requested window.
func RecordWindowCompleteBuckets(service, metric, direction string, count int) {
	windowCompleteBuckets.WithLabelValues(service, metric, direction).Set(float64(count))
}

// RecordWindowEvaluation records a bounded window result.
func RecordWindowEvaluation(service, metric, direction, result string) {
	windowEvaluationsTotal.WithLabelValues(service, metric, direction, result).Inc()
}

// RecordCooldownActive records cooldown status.
func RecordCooldownActive(service, direction string, active bool) {
	val := 0.0
	if active {
		val = 1.0
	}
	cooldownActive.WithLabelValues(service, direction).Set(val)
}
