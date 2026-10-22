package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/psteinroe/ninjascale/internal/adapter"
)

const healthCacheTTL = 10 * time.Second

var (
	scaleEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ninjascale_scale_events_total",
		Help: "Total number of scale events",
	}, []string{"service", "direction"})

	currentScale = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_current_scale",
		Help: "Current target instance count per service (ECS desired count)",
	}, []string{"service"})

	desiredScale = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_desired_scale",
		Help: "Desired instance count per service (before cooldown)",
	}, []string{"service"})

	// Reconciling is in-memory unless it scales, so the buckets separate
	// evaluation, an adapter call, a slow call, and one that nears the interval.
	reconcileLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ninjascale_reconcile_duration_seconds",
		Help:    "Time spent reconciling each service",
		Buckets: []float64{0.01, 0.1, 1, 10},
	}, []string{"service"})

	metricAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_metric_age_seconds",
		Help: "Age of the latest metric sample in seconds",
	}, []string{"service", "metric"})

	windowEvaluationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ninjascale_window_evaluations_total",
		Help: "Window evaluations by bounded result",
	}, []string{"service", "direction", "result"})

	cooldownActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_cooldown_active",
		Help: "Whether cooldown is active (1) or not (0)",
	}, []string{"service", "direction"})
)

// Detailed metrics have one series per window or metric binding, and per
// window result. They are only exported when detailed metrics are enabled.
var (
	metricValue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_metric_value",
		Help: "Latest metric values being tracked",
	}, []string{"service", "metric"})

	windowCompleteBuckets = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ninjascale_window_complete_buckets",
		Help: "Number of expected completed buckets containing samples",
	}, []string{"service", "metric", "direction"})

	// Replaces windowEvaluationsTotal, so both share a name.
	windowMetricEvaluationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ninjascale_window_evaluations_total",
		Help: "Window evaluations by bounded result",
	}, []string{"service", "metric", "direction", "result"})
)

// Options configures the server.
type Options struct {
	Address string
	// DetailedMetrics exports per-window and per-metric diagnostics.
	DetailedMetrics bool
}

// Server provides HTTP endpoints for health and metrics.
type Server struct {
	adapter adapter.Adapter
	server  *http.Server

	healthMu        sync.RWMutex
	healthCacheTime time.Time
	healthCacheErr  error
}

// New creates a new server.
func New(adp adapter.Adapter, opts Options) *Server {
	mux := http.NewServeMux()

	s := &Server{
		adapter: adp,
		server: &http.Server{
			Addr:    opts.Address,
			Handler: mux,
		},
	}

	registry := newRegistry(opts.DetailedMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))

	return s
}

// newRegistry matches the default registry, plus the NinjaScale metrics.
func newRegistry(detailed bool) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		scaleEventsTotal,
		currentScale,
		desiredScale,
		reconcileLatency,
		metricAge,
		cooldownActive,
	)
	if detailed {
		registry.MustRegister(metricValue, windowCompleteBuckets, windowMetricEvaluationsTotal)
	} else {
		registry.MustRegister(windowEvaluationsTotal)
	}
	return registry
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
	windowEvaluationsTotal.WithLabelValues(service, direction, result).Inc()
	windowMetricEvaluationsTotal.WithLabelValues(service, metric, direction, result).Inc()
}

// RecordCooldownActive records cooldown status.
func RecordCooldownActive(service, direction string, active bool) {
	val := 0.0
	if active {
		val = 1.0
	}
	cooldownActive.WithLabelValues(service, direction).Set(val)
}
