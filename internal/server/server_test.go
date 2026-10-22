package server

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/psteinroe/ninjascale/internal/adapter"
)

func TestWindowAndMetricAgeObservability(t *testing.T) {
	service, metric := "observability-test", "qd"
	RecordMetricAge(service, metric, 3*time.Second)
	RecordWindowCompleteBuckets(service, metric, "upscale", 2)
	counter := windowEvaluationsTotal.WithLabelValues(service, "upscale", "stale")
	detailedCounter := windowMetricEvaluationsTotal.WithLabelValues(service, metric, "upscale", "stale")
	before, detailedBefore := promtest.ToFloat64(counter), promtest.ToFloat64(detailedCounter)
	RecordWindowEvaluation(service, metric, "upscale", "stale")

	if got := promtest.ToFloat64(metricAge.WithLabelValues(service, metric)); got != 3 {
		t.Fatalf("metric age=%v", got)
	}
	if got := promtest.ToFloat64(windowCompleteBuckets.WithLabelValues(service, metric, "upscale")); got != 2 {
		t.Fatalf("complete buckets=%v", got)
	}
	if got := promtest.ToFloat64(counter); got != before+1 {
		t.Fatalf("evaluation counter=%v want=%v", got, before+1)
	}
	if got := promtest.ToFloat64(detailedCounter); got != detailedBefore+1 {
		t.Fatalf("detailed evaluation counter=%v want=%v", got, detailedBefore+1)
	}
}

func TestDefaultMetricsHaveOneSeriesPerServiceDirectionAndResult(t *testing.T) {
	service := "default-metrics-test"
	recordWindows(service)

	families := gather(t, newRegistry(false))
	if got := seriesFor(families, "ninjascale_window_evaluations_total", service); got != 2 {
		t.Fatalf("window evaluation series=%d want=2 (one per direction and result)", got)
	}
	for _, m := range families["ninjascale_window_evaluations_total"].GetMetric() {
		if hasLabel(m, "metric") {
			t.Fatalf("window evaluations have a metric label: %v", m.GetLabel())
		}
	}
	for _, name := range []string{"ninjascale_metric_value", "ninjascale_window_complete_buckets"} {
		if _, ok := families[name]; ok {
			t.Fatalf("%s exported without detailed metrics", name)
		}
	}
	// The shadow comparison relies on per-metric ages.
	if got := seriesFor(families, "ninjascale_metric_age_seconds", service); got != 3 {
		t.Fatalf("metric age series=%d want=3", got)
	}
}

func TestDetailedMetricsHaveOneSeriesPerWindow(t *testing.T) {
	service := "detailed-metrics-test"
	recordWindows(service)

	families := gather(t, newRegistry(true))
	if got := seriesFor(families, "ninjascale_window_evaluations_total", service); got != 3 {
		t.Fatalf("window evaluation series=%d want=3 (one per window and result)", got)
	}
	if got := seriesFor(families, "ninjascale_window_complete_buckets", service); got != 3 {
		t.Fatalf("complete bucket series=%d want=3", got)
	}
	if got := seriesFor(families, "ninjascale_metric_value", service); got != 3 {
		t.Fatalf("metric value series=%d want=3", got)
	}
}

func TestMetricsEndpointServesRegistry(t *testing.T) {
	RecordDesiredScale("endpoint-test", 3)
	srv := New(adapter.NewMemoryAdapter(), Options{})

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{`ninjascale_desired_scale{service="endpoint-test"} 3`, "go_goroutines", "process_cpu_seconds_total"} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics is missing %q", want)
		}
	}
}

// recordWindows records three windows on three metrics: two upscale windows
// with the same result and one downscale window.
func recordWindows(service string) {
	for _, w := range []struct{ metric, direction, result string }{
		{"queue_depth:a", "upscale", "stale"},
		{"queue_depth:b", "upscale", "stale"},
		{"queue_depth:c", "downscale", "breach"},
	} {
		RecordMetricValue(service, w.metric, 1)
		RecordMetricAge(service, w.metric, time.Second)
		RecordWindowCompleteBuckets(service, w.metric, w.direction, 1)
		RecordWindowEvaluation(service, w.metric, w.direction, w.result)
	}
}

func gather(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	return byName
}

func seriesFor(families map[string]*dto.MetricFamily, name, service string) int {
	count := 0
	for _, m := range families[name].GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "service" && label.GetValue() == service {
				count++
			}
		}
	}
	return count
}

func hasLabel(m *dto.Metric, name string) bool {
	for _, label := range m.GetLabel() {
		if label.GetName() == name {
			return true
		}
	}
	return false
}
