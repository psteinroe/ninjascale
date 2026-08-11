package server

import (
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestWindowAndMetricAgeObservability(t *testing.T) {
	service, metric := "observability-test", "qd"
	RecordMetricAge(service, metric, 3*time.Second)
	RecordWindowCompleteBuckets(service, metric, "upscale", 2)
	counter := windowEvaluationsTotal.WithLabelValues(service, metric, "upscale", "stale")
	before := promtest.ToFloat64(counter)
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
}
