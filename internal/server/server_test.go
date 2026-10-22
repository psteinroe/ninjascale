package server

import (
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestWindowAndMetricAgeObservability(t *testing.T) {
	service, metric := "observability-test", "qd"
	RecordMetricAge(service, metric, 3*time.Second)
	counter := windowEvaluationsTotal.WithLabelValues(service, "upscale", "stale")
	before := promtest.ToFloat64(counter)
	RecordWindowEvaluation(service, "upscale", "stale")

	if got := promtest.ToFloat64(metricAge.WithLabelValues(service, metric)); got != 3 {
		t.Fatalf("metric age=%v", got)
	}
	if got := promtest.ToFloat64(counter); got != before+1 {
		t.Fatalf("evaluation counter=%v want=%v", got, before+1)
	}
}

func TestWindowEvaluationsAreSummedOverWindows(t *testing.T) {
	before := promtest.CollectAndCount(windowEvaluationsTotal)
	// Two windows with the same direction and result share a series.
	RecordWindowEvaluation("summed-test", "upscale", "stale")
	RecordWindowEvaluation("summed-test", "upscale", "stale")
	RecordWindowEvaluation("summed-test", "downscale", "breach")

	if got := promtest.CollectAndCount(windowEvaluationsTotal) - before; got != 2 {
		t.Fatalf("new window evaluation series=%d want=2", got)
	}
}
