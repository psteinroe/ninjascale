package metrics

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/psteinroe/ninjascale/internal/testutil"
)

func metricRequest(metric *metricpb.Metric) *colmetricpb.ExportMetricsServiceRequest {
	return &colmetricpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricpb.ResourceMetrics{{
		ScopeMetrics: []*metricpb.ScopeMetrics{{Metrics: []*metricpb.Metric{metric}}},
	}}}
}

func numberPoint(value float64, at time.Time) *metricpb.NumberDataPoint {
	return &metricpb.NumberDataPoint{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: value}, TimeUnixNano: uint64(at.UnixNano())}
}

func TestOTLPReceiverPreservesGaugeAndSumTimestamps(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 30, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		metric *metricpb.Metric
	}{
		{name: "gauge", metric: &metricpb.Metric{Name: "raw.metric", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(42, now.Add(-time.Second))}}}}},
		{name: "sum", metric: &metricpb.Metric{Name: "raw.metric", Data: &metricpb.Metric_Sum{Sum: &metricpb.Sum{DataPoints: []*metricpb.NumberDataPoint{numberPoint(42, now.Add(-time.Second))}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := testutil.NewFakeClock(now)
			store := NewMetricStore(WithClock(clock))
			receiver := NewOTLPReceiver(0, 0, store, clock)
			key := MetricKey{Service: "worker", Name: "local"}
			receiver.RegisterBinding("raw.metric", key)
			_, err := receiver.Export(context.Background(), metricRequest(tc.metric))
			if err != nil {
				t.Fatal(err)
			}
			sample, ok := store.Snapshot().Latest(key)
			if !ok || sample.Value != 42 || !sample.ObservedAt.Equal(now.Add(-time.Second)) {
				t.Fatalf("sample = %+v, present=%v", sample, ok)
			}
		})
	}
}

func TestOTLPReceiverIngestsAllPointsByEventTime(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 30, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Service: "worker", Name: "qd"}
	receiver.RegisterBinding("raw", key)
	points := []*metricpb.NumberDataPoint{
		numberPoint(30, now.Add(-time.Second)),
		numberPoint(10, now.Add(-21*time.Second)),
		numberPoint(20, now.Add(-11*time.Second)),
	}
	receiver.processRequest(metricRequest(&metricpb.Metric{Name: "raw", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: points}}}), now)

	latest, _ := store.Snapshot().Latest(key)
	buckets, status := store.Snapshot().CompleteBuckets(key, now, 10*time.Second, 2, time.Time{})
	if latest.Value != 30 || status != BucketStatusComplete || len(buckets) != 2 || buckets[0].Sample.Value != 20 || buckets[1].Sample.Value != 30 {
		t.Fatalf("latest=%+v buckets=%+v status=%s", latest, buckets, status)
	}
}

func TestOTLPReceiverUsesReceiptTimeForZeroTimestamp(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Name: "qd"}
	receiver.RegisterBinding("raw", key)
	receiver.processMetric(&metricpb.Metric{Name: "raw", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{{Value: &metricpb.NumberDataPoint_AsInt{AsInt: 7}}}}}}, now)
	sample, ok := store.Snapshot().Latest(key)
	if !ok || !sample.ObservedAt.Equal(now) || sample.Value != 7 {
		t.Fatalf("sample=%+v present=%v", sample, ok)
	}
}

func TestOTLPReceiverIgnoresEmptyUnsupportedAndUnboundMetrics(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Name: "local"}
	receiver.RegisterBinding("empty-gauge", key)
	receiver.RegisterBinding("empty-sum", key)
	receiver.RegisterBinding("unsupported", key)
	receiver.processMetric(&metricpb.Metric{Name: "empty-gauge", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{}}}, now)
	receiver.processMetric(&metricpb.Metric{Name: "empty-sum", Data: &metricpb.Metric_Sum{Sum: &metricpb.Sum{}}}, now)
	receiver.processMetric(&metricpb.Metric{Name: "unsupported", Data: &metricpb.Metric_Histogram{Histogram: &metricpb.Histogram{}}}, now)
	receiver.processMetric(&metricpb.Metric{Name: "unbound", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(0, now)}}}}, now)
	if _, ok := store.Snapshot().Latest(key); ok {
		t.Fatal("invalid payload manufactured a sample")
	}
}

func TestOTLPReceiverAliasesOneRawMetricToMultipleServices(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	keys := []MetricKey{{Service: "a", Name: "qd"}, {Service: "b", Name: "queue"}}
	for _, key := range keys {
		receiver.RegisterBinding("raw.queue", key)
	}
	receiver.processMetric(&metricpb.Metric{Name: "raw.queue", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(9, now)}}}}, now)
	for _, key := range keys {
		if sample, ok := store.Snapshot().Latest(key); !ok || sample.Value != 9 {
			t.Fatalf("key %v sample=%+v present=%v", key, sample, ok)
		}
	}
}

func TestOTLPReceiverHTTPMatchesGRPC(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Name: "local"}
	receiver.RegisterBinding("raw", key)
	req := metricRequest(&metricpb.Metric{Name: "raw", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(11, now)}}}})
	body, _ := proto.Marshal(req)
	response := httptest.NewRecorder()
	receiver.handleHTTPMetrics(response, httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	sample, ok := store.Snapshot().Latest(key)
	if !ok || sample.Value != 11 || !sample.ObservedAt.Equal(now) {
		t.Fatalf("sample=%+v present=%v", sample, ok)
	}
}
