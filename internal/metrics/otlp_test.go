package metrics

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
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
			receiver.RegisterBinding("raw.metric", key, nil)
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
	receiver.RegisterBinding("raw", key, nil)
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
	receiver.RegisterBinding("raw", key, nil)
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
	receiver.RegisterBinding("empty-gauge", key, nil)
	receiver.RegisterBinding("empty-sum", key, nil)
	receiver.RegisterBinding("unsupported", key, nil)
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
		receiver.RegisterBinding("raw.queue", key, nil)
	}
	receiver.processMetric(&metricpb.Metric{Name: "raw.queue", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(9, now)}}}}, now)
	for _, key := range keys {
		if sample, ok := store.Snapshot().Latest(key); !ok || sample.Value != 9 {
			t.Fatalf("key %v sample=%+v present=%v", key, sample, ok)
		}
	}
}

func TestOTLPReceiverRoutesPointsByAttributes(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	messaging := MetricKey{Service: "worker", Name: "qd_messaging"}
	imports := MetricKey{Service: "worker", Name: "qd_imports"}
	priority := MetricKey{Service: "worker", Name: "qd_messaging_priority"}
	all := MetricKey{Service: "worker", Name: "qd_any"}
	receiver.RegisterBinding("raw.queue", messaging, map[string]string{"queue": "messaging"})
	receiver.RegisterBinding("raw.queue", imports, map[string]string{"queue": "imports"})
	receiver.RegisterBinding("raw.queue", priority, map[string]string{"queue": "messaging", "priority": "1"})
	receiver.RegisterBinding("raw.queue", all, nil)

	point := func(value float64, attributes ...*commonpb.KeyValue) *metricpb.NumberDataPoint {
		p := numberPoint(value, now)
		p.Attributes = attributes
		return p
	}
	stringAttribute := func(key, value string) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
	}
	intAttribute := func(key string, value int64) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}}}
	}
	receiver.processMetric(&metricpb.Metric{Name: "raw.queue", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{
		point(12, stringAttribute("queue", "messaging"), intAttribute("priority", 1)),
		point(3, stringAttribute("queue", "journeys")),
	}}}}, now)

	snapshot := store.Snapshot()
	for key, want := range map[MetricKey]float64{messaging: 12, priority: 12} {
		if sample, ok := snapshot.Latest(key); !ok || sample.Value != want {
			t.Fatalf("key %v sample=%+v present=%v want=%v", key, sample, ok, want)
		}
	}
	if sample, ok := snapshot.Latest(imports); ok {
		t.Fatalf("unmatched selector received sample %+v", sample)
	}
	if sample, ok := snapshot.Latest(all); !ok || sample.Value != 3 {
		t.Fatalf("binding without selector sample=%+v present=%v", sample, ok)
	}
}

func TestOTLPReceiverHTTPAcceptsGzip(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Name: "local"}
	receiver.RegisterBinding("raw", key, nil)
	req := metricRequest(&metricpb.Metric{Name: "raw", Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: []*metricpb.NumberDataPoint{numberPoint(11, now)}}}})
	body, _ := proto.Marshal(req)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/metrics", &compressed)
	request.Header.Set("Content-Encoding", "gzip")
	response := httptest.NewRecorder()
	receiver.handleHTTPMetrics(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	sample, ok := store.Snapshot().Latest(key)
	if !ok || sample.Value != 11 || !sample.ObservedAt.Equal(now) {
		t.Fatalf("sample=%+v present=%v", sample, ok)
	}
}

func TestOTLPReceiverHTTPRejectsUnsupportedCompression(t *testing.T) {
	receiver := NewOTLPReceiver(0, 0, NewMetricStore())
	request := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(nil))
	request.Header.Set("Content-Encoding", "br")
	response := httptest.NewRecorder()
	receiver.handleHTTPMetrics(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestOTLPReceiverHTTPMatchesGRPC(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	receiver := NewOTLPReceiver(0, 0, store, clock)
	key := MetricKey{Name: "local"}
	receiver.RegisterBinding("raw", key, nil)
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
