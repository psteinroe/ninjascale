package metrics

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPReceiver_Export_Gauge(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	req := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{},
						Metrics: []*metricpb.Metric{
							{
								Name: "queue_depth",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 42.5}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := receiver.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, ok := store.Get("queue_depth")
	if !ok {
		t.Fatal("expected metric queue_depth to exist")
	}
	if val != 42.5 {
		t.Errorf("got %v, want 42.5", val)
	}
}

func TestOTLPReceiver_Export_Sum(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	req := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{},
						Metrics: []*metricpb.Metric{
							{
								Name: "request_count",
								Data: &metricpb.Metric_Sum{
									Sum: &metricpb.Sum{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsInt{AsInt: 100}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := receiver.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, ok := store.Get("request_count")
	if !ok {
		t.Fatal("expected metric request_count to exist")
	}
	if val != 100.0 {
		t.Errorf("got %v, want 100.0", val)
	}
}

func TestOTLPReceiver_Export_MultipleMetrics(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	req := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{},
						Metrics: []*metricpb.Metric{
							{
								Name: "metric_a",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
										},
									},
								},
							},
							{
								Name: "metric_b",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 2.0}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := receiver.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	valA, _ := store.Get("metric_a")
	valB, _ := store.Get("metric_b")
	if valA != 1.0 {
		t.Errorf("metric_a = %v, want 1.0", valA)
	}
	if valB != 2.0 {
		t.Errorf("metric_b = %v, want 2.0", valB)
	}
}

func TestOTLPReceiver_Export_UsesLatestDataPoint(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	req := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{},
						Metrics: []*metricpb.Metric{
							{
								Name: "queue_depth",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 10.0}},
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 20.0}},
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 30.0}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := receiver.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, _ := store.Get("queue_depth")
	if val != 30.0 {
		t.Errorf("got %v, want 30.0 (last data point)", val)
	}
}

func TestOTLPReceiver_HTTP_Success(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	req := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{},
						Metrics: []*metricpb.Metric{
							{
								Name: "http_metric",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: 99.9}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	w := httptest.NewRecorder()

	receiver.handleHTTPMetrics(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	val, ok := store.Get("http_metric")
	if !ok {
		t.Fatal("expected metric http_metric to exist")
	}
	if val != 99.9 {
		t.Errorf("got %v, want 99.9", val)
	}
}

func TestOTLPReceiver_HTTP_MethodNotAllowed(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	httpReq := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	w := httptest.NewRecorder()

	receiver.handleHTTPMetrics(w, httpReq)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestOTLPReceiver_HTTP_InvalidProtobuf(t *testing.T) {
	store := NewMetricStore()
	receiver := &OTLPReceiver{store: store}

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader([]byte("not protobuf")))
	w := httptest.NewRecorder()

	receiver.handleHTTPMetrics(w, httpReq)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
