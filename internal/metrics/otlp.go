package metrics

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// OTLPReceiver receives metrics via OTLP gRPC and HTTP.
type OTLPReceiver struct {
	colmetricpb.UnimplementedMetricsServiceServer
	store      *MetricStore
	clock      Clock
	grpcServer *grpc.Server
	httpServer *http.Server
	grpcAddr   string
	httpAddr   string

	bindingsMu sync.RWMutex
	bindings   map[string][]MetricKey
}

// NewOTLPReceiver creates a receiver. Bindings must be registered before Start.
func NewOTLPReceiver(grpcPort, httpPort int, store *MetricStore, clocks ...Clock) *OTLPReceiver {
	clock := Clock(realClock{})
	if len(clocks) > 0 && clocks[0] != nil {
		clock = clocks[0]
	}
	return &OTLPReceiver{
		store:    store,
		clock:    clock,
		grpcAddr: fmt.Sprintf(":%d", grpcPort),
		httpAddr: fmt.Sprintf(":%d", httpPort),
		bindings: make(map[string][]MetricKey),
	}
}

// RegisterBinding routes one raw OTLP name to a service-local metric key.
func (o *OTLPReceiver) RegisterBinding(rawName string, key MetricKey) {
	if rawName == "" || key.Name == "" {
		return
	}
	o.bindingsMu.Lock()
	defer o.bindingsMu.Unlock()
	for _, existing := range o.bindings[rawName] {
		if existing == key {
			return
		}
	}
	o.bindings[rawName] = append(o.bindings[rawName], key)
}

// Start begins listening for OTLP metrics.
func (o *OTLPReceiver) Start(context.Context) error {
	o.grpcServer = grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(o.grpcServer, o)

	grpcLis, err := net.Listen("tcp", o.grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	go func() {
		if err := o.grpcServer.Serve(grpcLis); err != nil {
			slog.Error("grpc server error", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", o.handleHTTPMetrics)
	o.httpServer = &http.Server{Addr: o.httpAddr, Handler: mux}
	go func() {
		if err := o.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the receiver.
func (o *OTLPReceiver) Stop(ctx context.Context) error {
	if o.grpcServer != nil {
		o.grpcServer.GracefulStop()
	}
	if o.httpServer != nil {
		return o.httpServer.Shutdown(ctx)
	}
	return nil
}

// Export implements the OTLP gRPC MetricsService.
func (o *OTLPReceiver) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	o.processRequest(req, o.now())
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

func (o *OTLPReceiver) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	var req colmetricpb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, "failed to parse protobuf", http.StatusBadRequest)
		return
	}
	o.processRequest(&req, o.now())
	w.WriteHeader(http.StatusOK)
}

func (o *OTLPReceiver) now() time.Time {
	if o.clock == nil {
		return time.Now()
	}
	return o.clock.Now()
}

func (o *OTLPReceiver) processRequest(req *colmetricpb.ExportMetricsServiceRequest, receivedAt time.Time) {
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				o.processMetric(metric, receivedAt)
			}
		}
	}
}

func (o *OTLPReceiver) processMetric(metric *metricpb.Metric, receivedAt time.Time) {
	o.bindingsMu.RLock()
	bindings := append([]MetricKey(nil), o.bindings[metric.Name]...)
	o.bindingsMu.RUnlock()
	if len(bindings) == 0 {
		return
	}

	var points []*metricpb.NumberDataPoint
	switch data := metric.Data.(type) {
	case *metricpb.Metric_Gauge:
		if data.Gauge != nil {
			points = data.Gauge.DataPoints
		}
	case *metricpb.Metric_Sum:
		if data.Sum != nil {
			points = data.Sum.DataPoints
		}
	default:
		return
	}

	for _, point := range points {
		value, ok := extractValue(point)
		if !ok {
			continue
		}
		observedAt := receivedAt
		if point.TimeUnixNano != 0 {
			if point.TimeUnixNano > math.MaxInt64 {
				continue
			}
			observedAt = time.Unix(0, int64(point.TimeUnixNano))
		}
		for _, key := range bindings {
			o.store.Add(key, Sample{Value: value, ObservedAt: observedAt})
		}
	}
}

func extractValue(point *metricpb.NumberDataPoint) (float64, bool) {
	if point == nil {
		return 0, false
	}
	switch value := point.Value.(type) {
	case *metricpb.NumberDataPoint_AsDouble:
		return value.AsDouble, true
	case *metricpb.NumberDataPoint_AsInt:
		return float64(value.AsInt), true
	default:
		return 0, false
	}
}
