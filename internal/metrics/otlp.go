package metrics

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// OTLPReceiver receives metrics via OTLP gRPC and HTTP.
type OTLPReceiver struct {
	colmetricpb.UnimplementedMetricsServiceServer
	store      *MetricStore
	grpcServer *grpc.Server
	httpServer *http.Server
	grpcAddr   string
	httpAddr   string
}

// NewOTLPReceiver creates a new OTLP receiver.
func NewOTLPReceiver(grpcPort, httpPort int, store *MetricStore) *OTLPReceiver {
	return &OTLPReceiver{
		store:    store,
		grpcAddr: fmt.Sprintf(":%d", grpcPort),
		httpAddr: fmt.Sprintf(":%d", httpPort),
	}
}

// Start begins listening for OTLP metrics.
func (o *OTLPReceiver) Start(ctx context.Context) error {
	// gRPC server
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

	// HTTP server (OTLP/HTTP)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", o.handleHTTPMetrics)
	o.httpServer = &http.Server{Addr: o.httpAddr, Handler: mux}

	go func() {
		if err := o.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the receiver.
func (o *OTLPReceiver) Stop(ctx context.Context) error {
	o.grpcServer.GracefulStop()
	return o.httpServer.Shutdown(ctx)
}

// Export implements the OTLP gRPC MetricsService.
func (o *OTLPReceiver) Export(ctx context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				o.processMetric(m)
			}
		}
	}
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

	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				o.processMetric(m)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (o *OTLPReceiver) processMetric(m *metricpb.Metric) {
	name := m.Name
	var value float64

	switch data := m.Data.(type) {
	case *metricpb.Metric_Gauge:
		if len(data.Gauge.DataPoints) > 0 {
			dp := data.Gauge.DataPoints[len(data.Gauge.DataPoints)-1]
			value = o.extractValue(dp)
		}
	case *metricpb.Metric_Sum:
		if len(data.Sum.DataPoints) > 0 {
			dp := data.Sum.DataPoints[len(data.Sum.DataPoints)-1]
			value = o.extractValue(dp)
		}
	}

	o.store.Set(name, value)
}

func (o *OTLPReceiver) extractValue(dp *metricpb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricpb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricpb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}
