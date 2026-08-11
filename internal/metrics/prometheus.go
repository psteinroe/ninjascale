package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type prometheusAPI interface {
	Query(context.Context, string, time.Time, ...promv1.Option) (model.Value, promv1.Warnings, error)
}

// PrometheusSource fetches timestamped metrics from Prometheus.
type PrometheusSource struct {
	name    string
	address string
	api     prometheusAPI
}

// NewPrometheusSource creates a new Prometheus source.
func NewPrometheusSource(name, address, bearerTokenFile string) (*PrometheusSource, error) {
	cfg := api.Config{Address: address}
	if bearerTokenFile != "" {
		token, err := os.ReadFile(bearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read bearer token: %w", err)
		}
		cfg.RoundTripper = &bearerAuthTransport{token: string(token), rt: http.DefaultTransport}
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create prometheus client: %w", err)
	}
	return &PrometheusSource{name: name, address: address, api: promv1.NewAPI(client)}, nil
}

// Query executes a Prometheus query at evaluationTime. Empty vectors are
// missing, and vectors with more than one series are rejected as ambiguous.
func (p *PrometheusSource) Query(ctx context.Context, query string, evaluationTime time.Time) (Sample, bool, error) {
	result, warnings, err := p.api.Query(ctx, query, evaluationTime)
	if err != nil {
		return Sample{}, false, fmt.Errorf("prometheus query: %w", err)
	}
	for _, warning := range warnings {
		slog.Warn("prometheus query warning", "source", p.name, "warning", warning)
	}

	switch value := result.(type) {
	case model.Vector:
		switch len(value) {
		case 0:
			return Sample{}, false, nil
		case 1:
			return Sample{Value: float64(value[0].Value), ObservedAt: value[0].Timestamp.Time()}, true, nil
		default:
			return Sample{}, false, fmt.Errorf("prometheus query returned %d series; expected exactly one", len(value))
		}
	case *model.Scalar:
		return Sample{Value: float64(value.Value), ObservedAt: value.Timestamp.Time()}, true, nil
	default:
		return Sample{}, false, fmt.Errorf("unexpected prometheus result type: %T", result)
	}
}

// Name returns the source name.
func (p *PrometheusSource) Name() string { return p.name }

type bearerAuthTransport struct {
	token string
	rt    http.RoundTripper
}

func (t *bearerAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.rt.RoundTrip(clone)
}
