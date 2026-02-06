package metrics

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// PrometheusSource fetches metrics from Prometheus.
type PrometheusSource struct {
	name    string
	address string
	api     promv1.API
}

// NewPrometheusSource creates a new Prometheus source.
func NewPrometheusSource(name, address, bearerTokenFile string) (*PrometheusSource, error) {
	cfg := api.Config{
		Address: address,
	}

	if bearerTokenFile != "" {
		token, err := os.ReadFile(bearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read bearer token: %w", err)
		}
		cfg.RoundTripper = &bearerAuthTransport{
			token: string(token),
			rt:    http.DefaultTransport,
		}
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create prometheus client: %w", err)
	}

	return &PrometheusSource{
		name:    name,
		address: address,
		api:     promv1.NewAPI(client),
	}, nil
}

// Query executes a Prometheus query and returns the result.
func (p *PrometheusSource) Query(ctx context.Context, query string) (float64, error) {
	result, warnings, err := p.api.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("prometheus query: %w", err)
	}

	for _, w := range warnings {
		// Log warnings but don't fail
		_ = w
	}

	switch v := result.(type) {
	case model.Vector:
		if len(v) == 0 {
			return 0, nil // Empty result = 0
		}
		return float64(v[0].Value), nil
	case *model.Scalar:
		return float64(v.Value), nil
	default:
		return 0, fmt.Errorf("unexpected prometheus result type: %T", result)
	}
}

// Name returns the source name.
func (p *PrometheusSource) Name() string {
	return p.name
}

type bearerAuthTransport struct {
	token string
	rt    http.RoundTripper
}

func (t *bearerAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.rt.RoundTrip(req)
}
