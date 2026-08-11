//go:build integration

package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPrometheusSource_Integration(t *testing.T) {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "prom/prometheus:v2.47.0",
			ExposedPorts: []string{"9090/tcp"},
			WaitingFor:   wait.ForHTTP("/-/ready").WithPort("9090"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "9090")
	address := fmt.Sprintf("http://%s:%s", host, port.Port())

	source, err := NewPrometheusSource("test", address, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("query built-in metric", func(t *testing.T) {
		sample, present, err := source.Query(ctx, "up", time.Now())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !present || sample.Value != 1.0 {
			t.Errorf("got %+v, present=%v; want 1.0", sample, present)
		}
	})

	t.Run("empty result is missing", func(t *testing.T) {
		_, present, err := source.Query(ctx, "nonexistent_metric", time.Now())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if present {
			t.Error("empty vector must be missing")
		}
	})
}
