package adapter

import (
	"context"
	"fmt"

	"github.com/psteinroe/ninjascale/internal/config"
)

// Adapter defines the interface for platform-specific scaling operations.
type Adapter interface {
	// GetCurrentCount returns the current number of running instances.
	GetCurrentCount(ctx context.Context, service string) (int, error)

	// GetCurrentCounts returns current counts for multiple services in a single batch call.
	// This is more efficient than calling GetCurrentCount multiple times.
	GetCurrentCounts(ctx context.Context, services []string) (map[string]int, error)

	// Scale sets the desired instance count.
	Scale(ctx context.Context, service string, desired int) error

	// HealthCheck verifies connectivity to the platform.
	HealthCheck(ctx context.Context) error
}

// New creates an adapter based on the config.
func New(cfg config.AdapterConfig) (Adapter, error) {
	switch cfg.Type {
	case "ecs":
		return NewECSAdapter(cfg.ECS)
	case "memory":
		return NewMemoryAdapter(), nil
	default:
		return nil, fmt.Errorf("unknown adapter type: %s", cfg.Type)
	}
}
