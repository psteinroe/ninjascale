package adapter

import (
	"context"
	"sync"
)

// MemoryAdapter implements Adapter for testing purposes.
type MemoryAdapter struct {
	mu       sync.RWMutex
	services map[string]int
}

// NewMemoryAdapter creates a new in-memory adapter.
func NewMemoryAdapter() *MemoryAdapter {
	return &MemoryAdapter{
		services: make(map[string]int),
	}
}

func (a *MemoryAdapter) GetCurrentCount(ctx context.Context, service string) (int, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.services[service], nil
}

func (a *MemoryAdapter) GetCurrentCounts(ctx context.Context, services []string) (map[string]int, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[string]int, len(services))
	for _, svc := range services {
		result[svc] = a.services[svc]
	}
	return result, nil
}

func (a *MemoryAdapter) Scale(ctx context.Context, service string, desired int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.services[service] = desired
	return nil
}

func (a *MemoryAdapter) HealthCheck(ctx context.Context) error {
	return nil
}

// SetCount is a test helper to set the current count for a service.
func (a *MemoryAdapter) SetCount(service string, count int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.services[service] = count
}
