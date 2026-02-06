package metrics

import (
	"sync"
)

// MetricStore holds metric values from all sources.
type MetricStore struct {
	mu      sync.RWMutex
	metrics map[string]float64
}

// NewMetricStore creates a new metric store.
func NewMetricStore() *MetricStore {
	return &MetricStore{
		metrics: make(map[string]float64),
	}
}

// Set stores a metric value.
func (s *MetricStore) Set(name string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics[name] = value
}

// Get retrieves a metric value.
func (s *MetricStore) Get(name string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.metrics[name]
	return value, ok
}

// GetMultiple retrieves multiple metric values with a single lock acquisition.
func (s *MetricStore) GetMultiple(names []string) map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]float64, len(names))
	for _, name := range names {
		if value, ok := s.metrics[name]; ok {
			result[name] = value
		}
	}
	return result
}

// AsMap returns a copy of all metrics as a map.
func (s *MetricStore) AsMap() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]interface{}, len(s.metrics))
	for name, value := range s.metrics {
		result[name] = value
	}
	return result
}
