package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	defaultRetention           = 30 * time.Second
	defaultMaxSamplesPerSeries = 10_000
	defaultMaxSamplesTotal     = 100_000
)

// MetricKey identifies a service-local metric binding.
type MetricKey struct {
	Service string
	Name    string
}

func (k MetricKey) String() string {
	if k.Service == "" {
		return k.Name
	}
	return k.Service + "/" + k.Name
}

// Sample is one metric observation at its source event time.
type Sample struct {
	Value      float64
	ObservedAt time.Time
}

// Bucket is an epoch-aligned, half-open interval represented by its latest sample.
type Bucket struct {
	Start  time.Time
	End    time.Time
	Sample Sample
}

// BucketStatus describes whether a requested completed window is usable.
type BucketStatus string

const (
	BucketStatusComplete   BucketStatus = "complete"
	BucketStatusMissing    BucketStatus = "missing"
	BucketStatusStale      BucketStatus = "stale"
	BucketStatusGap        BucketStatus = "gap"
	BucketStatusIncomplete BucketStatus = "incomplete"
	BucketStatusPreReset   BucketStatus = "pre_reset"
)

// Snapshot is an immutable view of the metric store.
type Snapshot interface {
	Latest(MetricKey) (Sample, bool)
	CompleteBuckets(MetricKey, time.Time, time.Duration, int, time.Time) ([]Bucket, BucketStatus)
}

// Clock provides receipt time and makes store behavior deterministic in tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// StoreOption configures a MetricStore.
type StoreOption func(*MetricStore)

// WithRetention sets the maximum event-time history retained per series.
func WithRetention(retention time.Duration) StoreOption {
	return func(s *MetricStore) {
		if retention > 0 {
			s.retention = retention
		}
	}
}

// WithSampleLimits sets hard per-series and store-wide sample limits. When a
// limit is reached, the oldest event-time samples are discarded. Window
// policies then fail closed if the eviction makes a window incomplete.
func WithSampleLimits(perSeries, total int) StoreOption {
	return func(s *MetricStore) {
		if perSeries > 0 {
			s.maxSamplesPerSeries = perSeries
		}
		if total > 0 {
			s.maxSamplesTotal = total
		}
	}
}

// WithClock sets the store clock.
func WithClock(clock Clock) StoreOption {
	return func(s *MetricStore) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// MetricStore holds timestamped metric history from all sources.
type MetricStore struct {
	mu                  sync.RWMutex
	series              map[MetricKey][]Sample
	retention           time.Duration
	maxSamplesPerSeries int
	maxSamplesTotal     int
	clock               Clock
}

// NewMetricStore creates a race-safe metric store with bounded retention.
func NewMetricStore(opts ...StoreOption) *MetricStore {
	s := &MetricStore{
		series:              make(map[MetricKey][]Sample),
		retention:           defaultRetention,
		maxSamplesPerSeries: defaultMaxSamplesPerSeries,
		maxSamplesTotal:     defaultMaxSamplesTotal,
		clock:               realClock{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Add stores a valid sample. Zero timestamps, non-finite values, samples from
// the future, and samples older than the retained interval are ignored.
func (s *MetricStore) Add(key MetricKey, sample Sample) bool {
	if key.Name == "" || sample.ObservedAt.IsZero() || math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return false
	}

	now := s.clock.Now()
	if sample.ObservedAt.After(now) || sample.ObservedAt.Before(now.Add(-s.retention)) {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	values := s.series[key]
	index := sort.Search(len(values), func(i int) bool {
		return !values[i].ObservedAt.Before(sample.ObservedAt)
	})
	if index < len(values) && values[index].ObservedAt.Equal(sample.ObservedAt) {
		values[index] = sample
	} else {
		values = append(values, Sample{})
		copy(values[index+1:], values[index:])
		values[index] = sample
	}

	s.series[key] = values
	s.pruneLocked(now.Add(-s.retention))
	s.enforceSampleLimitsLocked()
	return true
}

// Set stores a value under an unscoped key at receipt time. It is retained for
// compatibility with callers that do not need service-local aliases.
func (s *MetricStore) Set(name string, value float64) {
	s.Add(MetricKey{Name: name}, Sample{Value: value, ObservedAt: s.clock.Now()})
}

// Get retrieves the latest value for an unscoped key.
func (s *MetricStore) Get(name string) (float64, bool) {
	sample, ok := s.Snapshot().Latest(MetricKey{Name: name})
	return sample.Value, ok
}

// Snapshot returns one internally consistent immutable view. Expired history
// is pruned even when a series has stopped receiving data; its single latest
// sample is retained for target-policy latest-value compatibility.
func (s *MetricStore) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneLocked(s.clock.Now().Add(-s.retention))
	s.enforceSampleLimitsLocked()
	copySeries := make(map[MetricKey][]Sample, len(s.series))
	for key, values := range s.series {
		copySeries[key] = append([]Sample(nil), values...)
	}
	return &metricSnapshot{series: copySeries}
}

func (s *MetricStore) pruneLocked(cutoff time.Time) {
	for key, values := range s.series {
		s.series[key] = pruneSamples(values, cutoff)
	}
}

func (s *MetricStore) enforceSampleLimitsLocked() {
	for key, values := range s.series {
		if len(values) > s.maxSamplesPerSeries {
			s.series[key] = values[len(values)-s.maxSamplesPerSeries:]
		}
	}

	total := 0
	for _, values := range s.series {
		total += len(values)
	}
	for total > s.maxSamplesTotal {
		var oldestKey MetricKey
		var oldest time.Time
		found := false
		for key, values := range s.series {
			if len(values) == 0 {
				continue
			}
			if !found || values[0].ObservedAt.Before(oldest) {
				oldestKey, oldest, found = key, values[0].ObservedAt, true
			}
		}
		if !found {
			return
		}
		values := s.series[oldestKey]
		if len(values) == 1 {
			delete(s.series, oldestKey)
		} else {
			s.series[oldestKey] = values[1:]
		}
		total--
	}
}

func pruneSamples(values []Sample, cutoff time.Time) []Sample {
	if len(values) <= 1 {
		return values
	}
	first := sort.Search(len(values), func(i int) bool {
		return !values[i].ObservedAt.Before(cutoff)
	})
	if first == 0 {
		return values
	}
	if first == len(values) {
		return append([]Sample(nil), values[len(values)-1])
	}
	return append([]Sample(nil), values[first:]...)
}

// AsMap returns latest metric values, keyed by service/name.
func (s *MetricStore) AsMap() map[string]interface{} {
	snapshot := s.Snapshot().(*metricSnapshot)
	result := make(map[string]interface{}, len(snapshot.series))
	for key, values := range snapshot.series {
		if len(values) > 0 {
			result[key.String()] = values[len(values)-1].Value
		}
	}
	return result
}

type metricSnapshot struct {
	series map[MetricKey][]Sample
}

func (s *metricSnapshot) samples(key MetricKey) []Sample {
	values := s.series[key]
	if len(values) == 0 && key.Service != "" {
		// Unscoped fallback keeps the programmatic API backwards compatible;
		// configured ingestion always writes service-scoped keys.
		values = s.series[MetricKey{Name: key.Name}]
	}
	return values
}

func (s *metricSnapshot) Latest(key MetricKey) (Sample, bool) {
	values := s.samples(key)
	if len(values) == 0 {
		return Sample{}, false
	}
	return values[len(values)-1], true
}

func (s *metricSnapshot) CompleteBuckets(key MetricKey, now time.Time, width time.Duration, count int, after time.Time) ([]Bucket, BucketStatus) {
	if width <= 0 || count <= 0 {
		return nil, BucketStatusIncomplete
	}

	newestEnd := now.Truncate(width)
	oldestStart := newestEnd.Add(-time.Duration(count) * width)

	if !after.IsZero() {
		for i := 0; i < count; i++ {
			start := oldestStart.Add(time.Duration(i) * width)
			if start.Before(after) {
				return nil, BucketStatusPreReset
			}
		}
	}

	values := s.samples(key)
	if len(values) == 0 {
		return nil, BucketStatusMissing
	}

	buckets := make([]Bucket, 0, count)
	missing := make([]int, 0)
	for i := 0; i < count; i++ {
		start := oldestStart.Add(time.Duration(i) * width)
		end := start.Add(width)
		first := sort.Search(len(values), func(j int) bool {
			return !values[j].ObservedAt.Before(start)
		})
		last := sort.Search(len(values), func(j int) bool {
			return !values[j].ObservedAt.Before(end)
		})
		if first == last {
			missing = append(missing, i)
			continue
		}
		buckets = append(buckets, Bucket{Start: start, End: end, Sample: values[last-1]})
	}

	if len(missing) == 0 {
		return buckets, BucketStatusComplete
	}
	if missing[len(missing)-1] == count-1 {
		return buckets, BucketStatusStale
	}
	for _, index := range missing {
		if index > 0 {
			return buckets, BucketStatusGap
		}
	}
	if len(buckets) == 0 {
		return nil, BucketStatusMissing
	}
	return buckets, BucketStatusIncomplete
}

// StatusReason returns a stable human-readable data-quality reason.
func StatusReason(status BucketStatus, expected, found int) string {
	switch status {
	case BucketStatusMissing:
		return "window missing: no metric samples"
	case BucketStatusStale:
		return "window stale: newest completed bucket is missing"
	case BucketStatusGap:
		return "window gap: completed buckets are not contiguous"
	case BucketStatusPreReset:
		return "window reset: available buckets predate the last scale event"
	case BucketStatusIncomplete:
		return fmt.Sprintf("window incomplete: expected %d complete buckets, found %d", expected, found)
	default:
		return string(status)
	}
}
