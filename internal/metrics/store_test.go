package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/psteinroe/ninjascale/internal/testutil"
)

func TestMetricStoreTimestampedHistory(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 30, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock), WithRetention(time.Minute))
	key := MetricKey{Service: "worker", Name: "qd"}

	for _, sample := range []Sample{
		{Value: 3, ObservedAt: now.Add(-time.Second)},
		{Value: 5, ObservedAt: now.Add(-9 * time.Second)}, // out of arrival order
		{Value: 4, ObservedAt: now.Add(-2 * time.Second)},
	} {
		if !store.Add(key, sample) {
			t.Fatalf("sample %+v was rejected", sample)
		}
	}

	latest, ok := store.Snapshot().Latest(key)
	if !ok || latest.Value != 3 || !latest.ObservedAt.Equal(now.Add(-time.Second)) {
		t.Fatalf("latest = %+v, %v", latest, ok)
	}
	buckets, status := store.Snapshot().CompleteBuckets(key, now, 10*time.Second, 1, time.Time{})
	if status != BucketStatusComplete || len(buckets) != 1 || buckets[0].Sample.Value != 3 {
		t.Fatalf("buckets = %+v, status = %s", buckets, status)
	}
}

func TestMetricStoreBucketBoundaryUsesNewerBucket(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 20, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock))
	key := MetricKey{Name: "qd"}
	store.Add(key, Sample{Value: 1, ObservedAt: now.Add(-10 * time.Second)})
	store.Add(key, Sample{Value: 2, ObservedAt: now}) // open [20,30), never [10,20)

	buckets, status := store.Snapshot().CompleteBuckets(key, now, 10*time.Second, 1, time.Time{})
	if status != BucketStatusComplete || buckets[0].Sample.Value != 1 {
		t.Fatalf("buckets = %+v, status = %s", buckets, status)
	}
}

func TestMetricStoreRejectsInvalidFutureAndExpiredSamples(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	store := NewMetricStore(WithClock(testutil.NewFakeClock(now)), WithRetention(time.Minute))
	key := MetricKey{Name: "qd"}
	for _, sample := range []Sample{
		{Value: 1},
		{Value: 1, ObservedAt: now.Add(time.Second)},
		{Value: 1, ObservedAt: now.Add(-time.Minute - time.Nanosecond)},
	} {
		if store.Add(key, sample) {
			t.Fatalf("invalid sample was accepted: %+v", sample)
		}
	}
	if _, ok := store.Snapshot().Latest(key); ok {
		t.Fatal("invalid samples created a series")
	}
}

func TestMetricStoreRetentionAndImmutableSnapshots(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 1, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := NewMetricStore(WithClock(clock), WithRetention(40*time.Second))
	key := MetricKey{Name: "qd"}
	store.Add(key, Sample{Value: 1, ObservedAt: now.Add(-40 * time.Second)})
	before := store.Snapshot()
	store.Add(key, Sample{Value: 2, ObservedAt: now.Add(-time.Second)})

	oldLatest, _ := before.Latest(key)
	newLatest, _ := store.Snapshot().Latest(key)
	if oldLatest.Value != 1 || newLatest.Value != 2 {
		t.Fatalf("snapshot consistency failed: before=%+v after=%+v", oldLatest, newLatest)
	}

	clock.Advance(10 * time.Second)
	store.Add(key, Sample{Value: 3, ObservedAt: clock.Now()})
	values := store.Snapshot().(*metricSnapshot).samples(key)
	if len(values) != 2 || values[0].Value != 2 {
		t.Fatalf("retained values = %+v", values)
	}

	clock.Advance(time.Hour)
	staleLatest, ok := store.Snapshot().Latest(key)
	if !ok || staleLatest.Value != 3 {
		t.Fatalf("latest target value was not preserved: %+v, %v", staleLatest, ok)
	}
	if history := store.Snapshot().(*metricSnapshot).samples(key); len(history) != 1 {
		t.Fatalf("expired history was not bounded: %+v", history)
	}
}

func TestMetricStoreDefaultRetentionIsThirtySeconds(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	store := NewMetricStore(WithClock(testutil.NewFakeClock(now)))
	key := MetricKey{Name: "qd"}
	if store.Add(key, Sample{Value: 1, ObservedAt: now.Add(-defaultRetention - time.Nanosecond)}) {
		t.Fatal("sample older than the default retention was accepted")
	}
	if !store.Add(key, Sample{Value: 2, ObservedAt: now.Add(-defaultRetention)}) {
		t.Fatal("sample at the default retention boundary was rejected")
	}
}

func TestMetricStoreEnforcesPerSeriesAndGlobalSampleLimits(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 20, 0, time.UTC)
	store := NewMetricStore(
		WithClock(testutil.NewFakeClock(now)),
		WithRetention(time.Minute),
		WithSampleLimits(3, 5),
	)
	keyA := MetricKey{Service: "worker-a", Name: "qd"}
	keyB := MetricKey{Service: "worker-b", Name: "qd"}
	for i := 0; i < 4; i++ {
		store.Add(keyA, Sample{Value: float64(i + 1), ObservedAt: now.Add(time.Duration(i-10) * time.Second)})
		store.Add(keyB, Sample{Value: float64(i + 1), ObservedAt: now.Add(time.Duration(i-6) * time.Second)})
	}

	snapshot := store.Snapshot().(*metricSnapshot)
	if got := len(snapshot.samples(keyA)); got != 2 {
		t.Fatalf("series A retained %d samples, want 2 after global eviction", got)
	}
	if got := len(snapshot.samples(keyB)); got != 3 {
		t.Fatalf("series B retained %d samples, want per-series limit 3", got)
	}
	total := 0
	for _, samples := range snapshot.series {
		total += len(samples)
	}
	if total != 5 {
		t.Fatalf("store retained %d samples, want global limit 5", total)
	}
	for _, key := range []MetricKey{keyA, keyB} {
		latest, ok := snapshot.Latest(key)
		if !ok || latest.Value != 4 {
			t.Fatalf("latest sample for %v = %+v, present=%v", key, latest, ok)
		}
	}
}

func TestMetricStoreConcurrentSnapshotsAndWrites(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	store := NewMetricStore(WithClock(testutil.NewFakeClock(now)), WithRetention(time.Hour))
	key := MetricKey{Service: "worker", Name: "qd"}
	var wg sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Add(key, Sample{Value: float64(writer*100 + i), ObservedAt: now.Add(-time.Duration(writer*100+i) * time.Microsecond)})
			}
		}()
	}
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Snapshot().Latest(key)
			}
		}()
	}
	wg.Wait()
	if _, ok := store.Snapshot().Latest(key); !ok {
		t.Fatal("expected a latest sample")
	}
}
