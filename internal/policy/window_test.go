package policy

import (
	"context"
	"testing"
	"time"

	"github.com/psteinroe/ninjascale/internal/metrics"
	"github.com/psteinroe/ninjascale/internal/testutil"
)

func windowSnapshot(t *testing.T, now time.Time, key metrics.MetricKey, samples ...metrics.Sample) metrics.Snapshot {
	t.Helper()
	store := metrics.NewMetricStore(
		metrics.WithClock(testutil.NewFakeClock(now)),
		metrics.WithRetention(24*time.Hour),
	)
	for _, sample := range samples {
		if !store.Add(key, sample) {
			t.Fatalf("sample rejected: %+v", sample)
		}
	}
	return store.Snapshot()
}

func completeSamples(now time.Time, width time.Duration, values ...float64) []metrics.Sample {
	oldest := now.Truncate(width).Add(-time.Duration(len(values)) * width)
	result := make([]metrics.Sample, len(values))
	for i, value := range values {
		result[i] = metrics.Sample{Value: value, ObservedAt: oldest.Add(time.Duration(i)*width + time.Second)}
	}
	return result
}

func TestWindowPolicyCompleteBuckets(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 20, 0, time.UTC)
	key := metrics.MetricKey{Name: "qd"}
	cases := []struct {
		name      string
		window    time.Duration
		samples   []metrics.Sample
		wantScale bool
		result    string
	}{
		{name: "one complete bucket satisfies ten seconds", window: 10 * time.Second, samples: completeSamples(now, 10*time.Second, 1), wantScale: true, result: "breach"},
		{name: "two contiguous buckets satisfy twenty seconds", window: 20 * time.Second, samples: completeSamples(now, 10*time.Second, 1, 2), wantScale: true, result: "breach"},
		{name: "missing newest bucket is stale", window: 20 * time.Second, samples: []metrics.Sample{{Value: 1, ObservedAt: now.Add(-19 * time.Second)}}, result: "stale"},
		{name: "interior gap", window: 30 * time.Second, samples: []metrics.Sample{{Value: 1, ObservedAt: now.Add(-29 * time.Second)}, {Value: 1, ObservedAt: now.Add(-9 * time.Second)}}, result: "gap"},
		{name: "one nonbreaching bucket breaks sequence", window: 20 * time.Second, samples: completeSamples(now, 10*time.Second, 1, 0), result: "not_breaching"},
		{name: "equality is not an upscale breach", window: 10 * time.Second, samples: completeSamples(now, 10*time.Second, 0.5), result: "not_breaching"},
		{name: "sample in current bucket is ignored", window: 10 * time.Second, samples: []metrics.Sample{{Value: 1, ObservedAt: now}}, result: "stale"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &WindowPolicy{Metric: "qd", BucketDuration: 10 * time.Second, Upscale: &WindowThreshold{Threshold: 0.5, SustainedFor: tc.window, Step: 1}}
			evaluation, err := p.Evaluate(context.Background(), 2, windowSnapshot(t, now, key, tc.samples...), now)
			if err != nil {
				t.Fatal(err)
			}
			scaled := evaluation.Decision != nil && evaluation.Decision.DesiredCount == 3
			if scaled != tc.wantScale {
				t.Fatalf("decision=%+v", evaluation.Decision)
			}
			if len(evaluation.Windows) != 1 || evaluation.Windows[0].Result != tc.result {
				t.Fatalf("observations=%+v", evaluation.Windows)
			}
		})
	}
}

func TestWindowPolicyDoesNotReuseOneSampleAsTimePasses(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 10, 0, time.UTC)
	key := metrics.MetricKey{Name: "qd"}
	snapshot := windowSnapshot(t, start, key, metrics.Sample{Value: 5, ObservedAt: start.Add(-time.Second)})
	p := &WindowPolicy{Metric: "qd", BucketDuration: 10 * time.Second, Upscale: &WindowThreshold{Threshold: .5, SustainedFor: 20 * time.Second, Step: 1}}
	for _, at := range []time.Time{start, start.Add(10 * time.Second), start.Add(20 * time.Second)} {
		evaluation, _ := p.Evaluate(context.Background(), 2, snapshot, at)
		if evaluation.Decision != nil {
			t.Fatalf("stale sample scaled at %v: %+v", at, evaluation.Decision)
		}
	}
}

func TestWindowPolicyDownscaleFailsClosed(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 10, 0, 0, time.UTC)
	key := metrics.MetricKey{Name: "busy"}
	p := &WindowPolicy{Metric: "busy", BucketDuration: 10 * time.Second, Downscale: &WindowThreshold{Threshold: .5, SustainedFor: 10 * time.Minute, Step: 1}}

	missing, _ := p.Evaluate(context.Background(), 5, windowSnapshot(t, now, key), now)
	if missing.Decision == nil || missing.Decision.DesiredCount != 5 {
		t.Fatalf("missing data must hold: %+v", missing.Decision)
	}

	zeros := completeSamples(now, 10*time.Second, make([]float64, 60)...)
	complete, _ := p.Evaluate(context.Background(), 5, windowSnapshot(t, now, key, zeros...), now)
	if complete.Decision == nil || complete.Decision.DesiredCount != 4 {
		t.Fatalf("60 zero buckets must downscale: %+v", complete.Decision)
	}

	equalValues := make([]float64, 60)
	equalValues[len(equalValues)-1] = .5
	equal := completeSamples(now, 10*time.Second, equalValues...)
	held, _ := p.Evaluate(context.Background(), 5, windowSnapshot(t, now, key, equal...), now)
	if held.Decision == nil || held.Decision.DesiredCount != 5 {
		t.Fatalf("equality must hold: %+v", held.Decision)
	}
}

func TestWindowPolicyResetRequiresPostResetBuckets(t *testing.T) {
	resetAt := time.Date(2024, 1, 1, 12, 0, 20, 0, time.UTC)
	key := metrics.MetricKey{Name: "qd"}
	p := &WindowPolicy{Metric: "qd", BucketDuration: 10 * time.Second, Upscale: &WindowThreshold{Threshold: .5, SustainedFor: 20 * time.Second, Step: 1}}
	p.Reset(resetAt)

	preScale := completeSamples(resetAt, 10*time.Second, 1, 1)
	immediate, _ := p.Evaluate(context.Background(), 3, windowSnapshot(t, resetAt, key, preScale...), resetAt)
	if immediate.Decision != nil || immediate.Windows[0].Result != "pre_reset" {
		t.Fatalf("pre-reset history reused: %+v", immediate)
	}

	now := resetAt.Add(20 * time.Second)
	all := append(preScale, completeSamples(now, 10*time.Second, 1, 1)...)
	postReset, _ := p.Evaluate(context.Background(), 3, windowSnapshot(t, now, key, all...), now)
	if postReset.Decision == nil || postReset.Decision.DesiredCount != 4 {
		t.Fatalf("post-reset buckets did not scale: %+v", postReset)
	}
}
