package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/psteinroe/ninjascale/internal/adapter"
	"github.com/psteinroe/ninjascale/internal/config"
	"github.com/psteinroe/ninjascale/internal/metrics"
	"github.com/psteinroe/ninjascale/internal/policy"
	"github.com/psteinroe/ninjascale/internal/schedule"
	"github.com/psteinroe/ninjascale/internal/testutil"
)

func TestReconciler_ScaleUpWins(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 3)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 100)

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{})
	err := r.reconcileService(context.Background(), svc, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10", count)
	}
}

func TestReconciler_ClampsToScheduleBounds(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 5)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 500) // wants 50

	clock := testutil.NewFakeClock(time.Date(2024, 1, 13, 12, 0, 0, 0, time.UTC)) // Saturday

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   100,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Schedule: &schedule.Schedule{
			Timezone: "UTC",
			Entries: []schedule.Entry{
				{
					Days:     []time.Weekday{time.Saturday, time.Sunday},
					Start:    "00:00",
					End:      "23:59",
					MinCount: 1,
					MaxCount: 10,
				},
			},
		},
		Policies: []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})

	err := r.reconcileService(context.Background(), svc, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10 (clamped to schedule max)", count)
	}
}

func TestReconciler_UpscaleCooldown(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 5)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 100)

	clock := testutil.NewFakeClock(time.Now())

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: 60 * time.Second, Downscale: 600 * time.Second},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})

	// First reconcile: should scale
	err := r.reconcileService(context.Background(), svc, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10", count)
	}

	// Increase demand
	store.Set("queue_depth", 200) // wants 20

	// Second reconcile immediately: should be blocked by cooldown
	err = r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ = adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10 (blocked by cooldown)", count)
	}

	// Advance past cooldown
	clock.Advance(61 * time.Second)

	// Third reconcile: should scale
	err = r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ = adp.GetCurrentCount(context.Background(), "svc1")
	if count != 20 {
		t.Errorf("got count %d, want 20", count)
	}
}

func TestReconciler_DownscaleCooldownIndependent(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 10)

	store := metrics.NewMetricStore()

	clock := testutil.NewFakeClock(time.Now())

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: 30 * time.Second, Downscale: 300 * time.Second},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})

	// Scale down
	store.Set("queue_depth", 50) // wants 5
	if err := r.reconcileService(context.Background(), svc, 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 5 {
		t.Errorf("got count %d, want 5", count)
	}

	// Immediately try to scale up (past upscale cooldown but not downscale)
	clock.Advance(31 * time.Second)
	store.Set("queue_depth", 150) // wants 15

	if err := r.reconcileService(context.Background(), svc, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, _ = adp.GetCurrentCount(context.Background(), "svc1")
	if count != 15 {
		t.Errorf("got count %d, want 15 (upscale independent of downscale cooldown)", count)
	}
}

func TestReconciler_DownscaleRequiresAllPoliciesAgree(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 10)

	store := metrics.NewMetricStore()
	store.Set("metric_a", 30)  // policy A wants ceil(30/10) = 3
	store.Set("metric_b", 100) // policy B wants ceil(100/10) = 10 (current)

	policyA, _ := policy.NewTargetPolicy("ceil(metric_a / 10)", []string{"metric_a"})
	policyB, _ := policy.NewTargetPolicy("ceil(metric_b / 10)", []string{"metric_b"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Policies:   []policy.Policy{policyA, policyB},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{})
	err := r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should NOT downscale because policy B still wants 10
	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10 (policy B still wants 10)", count)
	}
}

func TestReconciler_DownscaleTakesLeastAggressive(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 10)

	store := metrics.NewMetricStore()
	store.Set("metric_a", 30) // policy A wants ceil(30/10) = 3
	store.Set("metric_b", 50) // policy B wants ceil(50/10) = 5

	policyA, _ := policy.NewTargetPolicy("ceil(metric_a / 10)", []string{"metric_a"})
	policyB, _ := policy.NewTargetPolicy("ceil(metric_b / 10)", []string{"metric_b"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Policies:   []policy.Policy{policyA, policyB},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{})
	err := r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should downscale to 5 (least aggressive), not 3
	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 5 {
		t.Errorf("got count %d, want 5 (least aggressive downscale)", count)
	}
}

func TestReconciler_WindowPolicyGatesDownscale(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 10)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 30) // target policy wants ceil(30/10) = 3
	store.Set("latency_ms", 20)  // below threshold but not sustained yet

	clock := testutil.NewFakeClock(time.Now())

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})
	windowPolicy := &policy.WindowPolicy{
		Metric:         "latency_ms",
		BucketDuration: 10 * time.Second,
		Downscale: &policy.WindowThreshold{
			Threshold:    50,
			SustainedFor: 60 * time.Second,
			Step:         1,
		},
	}

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Policies:   []policy.Policy{targetPolicy, windowPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})

	// First reconcile: window policy should gate downscale (not sustained yet)
	err := r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10 (window gates downscale)", count)
	}

	// Advancing time without new buckets must not satisfy the window.
	clock.Advance(61 * time.Second)
	err = r.reconcileService(context.Background(), svc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ = adp.GetCurrentCount(context.Background(), "svc1")
	if count != 10 {
		t.Errorf("got count %d, want 10 (stale window must hold)", count)
	}
}

func TestReconciler_ScaleFromZeroBypassesCooldown(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 0)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 10)

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   0,
		MaxCount:   10,
		Cooldown:   config.CooldownConfig{Upscale: 60 * time.Second, Downscale: 600 * time.Second},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{})

	// Set a recent upscale time (normally would block)
	r.state["svc1"] = &serviceState{
		lastUpscaleTime: time.Now().Add(-10 * time.Second),
	}

	// Should still scale because current is 0
	err := r.reconcileService(context.Background(), svc, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 1 {
		t.Errorf("got count %d, want 1 (scale from zero bypasses cooldown)", count)
	}
}

func TestReconciler_DryRunDoesNotScale(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 3)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 100)

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: time.Hour, Downscale: time.Hour},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{DryRun: true})
	err := r.reconcileService(context.Background(), svc, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Count should remain unchanged in dry-run mode
	count, _ := adp.GetCurrentCount(context.Background(), "svc1")
	if count != 3 {
		t.Errorf("got count %d, want 3 (dry-run should not scale)", count)
	}
}

func TestReconciler_DryRunStillTracksCooldowns(t *testing.T) {
	adp := adapter.NewMemoryAdapter()
	adp.SetCount("svc1", 3)

	store := metrics.NewMetricStore()
	store.Set("queue_depth", 100)

	clock := testutil.NewFakeClock(time.Now())

	targetPolicy, _ := policy.NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})

	svc := &config.Service{
		Name:       "svc1",
		Identifier: "svc1",
		MinCount:   1,
		MaxCount:   20,
		Cooldown:   config.CooldownConfig{Upscale: 60 * time.Second, Downscale: 600 * time.Second},
		Policies:   []policy.Policy{targetPolicy},
	}

	r := New(adp, []*config.Service{svc}, store, nil, Options{DryRun: true, Clock: clock})

	// First reconcile in dry-run
	if err := r.reconcileService(context.Background(), svc, 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cooldown state should still be tracked
	r.mu.RLock()
	state := r.state["svc1"]
	r.mu.RUnlock()

	if state == nil {
		t.Fatal("expected state to be tracked in dry-run mode")
	}
	if state.lastUpscaleTime.IsZero() {
		t.Error("expected lastUpscaleTime to be set in dry-run mode")
	}
}

func TestReconcilerConservativeCompleteWindows(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 20, 0, time.UTC)
	cases := []struct {
		name       string
		qdValues   []float64
		busyValues []float64
		want       int
	}{
		{name: "missing busy blocks qd downscale", qdValues: []float64{0, 0}, want: 5},
		{name: "missing qd blocks busy downscale", busyValues: []float64{0, 0}, want: 5},
		{name: "both complete permit one step", qdValues: []float64{0, 0}, busyValues: []float64{0, 0}, want: 4},
		{name: "upscale wins over downscale", qdValues: []float64{5, 5}, busyValues: []float64{0, 0}, want: 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := testutil.NewFakeClock(now)
			store := metrics.NewMetricStore(metrics.WithClock(clock))
			addBuckets := func(name string, values []float64) {
				for i, value := range values {
					at := now.Add(-time.Duration(len(values)-i)*10*time.Second + time.Second)
					store.Add(metrics.MetricKey{Name: name}, metrics.Sample{Value: value, ObservedAt: at})
				}
			}
			addBuckets("qd", tc.qdValues)
			addBuckets("busy", tc.busyValues)
			qd := &policy.WindowPolicy{
				Metric: "qd", BucketDuration: 10 * time.Second,
				Upscale:   &policy.WindowThreshold{Threshold: .5, SustainedFor: 20 * time.Second, Step: 1},
				Downscale: &policy.WindowThreshold{Threshold: .5, SustainedFor: 20 * time.Second, Step: 1},
			}
			busy := &policy.WindowPolicy{
				Metric: "busy", BucketDuration: 10 * time.Second,
				Downscale: &policy.WindowThreshold{Threshold: .5, SustainedFor: 20 * time.Second, Step: 1},
			}
			adp := adapter.NewMemoryAdapter()
			adp.SetCount("svc", 5)
			svc := &config.Service{Name: "svc", Identifier: "svc", MinCount: 1, MaxCount: 10, Cooldown: config.CooldownConfig{}, Policies: []policy.Policy{qd, busy}}
			r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})
			if err := r.reconcileService(context.Background(), svc, 5); err != nil {
				t.Fatal(err)
			}
			count, _ := adp.GetCurrentCount(context.Background(), "svc")
			if count != tc.want {
				t.Fatalf("count=%d want=%d", count, tc.want)
			}
		})
	}
}

type snapshotProbePolicy struct {
	store  *metrics.MetricStore
	key    metrics.MetricKey
	mutate bool
	got    float64
}

func (p *snapshotProbePolicy) RequiredMetrics() []string { return []string{p.key.Name} }
func (p *snapshotProbePolicy) Reset(time.Time)           {}
func (p *snapshotProbePolicy) Evaluate(_ context.Context, _ int, snapshot metrics.Snapshot, now time.Time) (policy.Evaluation, error) {
	sample, _ := snapshot.Latest(p.key)
	p.got = sample.Value
	if p.mutate {
		p.store.Add(p.key, metrics.Sample{Value: 2, ObservedAt: now})
	}
	return policy.Evaluation{}, nil
}

func TestReconcilerPoliciesShareOneImmutableSnapshot(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := testutil.NewFakeClock(now)
	store := metrics.NewMetricStore(metrics.WithClock(clock))
	key := metrics.MetricKey{Name: "value"}
	store.Add(key, metrics.Sample{Value: 1, ObservedAt: now.Add(-time.Second)})
	first := &snapshotProbePolicy{store: store, key: key, mutate: true}
	second := &snapshotProbePolicy{store: store, key: key}
	svc := &config.Service{Name: "svc", Identifier: "svc", MinCount: 1, MaxCount: 10, Policies: []policy.Policy{first, second}}
	adp := adapter.NewMemoryAdapter()
	r := New(adp, []*config.Service{svc}, store, nil, Options{Clock: clock})
	if err := r.reconcileService(context.Background(), svc, 1); err != nil {
		t.Fatal(err)
	}
	if first.got != 1 || second.got != 1 {
		t.Fatalf("policies observed different values: first=%v second=%v", first.got, second.got)
	}
}
