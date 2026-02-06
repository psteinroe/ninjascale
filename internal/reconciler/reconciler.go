package reconciler

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/psteinroe/ninjascale/internal/adapter"
	"github.com/psteinroe/ninjascale/internal/config"
	"github.com/psteinroe/ninjascale/internal/metrics"
	"github.com/psteinroe/ninjascale/internal/policy"
	"github.com/psteinroe/ninjascale/internal/server"
)

// ScaleDirection indicates the direction of scaling.
type ScaleDirection int

const (
	ScaleNone ScaleDirection = iota
	ScaleUp
	ScaleDown
)

func (d ScaleDirection) String() string {
	switch d {
	case ScaleUp:
		return "up"
	case ScaleDown:
		return "down"
	default:
		return "none"
	}
}

// Options configures the reconciler.
type Options struct {
	Interval time.Duration
	Clock    Clock
	DryRun   bool
}

// Clock interface for time operations.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Reconciler performs periodic scaling reconciliation.
type Reconciler struct {
	adapter     adapter.Adapter
	services    []*config.Service
	store       *metrics.MetricStore
	promSources map[string]*metrics.PrometheusSource
	interval    time.Duration
	clock       Clock
	dryRun      bool

	mu    sync.RWMutex
	state map[string]*serviceState
}

type serviceState struct {
	lastUpscaleTime   time.Time
	lastDownscaleTime time.Time
}

// New creates a new reconciler.
func New(adp adapter.Adapter, services []*config.Service, store *metrics.MetricStore, promSources map[string]*metrics.PrometheusSource, opts Options) *Reconciler {
	if opts.Interval == 0 {
		opts.Interval = 10 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}

	if promSources == nil {
		promSources = make(map[string]*metrics.PrometheusSource)
	}

	return &Reconciler{
		adapter:     adp,
		services:    services,
		store:       store,
		promSources: promSources,
		interval:    opts.Interval,
		clock:       opts.Clock,
		dryRun:      opts.DryRun,
		state:       make(map[string]*serviceState),
	}
}

// Run starts the reconciliation loop.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Run immediately on start
	r.Tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.Tick(ctx)
		}
	}
}

// Tick performs a single reconciliation pass.
func (r *Reconciler) Tick(ctx context.Context) {
	// Pull metrics from Prometheus sources
	r.pollPrometheusMetrics(ctx)

	// Batch-fetch all service counts in a single API call
	identifiers := make([]string, len(r.services))
	for i, svc := range r.services {
		identifiers[i] = svc.Identifier
	}

	counts, err := r.adapter.GetCurrentCounts(ctx, identifiers)
	if err != nil {
		slog.Error("batch get counts failed", "error", err)
		return
	}

	for _, svc := range r.services {
		current := counts[svc.Identifier]
		if err := r.reconcileService(ctx, svc, current); err != nil {
			slog.Error("reconcile failed", "service", svc.Name, "error", err)
		}
	}
}

func (r *Reconciler) pollPrometheusMetrics(ctx context.Context) {
	if len(r.promSources) == 0 {
		return
	}

	for _, svc := range r.services {
		for _, mb := range svc.Metrics {
			if !strings.HasPrefix(mb.Source, "prometheus.") {
				continue
			}
			srcName := mb.Source[len("prometheus."):]
			src, ok := r.promSources[srcName]
			if !ok {
				slog.Warn("unknown prometheus source", "source", mb.Source, "metric", mb.Name)
				continue
			}
			val, err := src.Query(ctx, mb.Query)
			if err != nil {
				slog.Warn("prometheus query failed", "source", mb.Source, "query", mb.Query, "error", err)
				continue
			}
			r.store.Set(mb.Name, val)
		}
	}
}

func (r *Reconciler) reconcileService(ctx context.Context, svc *config.Service, current int) error {
	start := r.clock.Now()
	defer func() {
		server.RecordReconcileLatency(svc.Name, r.clock.Now().Sub(start))
	}()

	// Record current scale
	server.RecordCurrentScale(svc.Name, current)

	// Collect all metric names needed by policies (single lock acquisition)
	var allMetricNames []string
	seen := make(map[string]bool)
	for _, p := range svc.Policies {
		for _, name := range p.RequiredMetrics() {
			if !seen[name] {
				seen[name] = true
				allMetricNames = append(allMetricNames, name)
			}
		}
	}
	allMetrics := r.store.GetMultiple(allMetricNames)

	// Record metric values for observability
	for name, val := range allMetrics {
		server.RecordMetricValue(svc.Name, name, val)
	}

	// 1. Determine active bounds from schedule
	minCount, maxCount := svc.MinCount, svc.MaxCount
	if svc.Schedule != nil {
		if min, max, matched := svc.Schedule.GetActiveBounds(r.clock.Now()); matched {
			minCount, maxCount = min, max
		}
	}

	// 2. Evaluate all policies (each gets only the metrics it needs)
	var decisions []*policy.ScaleDecision
	for _, p := range svc.Policies {
		// Filter metrics to only what this policy needs
		required := p.RequiredMetrics()
		policyMetrics := make(map[string]float64, len(required))
		for _, name := range required {
			if val, ok := allMetrics[name]; ok {
				policyMetrics[name] = val
			}
		}

		decision, err := p.Evaluate(ctx, current, policyMetrics)
		if err != nil {
			slog.Warn("policy evaluation failed", "service", svc.Name, "error", err)
			continue
		}
		if decision != nil {
			decisions = append(decisions, decision)
		}
	}

	if len(decisions) == 0 {
		slog.Debug("no policy decisions", "service", svc.Name, "current", current)
		return nil
	}

	// 3. Compute desired: scale-up wins, otherwise take least aggressive downscale
	desired := r.computeDesired(current, decisions)

	// 4. Clamp to bounds
	desired = clamp(desired, minCount, maxCount)

	// Record desired scale (before cooldown check)
	server.RecordDesiredScale(svc.Name, desired)

	// 5. No change needed
	if desired == current {
		return nil
	}

	// 6. Determine direction
	direction := ScaleUp
	if desired < current {
		direction = ScaleDown
	}

	// 7. Check cooldowns
	canScale := r.canScale(svc, current, direction)
	server.RecordCooldownActive(svc.Name, direction.String(), !canScale)

	if !canScale {
		slog.Debug("in cooldown",
			"service", svc.Name,
			"direction", direction,
			"desired", desired,
			"current", current)
		return nil
	}

	// 8. Execute scale
	if r.dryRun {
		slog.Info("dry-run: would scale",
			"service", svc.Name,
			"from", current,
			"to", desired,
			"direction", direction.String(),
			"reasons", formatReasons(decisions))
	} else {
		slog.Info("scaling",
			"service", svc.Name,
			"from", current,
			"to", desired,
			"direction", direction.String(),
			"reasons", formatReasons(decisions))

		if err := r.adapter.Scale(ctx, svc.Identifier, desired); err != nil {
			return err
		}
	}

	// 9. Record metrics and update state
	server.RecordScaleEvent(svc.Name, direction.String())
	r.recordScaleEvent(svc, direction)

	return nil
}

func (r *Reconciler) computeDesired(current int, decisions []*policy.ScaleDecision) int {
	// Find max (scale-up wins)
	maxDesired := current
	for _, d := range decisions {
		if d.DesiredCount > maxDesired {
			maxDesired = d.DesiredCount
		}
	}

	if maxDesired > current {
		return maxDesired
	}

	// No scale-up: only downscale if ALL policies agree to downscale.
	// Take max of downscale values (least aggressive).
	downscaleTarget := 0
	for _, d := range decisions {
		if d.DesiredCount >= current {
			// At least one policy doesn't want to downscale
			return current
		}
		if d.DesiredCount > downscaleTarget {
			downscaleTarget = d.DesiredCount
		}
	}

	return downscaleTarget
}

func (r *Reconciler) canScale(svc *config.Service, current int, direction ScaleDirection) bool {
	r.mu.RLock()
	state := r.state[svc.Name]
	r.mu.RUnlock()

	if state == nil {
		return true
	}

	// Fast wake-up: scaling from zero bypasses upscale cooldown
	if current == 0 && direction == ScaleUp {
		return true
	}

	now := r.clock.Now()

	switch direction {
	case ScaleUp:
		if state.lastUpscaleTime.IsZero() {
			return true
		}
		return now.Sub(state.lastUpscaleTime) >= svc.Cooldown.Upscale
	case ScaleDown:
		if state.lastDownscaleTime.IsZero() {
			return true
		}
		return now.Sub(state.lastDownscaleTime) >= svc.Cooldown.Downscale
	}

	return true
}

func (r *Reconciler) recordScaleEvent(svc *config.Service, direction ScaleDirection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state[svc.Name] == nil {
		r.state[svc.Name] = &serviceState{}
	}

	now := r.clock.Now()
	switch direction {
	case ScaleUp:
		r.state[svc.Name].lastUpscaleTime = now
	case ScaleDown:
		r.state[svc.Name].lastDownscaleTime = now
	}

	// Reset window policy timers after scale
	for _, p := range svc.Policies {
		p.Reset()
	}
}

func clamp(val, min, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func formatReasons(decisions []*policy.ScaleDecision) []string {
	reasons := make([]string, len(decisions))
	for i, d := range decisions {
		reasons[i] = d.Reason
	}
	return reasons
}
