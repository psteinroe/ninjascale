package policy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/psteinroe/ninjascale/internal/metrics"
)

// WindowThreshold defines one strict threshold condition.
type WindowThreshold struct {
	Threshold    float64
	SustainedFor time.Duration
	Step         int
}

// WindowPolicy evaluates consecutive completed metric buckets.
type WindowPolicy struct {
	Service        string
	Metric         string
	BucketDuration time.Duration
	Upscale        *WindowThreshold
	Downscale      *WindowThreshold

	mu          sync.RWMutex
	resetCutoff time.Time
}

func (w *WindowPolicy) RequiredMetrics() []string { return []string{w.Metric} }

// Evaluate checks completed buckets, never the currently open bucket.
func (w *WindowPolicy) Evaluate(_ context.Context, current int, snapshot metrics.Snapshot, now time.Time) (Evaluation, error) {
	w.mu.RLock()
	cutoff := w.resetCutoff
	w.mu.RUnlock()

	key := metrics.MetricKey{Service: w.Service, Name: w.Metric}
	result := Evaluation{}
	if w.Upscale != nil {
		decision, breached, observation := w.evaluateDirection(snapshot, key, now, cutoff, current, "upscale", w.Upscale, func(value, threshold float64) bool {
			return value > threshold
		})
		result.Windows = append(result.Windows, observation)
		if breached {
			result.Decision = decision
			return result, nil
		}
	}

	if w.Downscale != nil {
		decision, _, observation := w.evaluateDirection(snapshot, key, now, cutoff, current, "downscale", w.Downscale, func(value, threshold float64) bool {
			return value < threshold
		})
		result.Windows = append(result.Windows, observation)
		result.Decision = decision // Explicit hold when downscale cannot safely proceed.
	}
	return result, nil
}

func (w *WindowPolicy) evaluateDirection(
	snapshot metrics.Snapshot,
	key metrics.MetricKey,
	now, cutoff time.Time,
	current int,
	direction string,
	threshold *WindowThreshold,
	compare func(float64, float64) bool,
) (*ScaleDecision, bool, WindowEvaluation) {
	required := int(threshold.SustainedFor / w.BucketDuration)
	buckets, status := snapshot.CompleteBuckets(key, now, w.BucketDuration, required, cutoff)
	observation := WindowEvaluation{
		Service: w.Service, Metric: w.Metric, Direction: direction,
		Result: string(status), CompleteBuckets: len(buckets),
	}

	if status != metrics.BucketStatusComplete {
		reason := metrics.StatusReason(status, required, len(buckets))
		if direction == "downscale" {
			return &ScaleDecision{DesiredCount: current, Reason: reason}, false, observation
		}
		return nil, false, observation
	}

	for _, bucket := range buckets {
		if !compare(bucket.Sample.Value, threshold.Threshold) {
			observation.Result = "not_breaching"
			reason := fmt.Sprintf("%s %s threshold not breached in every completed bucket", w.Metric, direction)
			if direction == "downscale" {
				return &ScaleDecision{DesiredCount: current, Reason: reason}, false, observation
			}
			return nil, false, observation
		}
	}

	observation.Result = "breach"
	operator := ">"
	desired := current + threshold.Step
	if direction == "downscale" {
		operator = "<"
		desired = current - threshold.Step
	}
	return &ScaleDecision{
		DesiredCount: desired,
		Reason: fmt.Sprintf(
			"%s %s %.2f in %d consecutive complete %s buckets",
			w.Metric, operator, threshold.Threshold, required, w.BucketDuration,
		),
	}, true, observation
}

// Reset records a cutoff without deleting shared metric history.
func (w *WindowPolicy) Reset(at time.Time) {
	w.mu.Lock()
	w.resetCutoff = at
	w.mu.Unlock()
}
