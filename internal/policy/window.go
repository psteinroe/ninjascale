package policy

import (
	"context"
	"fmt"
	"time"
)

// WindowThreshold defines threshold conditions for window policy.
type WindowThreshold struct {
	Threshold    float64
	SustainedFor time.Duration
	Step         int
}

// WindowPolicy triggers scaling when a metric breaches a threshold for a sustained period.
type WindowPolicy struct {
	Metric    string
	Upscale   WindowThreshold
	Downscale WindowThreshold

	// Internal state
	breachingSince *time.Time
	belowSince     *time.Time

	// Clock for testing
	clock Clock
}

// Clock interface for time operations.
type Clock interface {
	Now() time.Time
}

func (w *WindowPolicy) now() time.Time {
	if w.clock != nil {
		return w.clock.Now()
	}
	return time.Now()
}

// RequiredMetrics returns the metric names this policy needs.
func (w *WindowPolicy) RequiredMetrics() []string {
	return []string{w.Metric}
}

// Evaluate checks if the metric has breached thresholds for the required duration.
func (w *WindowPolicy) Evaluate(ctx context.Context, current int, metrics map[string]float64) (*ScaleDecision, error) {
	val, ok := metrics[w.Metric]
	if !ok {
		return nil, nil // No opinion if metric missing
	}

	now := w.now()

	// Check upscale condition (> threshold)
	if w.Upscale.Threshold > 0 && val > w.Upscale.Threshold {
		if w.breachingSince == nil {
			w.breachingSince = &now
		}
		if now.Sub(*w.breachingSince) >= w.Upscale.SustainedFor {
			return &ScaleDecision{
				DesiredCount: current + w.Upscale.Step,
				Reason:       fmt.Sprintf("%s > %.2f for %s", w.Metric, w.Upscale.Threshold, w.Upscale.SustainedFor),
			}, nil
		}
		w.belowSince = nil // Reset downscale timer
	} else {
		w.breachingSince = nil // Reset upscale timer
	}

	// Check downscale condition (< threshold)
	if w.Downscale.Threshold > 0 && val < w.Downscale.Threshold {
		if w.belowSince == nil {
			w.belowSince = &now
		}
		if now.Sub(*w.belowSince) >= w.Downscale.SustainedFor {
			return &ScaleDecision{
				DesiredCount: current - w.Downscale.Step,
				Reason:       fmt.Sprintf("%s < %.2f for %s", w.Metric, w.Downscale.Threshold, w.Downscale.SustainedFor),
			}, nil
		}
	} else {
		w.belowSince = nil
	}

	// If downscale is configured but threshold not sustained, actively vote to stay at current.
	// This gates downscaling - other policies can't downscale until this window policy agrees.
	if w.Downscale.Threshold > 0 {
		return &ScaleDecision{
			DesiredCount: current,
			Reason:       fmt.Sprintf("%s downscale threshold not sustained", w.Metric),
		}, nil
	}

	return nil, nil
}

// Reset clears internal state.
func (w *WindowPolicy) Reset() {
	w.breachingSince = nil
	w.belowSince = nil
}

// SetClock sets a custom clock (for testing).
func (w *WindowPolicy) SetClock(c Clock) {
	w.clock = c
}
