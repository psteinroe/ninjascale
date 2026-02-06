package policy

import (
	"context"
)

// Policy computes a desired instance count.
type Policy interface {
	// RequiredMetrics returns the names of metrics this policy needs.
	RequiredMetrics() []string

	// Evaluate returns the desired count based on current state and metrics.
	// Only the metrics declared by RequiredMetrics() are passed.
	// Returns nil if the policy has no opinion.
	Evaluate(ctx context.Context, current int, metrics map[string]float64) (*ScaleDecision, error)

	// Reset clears internal state (called after a scale event).
	Reset()
}

// ScaleDecision represents a scaling decision from a policy.
type ScaleDecision struct {
	DesiredCount int
	Reason       string
}
