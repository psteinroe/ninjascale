package policy

import (
	"context"
	"time"

	"github.com/psteinroe/ninjascale/internal/metrics"
)

// Policy computes a desired instance count from one immutable metric snapshot.
type Policy interface {
	RequiredMetrics() []string
	Evaluate(context.Context, int, metrics.Snapshot, time.Time) (Evaluation, error)
	Reset(time.Time)
}

// Evaluation contains an optional scaling opinion and bounded diagnostics.
type Evaluation struct {
	Decision *ScaleDecision
	Windows  []WindowEvaluation
}

// WindowEvaluation is a bounded-label observability result.
type WindowEvaluation struct {
	Service         string
	Metric          string
	Direction       string
	Result          string
	CompleteBuckets int
}

// ScaleDecision represents a scaling decision from a policy.
type ScaleDecision struct {
	DesiredCount int
	Reason       string
}
