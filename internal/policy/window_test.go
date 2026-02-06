package policy

import (
	"context"
	"testing"
	"time"

	"github.com/psteinroe/ninjascale/internal/testutil"
)

func TestWindowPolicy_Upscale(t *testing.T) {
	tests := []struct {
		name         string
		policy       WindowPolicy
		current      int
		metricValues []float64
		tickInterval time.Duration
		wantDecision *int
	}{
		{
			name: "breaches threshold but not sustained",
			policy: WindowPolicy{
				Metric: "queue_time",
				Upscale: WindowThreshold{
					Threshold:    50,
					SustainedFor: 20 * time.Second,
					Step:         1,
				},
			},
			current:      2,
			metricValues: []float64{60, 40},
			tickInterval: 10 * time.Second,
			wantDecision: nil,
		},
		{
			name: "sustained breach triggers upscale",
			policy: WindowPolicy{
				Metric: "queue_time",
				Upscale: WindowThreshold{
					Threshold:    50,
					SustainedFor: 20 * time.Second,
					Step:         2,
				},
			},
			current:      2,
			metricValues: []float64{60, 60, 60},
			tickInterval: 10 * time.Second,
			wantDecision: testutil.Ptr(4),
		},
		{
			name: "exactly at threshold does not trigger",
			policy: WindowPolicy{
				Metric: "queue_time",
				Upscale: WindowThreshold{
					Threshold:    50,
					SustainedFor: 10 * time.Second,
					Step:         1,
				},
			},
			current:      2,
			metricValues: []float64{50, 50},
			tickInterval: 10 * time.Second,
			wantDecision: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := testutil.NewFakeClock(time.Now())
			tt.policy.SetClock(clock)

			var decision *ScaleDecision
			for _, val := range tt.metricValues {
				metrics := map[string]float64{tt.policy.Metric: val}
				decision, _ = tt.policy.Evaluate(context.Background(), tt.current, metrics)
				clock.Advance(tt.tickInterval)
			}

			if tt.wantDecision == nil {
				if decision != nil {
					t.Errorf("expected nil decision, got %+v", decision)
				}
			} else {
				if decision == nil {
					t.Fatal("expected decision, got nil")
				}
				if decision.DesiredCount != *tt.wantDecision {
					t.Errorf("DesiredCount = %d, want %d", decision.DesiredCount, *tt.wantDecision)
				}
			}
		})
	}
}

func TestWindowPolicy_Downscale(t *testing.T) {
	tests := []struct {
		name         string
		policy       WindowPolicy
		current      int
		metricValues []float64
		tickInterval time.Duration
		wantDecision *int
	}{
		{
			name: "below threshold not long enough gates downscale",
			policy: WindowPolicy{
				Metric: "queue_time",
				Downscale: WindowThreshold{
					Threshold:    25,
					SustainedFor: 600 * time.Second,
					Step:         1,
				},
			},
			current:      5,
			metricValues: []float64{10, 10, 10},
			tickInterval: 10 * time.Second,
			wantDecision: testutil.Ptr(5), // stays at current to gate downscale
		},
		{
			name: "sustained below triggers downscale",
			policy: WindowPolicy{
				Metric: "queue_time",
				Downscale: WindowThreshold{
					Threshold:    25,
					SustainedFor: 30 * time.Second,
					Step:         1,
				},
			},
			current:      5,
			metricValues: []float64{10, 10, 10, 10},
			tickInterval: 10 * time.Second,
			wantDecision: testutil.Ptr(4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := testutil.NewFakeClock(time.Now())
			tt.policy.SetClock(clock)

			var decision *ScaleDecision
			for _, val := range tt.metricValues {
				metrics := map[string]float64{tt.policy.Metric: val}
				decision, _ = tt.policy.Evaluate(context.Background(), tt.current, metrics)
				clock.Advance(tt.tickInterval)
			}

			if tt.wantDecision == nil {
				if decision != nil {
					t.Errorf("expected nil decision, got %+v", decision)
				}
			} else {
				if decision == nil {
					t.Fatal("expected decision, got nil")
				}
				if decision.DesiredCount != *tt.wantDecision {
					t.Errorf("DesiredCount = %d, want %d", decision.DesiredCount, *tt.wantDecision)
				}
			}
		})
	}
}

func TestWindowPolicy_ResetAfterScale(t *testing.T) {
	p := &WindowPolicy{
		Metric: "queue_time",
		Upscale: WindowThreshold{
			Threshold:    50,
			SustainedFor: 20 * time.Second,
			Step:         1,
		},
	}

	clock := testutil.NewFakeClock(time.Now())
	p.SetClock(clock)

	metrics := map[string]float64{"queue_time": 60}

	// Build up to breach
	_, _ = p.Evaluate(context.Background(), 2, metrics)
	clock.Advance(10 * time.Second)

	_, _ = p.Evaluate(context.Background(), 2, metrics)
	clock.Advance(10 * time.Second)

	// This should trigger
	decision, _ := p.Evaluate(context.Background(), 2, metrics)
	if decision == nil {
		t.Fatal("expected decision, got nil")
	}

	// Reset (simulating what reconciler does after scale)
	p.Reset()

	// Immediately after reset, should not trigger
	decision, _ = p.Evaluate(context.Background(), 3, metrics)
	if decision != nil {
		t.Errorf("expected nil decision after reset, got %+v", decision)
	}
}

func TestWindowPolicy_MissingMetric(t *testing.T) {
	p := &WindowPolicy{
		Metric: "queue_time",
		Upscale: WindowThreshold{
			Threshold:    50,
			SustainedFor: 10 * time.Second,
			Step:         1,
		},
	}

	// Empty metrics map - metric is missing
	metrics := map[string]float64{}

	decision, err := p.Evaluate(context.Background(), 2, metrics)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if decision != nil {
		t.Errorf("expected nil decision for missing metric, got %+v", decision)
	}
}
