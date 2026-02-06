package policy

import (
	"context"
	"testing"
)

func TestTargetPolicy_Evaluate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		metrics    map[string]float64
		current    int
		want       int
		wantErr    bool
	}{
		{
			name:       "simple division with ceil",
			expression: "ceil(queue_depth / 10)",
			metrics:    map[string]float64{"queue_depth": 25},
			current:    2,
			want:       3,
		},
		{
			name:       "zero queue returns zero",
			expression: "ceil(queue_depth / 10)",
			metrics:    map[string]float64{"queue_depth": 0},
			current:    2,
			want:       0,
		},
		{
			name:       "multi-metric max",
			expression: "max(ceil(queue_depth / 10), ceil(connections / 100))",
			metrics:    map[string]float64{"queue_depth": 25, "connections": 350},
			current:    2,
			want:       4,
		},
		{
			name:       "uses current",
			expression: "max(ceil(queue_depth / 10), current / 2)",
			metrics:    map[string]float64{"queue_depth": 5},
			current:    10,
			want:       5,
		},
		{
			name:       "negative result clamped to zero",
			expression: "queue_depth - 100",
			metrics:    map[string]float64{"queue_depth": 50},
			current:    2,
			want:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metricNames := make([]string, 0, len(tt.metrics))
			for k := range tt.metrics {
				metricNames = append(metricNames, k)
			}

			p, err := NewTargetPolicy(tt.expression, metricNames)
			if err != nil {
				t.Fatalf("unexpected compile error: %v", err)
			}

			decision, err := p.Evaluate(context.Background(), tt.current, tt.metrics)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if decision == nil {
				t.Fatal("expected decision, got nil")
			}
			if decision.DesiredCount != tt.want {
				t.Errorf("DesiredCount = %d, want %d", decision.DesiredCount, tt.want)
			}
		})
	}
}

func TestTargetPolicy_CompileError(t *testing.T) {
	_, err := NewTargetPolicy("invalid(syntax", []string{"queue_depth"})
	if err == nil {
		t.Error("expected error, got nil")
	}
}
