package policy

import (
	"context"
	"testing"
	"time"

	"github.com/psteinroe/ninjascale/internal/metrics"
)

func targetSnapshot(values map[string]float64) metrics.Snapshot {
	store := metrics.NewMetricStore()
	for name, value := range values {
		store.Set(name, value)
	}
	return store.Snapshot()
}

func TestTargetPolicyEvaluateLatestValues(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		values     map[string]float64
		current    int
		want       int
	}{
		{name: "simple division with ceil", expression: "ceil(queue_depth / 10)", values: map[string]float64{"queue_depth": 25}, current: 2, want: 3},
		{name: "zero queue returns zero", expression: "ceil(queue_depth / 10)", values: map[string]float64{"queue_depth": 0}, current: 2, want: 0},
		{name: "multi metric max", expression: "max(ceil(queue_depth / 10), ceil(connections / 100))", values: map[string]float64{"queue_depth": 25, "connections": 350}, current: 2, want: 4},
		{name: "uses current", expression: "max(ceil(queue_depth / 10), current / 2)", values: map[string]float64{"queue_depth": 5}, current: 10, want: 5},
		{name: "negative clamps to zero", expression: "queue_depth - 100", values: map[string]float64{"queue_depth": 50}, current: 2, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			names := make([]string, 0, len(tc.values))
			for name := range tc.values {
				names = append(names, name)
			}
			p, err := NewTargetPolicy(tc.expression, names)
			if err != nil {
				t.Fatal(err)
			}
			evaluation, err := p.Evaluate(context.Background(), tc.current, targetSnapshot(tc.values), time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			if evaluation.Decision == nil || evaluation.Decision.DesiredCount != tc.want {
				t.Fatalf("decision=%+v want=%d", evaluation.Decision, tc.want)
			}
		})
	}
}

func TestTargetPolicyUsesLatestEventTimeWithoutBucketFreshness(t *testing.T) {
	store := metrics.NewMetricStore()
	store.Set("queue_depth", 20)
	p, _ := NewTargetPolicy("ceil(queue_depth / 10)", []string{"queue_depth"})
	evaluation, err := p.Evaluate(context.Background(), 1, store.Snapshot(), time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC))
	if err != nil || evaluation.Decision.DesiredCount != 2 {
		t.Fatalf("decision=%+v err=%v", evaluation.Decision, err)
	}
}

func TestTargetPolicyCompileError(t *testing.T) {
	if _, err := NewTargetPolicy("invalid(syntax", []string{"queue_depth"}); err == nil {
		t.Fatal("expected compile error")
	}
}
