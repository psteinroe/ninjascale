package policy

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/psteinroe/ninjascale/internal/metrics"
)

// TargetPolicy computes desired count from the latest available metric values.
type TargetPolicy struct {
	Service     string
	Expression  string
	metricNames []string
	program     *vm.Program
}

// NewTargetPolicy creates a target policy with a compiled expression.
func NewTargetPolicy(expression string, metricNames []string) (*TargetPolicy, error) {
	program, err := CompileExpression(expression, metricNames)
	if err != nil {
		return nil, err
	}
	return &TargetPolicy{Expression: expression, metricNames: metricNames, program: program}, nil
}

// RequiredMetrics returns the local metric names referenced by the expression environment.
func (t *TargetPolicy) RequiredMetrics() []string { return t.metricNames }

// CompileExpression compiles an expression for validation.
func CompileExpression(expression string, metricNames []string) (*vm.Program, error) {
	env := make(map[string]interface{})
	for _, name := range metricNames {
		env[name] = 0.0
	}
	env["current"] = 0.0

	program, err := expr.Compile(expression, expr.Env(env), expr.AsFloat64())
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
	}
	return program, nil
}

// Evaluate preserves target policies' latest-value semantics.
func (t *TargetPolicy) Evaluate(_ context.Context, current int, snapshot metrics.Snapshot, _ time.Time) (Evaluation, error) {
	env := make(map[string]interface{}, len(t.metricNames)+1)
	env["current"] = float64(current)
	for _, name := range t.metricNames {
		sample, ok := snapshot.Latest(metrics.MetricKey{Service: t.Service, Name: name})
		if !ok {
			return Evaluation{}, fmt.Errorf("metric %s is missing", name)
		}
		env[name] = sample.Value
	}

	result, err := expr.Run(t.program, env)
	if err != nil {
		return Evaluation{}, fmt.Errorf("evaluate expression: %w", err)
	}

	desired := int(math.Ceil(result.(float64)))
	if desired < 0 {
		desired = 0
	}
	return Evaluation{Decision: &ScaleDecision{DesiredCount: desired, Reason: fmt.Sprintf("expr(%s) = %d", t.Expression, desired)}}, nil
}

// Reset is a no-op for target policies.
func (t *TargetPolicy) Reset(time.Time) {}
