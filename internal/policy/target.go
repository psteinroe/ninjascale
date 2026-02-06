package policy

import (
	"context"
	"fmt"
	"math"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// TargetPolicy computes desired count using an expression.
type TargetPolicy struct {
	Expression  string
	metricNames []string
	program     *vm.Program
}

// NewTargetPolicy creates a new target policy with compiled expression.
func NewTargetPolicy(expression string, metricNames []string) (*TargetPolicy, error) {
	program, err := CompileExpression(expression, metricNames)
	if err != nil {
		return nil, err
	}
	return &TargetPolicy{
		Expression:  expression,
		metricNames: metricNames,
		program:     program,
	}, nil
}

// RequiredMetrics returns the metric names this policy needs.
func (t *TargetPolicy) RequiredMetrics() []string {
	return t.metricNames
}

// CompileExpression compiles an expression for validation.
func CompileExpression(expression string, metricNames []string) (*vm.Program, error) {
	env := make(map[string]interface{})
	for _, name := range metricNames {
		env[name] = 0.0
	}
	env["current"] = 0

	program, err := expr.Compile(expression, expr.Env(env), expr.AsFloat64())
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
	}
	return program, nil
}

// Evaluate computes the desired count using the expression.
func (t *TargetPolicy) Evaluate(ctx context.Context, current int, metrics map[string]float64) (*ScaleDecision, error) {
	env := make(map[string]interface{}, len(metrics)+1)
	env["current"] = float64(current)
	for name, val := range metrics {
		env[name] = val
	}

	result, err := expr.Run(t.program, env)
	if err != nil {
		return nil, fmt.Errorf("evaluate expression: %w", err)
	}

	desired := int(math.Ceil(result.(float64)))
	if desired < 0 {
		desired = 0
	}

	return &ScaleDecision{
		DesiredCount: desired,
		Reason:       fmt.Sprintf("expr(%s) = %d", t.Expression, desired),
	}, nil
}

// Reset is a no-op for target policies.
func (t *TargetPolicy) Reset() {}
