package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	t.Run("valid full config", func(t *testing.T) {
		yaml := `
reconcile_interval: 15s

defaults:
  min_count: 1
  max_count: 10
  cooldown:
    upscale: 60s
    downscale: 600s

metrics:
  otlp:
    enabled: true
    grpc_port: 4317
    http_port: 4318
  prometheus:
    - name: primary
      address: "http://prometheus:9090"

adapter:
  type: ecs
  ecs:
    region: eu-central-1
    cluster: production

server:
  address: ":8080"

services:
  - name: api-worker
    identifier: api-worker-svc
    min_count: 2
    max_count: 20
    cooldown:
      upscale: 30s
      downscale: 300s
    metrics:
      - name: queue_depth
        source: prometheus.primary
        query: "sum(queue_depth)"
    policies:
      - type: target
        expression: "ceil(queue_depth / 10)"
`
		cfg, err := Parse([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.ReconcileInterval != 15*time.Second {
			t.Errorf("ReconcileInterval = %v, want %v", cfg.ReconcileInterval, 15*time.Second)
		}
		if cfg.Defaults.MinCount != 1 {
			t.Errorf("Defaults.MinCount = %d, want 1", cfg.Defaults.MinCount)
		}
		if cfg.Defaults.MaxCount != 10 {
			t.Errorf("Defaults.MaxCount = %d, want 10", cfg.Defaults.MaxCount)
		}
		if !cfg.Metrics.OTLP.Enabled {
			t.Error("expected OTLP enabled")
		}
		if cfg.Adapter.Type != "ecs" {
			t.Errorf("Adapter.Type = %q, want %q", cfg.Adapter.Type, "ecs")
		}
		if cfg.Adapter.ECS.Region != "eu-central-1" {
			t.Errorf("Adapter.ECS.Region = %q, want %q", cfg.Adapter.ECS.Region, "eu-central-1")
		}
		if len(cfg.Services) != 1 {
			t.Fatalf("len(Services) = %d, want 1", len(cfg.Services))
		}
		if cfg.Services[0].Name != "api-worker" {
			t.Errorf("Services[0].Name = %q, want %q", cfg.Services[0].Name, "api-worker")
		}
		if *cfg.Services[0].MinCount != 2 {
			t.Errorf("Services[0].MinCount = %d, want 2", *cfg.Services[0].MinCount)
		}
	})

	t.Run("minimal config uses defaults", func(t *testing.T) {
		yaml := `
adapter:
  type: memory

services:
  - name: worker
    identifier: worker
    policies:
      - type: target
        expression: "1"
`
		cfg, err := Parse([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.ReconcileInterval != 10*time.Second {
			t.Errorf("ReconcileInterval = %v, want %v", cfg.ReconcileInterval, 10*time.Second)
		}
		if cfg.Defaults.MinCount != 1 {
			t.Errorf("Defaults.MinCount = %d, want 1", cfg.Defaults.MinCount)
		}
		if cfg.Defaults.MaxCount != 10 {
			t.Errorf("Defaults.MaxCount = %d, want 10", cfg.Defaults.MaxCount)
		}
		if cfg.Defaults.Cooldown.Upscale != 60*time.Second {
			t.Errorf("Defaults.Cooldown.Upscale = %v, want %v", cfg.Defaults.Cooldown.Upscale, 60*time.Second)
		}
		if cfg.Defaults.Cooldown.Downscale != 600*time.Second {
			t.Errorf("Defaults.Cooldown.Downscale = %v, want %v", cfg.Defaults.Cooldown.Downscale, 600*time.Second)
		}
	})

	t.Run("service inherits defaults", func(t *testing.T) {
		yaml := `
defaults:
  min_count: 2
  max_count: 50
  cooldown:
    upscale: 30s
    downscale: 120s

adapter:
  type: memory

services:
  - name: worker
    identifier: worker
    policies:
      - type: target
        expression: "1"
`
		cfg, err := Parse([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		svc := cfg.Services[0]
		if *svc.MinCount != 2 {
			t.Errorf("MinCount = %d, want 2", *svc.MinCount)
		}
		if *svc.MaxCount != 50 {
			t.Errorf("MaxCount = %d, want 50", *svc.MaxCount)
		}
		if svc.Cooldown.Upscale != 30*time.Second {
			t.Errorf("Cooldown.Upscale = %v, want %v", svc.Cooldown.Upscale, 30*time.Second)
		}
		if svc.Cooldown.Downscale != 120*time.Second {
			t.Errorf("Cooldown.Downscale = %v, want %v", svc.Cooldown.Downscale, 120*time.Second)
		}
	})

	t.Run("service overrides defaults", func(t *testing.T) {
		yaml := `
defaults:
  min_count: 2
  max_count: 50

adapter:
  type: memory

services:
  - name: worker
    identifier: worker
    min_count: 5
    max_count: 100
    policies:
      - type: target
        expression: "1"
`
		cfg, err := Parse([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		svc := cfg.Services[0]
		if *svc.MinCount != 5 {
			t.Errorf("MinCount = %d, want 5", *svc.MinCount)
		}
		if *svc.MaxCount != 100 {
			t.Errorf("MaxCount = %d, want 100", *svc.MaxCount)
		}
	})
}

func TestParseConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing adapter",
			yaml:    `services: []`,
			wantErr: "adapter.type is required",
		},
		{
			name: "invalid adapter type",
			yaml: `
adapter:
  type: kubernetes
services: []
`,
			wantErr: "unknown adapter type: kubernetes",
		},
		{
			name: "missing services",
			yaml: `
adapter:
  type: memory
`,
			wantErr: "at least one service is required",
		},
		{
			name: "service missing name",
			yaml: `
adapter:
  type: memory
services:
  - identifier: foo
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "service[0]: name is required",
		},
		{
			name: "service missing identifier",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "service[0]: identifier is required",
		},
		{
			name: "service missing policies",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
`,
			wantErr: "service[0]: at least one policy is required",
		},
		{
			name: "invalid policy type",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    policies:
      - type: unknown
`,
			wantErr: "service[0].policies[0]: unknown policy type: unknown",
		},
		{
			name: "min_count greater than max_count",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    min_count: 10
    max_count: 5
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "service[0]: min_count (10) cannot exceed max_count (5)",
		},
		{
			name: "window policy missing metric",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    policies:
      - type: window
        upscale:
          threshold: 50
`,
			wantErr: "service[0].policies[0]: metric is required for window policy",
		},
		{
			name: "target policy missing expression",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    policies:
      - type: target
`,
			wantErr: "service[0].policies[0]: expression is required for target policy",
		},
		{
			name: "ECS adapter missing cluster",
			yaml: `
adapter:
  type: ecs
  ecs:
    region: us-east-1
services:
  - name: foo
    identifier: foo
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "adapter.ecs.cluster is required",
		},
		{
			name: "ECS adapter missing region",
			yaml: `
adapter:
  type: ecs
  ecs:
    cluster: my-cluster
services:
  - name: foo
    identifier: foo
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "adapter.ecs.region is required",
		},
		{
			name: "prometheus source missing address",
			yaml: `
metrics:
  prometheus:
    - name: primary
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "metrics.prometheus[0]: address is required",
		},
		{
			name: "schedule invalid timezone",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    schedule:
      timezone: "Invalid/Zone"
      entries:
        - start: "08:00"
          end: "18:00"
          min_count: 1
          max_count: 10
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "service[0].schedule: unknown timezone: Invalid/Zone",
		},
		{
			name: "schedule invalid time format",
			yaml: `
adapter:
  type: memory
services:
  - name: foo
    identifier: foo
    schedule:
      timezone: UTC
      entries:
        - start: "8:00"
          end: "18:00"
          min_count: 1
          max_count: 10
    policies:
      - type: target
        expression: "1"
`,
			wantErr: "service[0].schedule.entries[0]: invalid start time format, expected HH:MM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestParseConfig_ScheduleParsing(t *testing.T) {
	yaml := `
adapter:
  type: memory
services:
  - name: worker
    identifier: worker
    schedule:
      timezone: Europe/Berlin
      entries:
        - days: [mon, tue, wed, thu, fri]
          start: "08:00"
          end: "18:00"
          min_count: 3
          max_count: 20
        - days: [sat, sun]
          start: "00:00"
          end: "23:59"
          min_count: 1
          max_count: 5
    policies:
      - type: target
        expression: "1"
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sched := cfg.Services[0].Schedule
	if sched == nil {
		t.Fatal("expected schedule, got nil")
	}
	if sched.Timezone != "Europe/Berlin" {
		t.Errorf("Timezone = %q, want %q", sched.Timezone, "Europe/Berlin")
	}
	if len(sched.Entries) != 2 {
		t.Fatalf("len(Entries) = %d, want 2", len(sched.Entries))
	}

	wantDays := []string{"mon", "tue", "wed", "thu", "fri"}
	if len(sched.Entries[0].Days) != len(wantDays) {
		t.Fatalf("len(Entries[0].Days) = %d, want %d", len(sched.Entries[0].Days), len(wantDays))
	}
	for i, d := range wantDays {
		if sched.Entries[0].Days[i] != d {
			t.Errorf("Entries[0].Days[%d] = %q, want %q", i, sched.Entries[0].Days[i], d)
		}
	}
	if sched.Entries[0].Start != "08:00" {
		t.Errorf("Entries[0].Start = %q, want %q", sched.Entries[0].Start, "08:00")
	}
	if sched.Entries[0].End != "18:00" {
		t.Errorf("Entries[0].End = %q, want %q", sched.Entries[0].End, "18:00")
	}

	wantWeekendDays := []string{"sat", "sun"}
	if len(sched.Entries[1].Days) != len(wantWeekendDays) {
		t.Fatalf("len(Entries[1].Days) = %d, want %d", len(sched.Entries[1].Days), len(wantWeekendDays))
	}
	for i, d := range wantWeekendDays {
		if sched.Entries[1].Days[i] != d {
			t.Errorf("Entries[1].Days[%d] = %q, want %q", i, sched.Entries[1].Days[i], d)
		}
	}
}

func TestParseConfig_PolicyParsing(t *testing.T) {
	yaml := `
adapter:
  type: memory
services:
  - name: worker
    identifier: worker
    metrics:
      - name: queue_time
        source: otlp
        metric: http.queue_time
      - name: queue_depth
        source: otlp
        metric: sqs.queue_depth
    policies:
      - type: window
        metric: queue_time
        upscale:
          threshold: 50
          sustained_for: 20s
          step: 2
        downscale:
          threshold: 25
          sustained_for: 600s
          step: 1
      - type: target
        expression: "ceil(queue_depth / 10)"
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	policies := cfg.Services[0].Policies
	if len(policies) != 2 {
		t.Fatalf("len(Policies) = %d, want 2", len(policies))
	}

	wp := policies[0]
	if wp.Type != "window" {
		t.Errorf("policies[0].Type = %q, want %q", wp.Type, "window")
	}
	if wp.Metric != "queue_time" {
		t.Errorf("policies[0].Metric = %q, want %q", wp.Metric, "queue_time")
	}
	if wp.Upscale.Threshold != 50.0 {
		t.Errorf("Upscale.Threshold = %v, want %v", wp.Upscale.Threshold, 50.0)
	}
	if wp.Upscale.SustainedFor != 20*time.Second {
		t.Errorf("Upscale.SustainedFor = %v, want %v", wp.Upscale.SustainedFor, 20*time.Second)
	}
	if wp.Upscale.Step != 2 {
		t.Errorf("Upscale.Step = %d, want 2", wp.Upscale.Step)
	}
	if wp.Downscale.Threshold != 25.0 {
		t.Errorf("Downscale.Threshold = %v, want %v", wp.Downscale.Threshold, 25.0)
	}
	if wp.Downscale.SustainedFor != 600*time.Second {
		t.Errorf("Downscale.SustainedFor = %v, want %v", wp.Downscale.SustainedFor, 600*time.Second)
	}
	if wp.Downscale.Step != 1 {
		t.Errorf("Downscale.Step = %d, want 1", wp.Downscale.Step)
	}

	tp := policies[1]
	if tp.Type != "target" {
		t.Errorf("policies[1].Type = %q, want %q", tp.Type, "target")
	}
	if tp.Expression != "ceil(queue_depth / 10)" {
		t.Errorf("policies[1].Expression = %q, want %q", tp.Expression, "ceil(queue_depth / 10)")
	}
}

func TestWindowConfigurationValidation(t *testing.T) {
	base := func(policyBody string) string {
		return `
adapter:
  type: memory
services:
  - name: worker
    identifier: worker
    metrics:
      - name: qd
        source: otlp
        metric: raw.qd
    policies:
` + policyBody
	}
	cases := []struct {
		name, policy, want string
	}{
		{name: "zero bucket", policy: "      - type: window\n        metric: qd\n        bucket_duration: 0s\n        upscale: {threshold: 1, sustained_for: 10s, step: 1}\n", want: "bucket_duration must be positive"},
		{name: "negative bucket", policy: "      - type: window\n        metric: qd\n        bucket_duration: -10s\n        upscale: {threshold: 1, sustained_for: 10s, step: 1}\n", want: "bucket_duration must be positive"},
		{name: "zero sustained", policy: "      - type: window\n        metric: qd\n        upscale: {threshold: 1, sustained_for: 0s, step: 1}\n", want: "sustained_for must be positive"},
		{name: "not divisible", policy: "      - type: window\n        metric: qd\n        bucket_duration: 10s\n        upscale: {threshold: 1, sustained_for: 15s, step: 1}\n", want: "exact multiple"},
		{name: "no direction", policy: "      - type: window\n        metric: qd\n", want: "requires upscale or downscale"},
		{name: "nonpositive step", policy: "      - type: window\n        metric: qd\n        upscale: {threshold: 1, sustained_for: 10s, step: 0}\n", want: "step must be positive"},
		{name: "unbound metric", policy: "      - type: window\n        metric: busy\n        upscale: {threshold: 1, sustained_for: 10s, step: 1}\n", want: "does not match a service binding"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(base(tc.policy)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestMetricBindingValidation(t *testing.T) {
	cases := []struct{ name, binding, want string }{
		{name: "otlp metric required", binding: "      - name: qd\n        source: otlp\n", want: "metric is required for otlp source"},
		{name: "prometheus query required", binding: "      - name: qd\n        source: prometheus.primary\n", want: "query is required for prometheus source"},
		{name: "source required", binding: "      - name: qd\n", want: "unknown source"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			yaml := `
metrics:
  prometheus:
    - name: primary
      address: http://prometheus:9090
adapter:
  type: memory
services:
  - name: worker
    identifier: worker
    metrics:
` + tc.binding + `    policies:
      - type: target
        expression: "1"
`
			_, err := Parse([]byte(yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want=%q", err, tc.want)
			}
		})
	}
}

func TestWindowBucketDefaultsAndRetention(t *testing.T) {
	yaml := `
adapter:
  type: memory
services:
  - name: worker
    identifier: worker
    metrics:
      - name: qd
        source: otlp
        metric: raw.qd
    policies:
      - type: window
        metric: qd
        upscale: {threshold: 0.5, sustained_for: 20s, step: 1}
      - type: window
        metric: qd
        bucket_duration: 5s
        downscale: {threshold: 0.5, sustained_for: 60s, step: 1}
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if *cfg.Services[0].Policies[0].BucketDuration != 10*time.Second || *cfg.Services[0].Policies[1].BucketDuration != 5*time.Second {
		t.Fatalf("bucket defaults not applied: %+v", cfg.Services[0].Policies)
	}
	if got := MetricRetention(cfg); got != 70*time.Second {
		t.Fatalf("retention=%v want=70s", got)
	}
}
