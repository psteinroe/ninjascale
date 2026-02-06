package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/psteinroe/ninjascale/internal/metrics"
	"github.com/psteinroe/ninjascale/internal/policy"
	"github.com/psteinroe/ninjascale/internal/schedule"
)

const (
	EnvConfigYAML       = "NINJASCALE_CONFIG"
	EnvConfigYAMLBase64 = "NINJASCALE_CONFIG_BASE64"
)

// Config represents the root configuration.
type Config struct {
	ReconcileInterval time.Duration   `yaml:"reconcile_interval"`
	DryRun            bool            `yaml:"dry_run"`
	Defaults          DefaultsConfig  `yaml:"defaults"`
	Metrics           MetricsConfig   `yaml:"metrics"`
	Adapter           AdapterConfig   `yaml:"adapter"`
	Server            ServerConfig    `yaml:"server"`
	Services          []ServiceConfig `yaml:"services"`
}

// DefaultsConfig holds default values for services.
type DefaultsConfig struct {
	MinCount int            `yaml:"min_count"`
	MaxCount int            `yaml:"max_count"`
	Cooldown CooldownConfig `yaml:"cooldown"`
}

// CooldownConfig holds cooldown settings.
type CooldownConfig struct {
	Upscale   time.Duration `yaml:"upscale"`
	Downscale time.Duration `yaml:"downscale"`
}

// MetricsConfig holds metrics source configuration.
type MetricsConfig struct {
	OTLP       OTLPConfig         `yaml:"otlp"`
	Prometheus []PrometheusConfig `yaml:"prometheus"`
}

// OTLPConfig holds OTLP receiver configuration.
type OTLPConfig struct {
	Enabled  bool `yaml:"enabled"`
	GRPCPort int  `yaml:"grpc_port"`
	HTTPPort int  `yaml:"http_port"`
}

// PrometheusConfig holds Prometheus source configuration.
type PrometheusConfig struct {
	Name            string `yaml:"name"`
	Address         string `yaml:"address"`
	BearerTokenFile string `yaml:"bearer_token_file"`
}

// AdapterConfig holds adapter configuration.
type AdapterConfig struct {
	Type string    `yaml:"type"`
	ECS  ECSConfig `yaml:"ecs"`
}

// ECSConfig holds ECS adapter configuration.
type ECSConfig struct {
	Region  string `yaml:"region"`
	Cluster string `yaml:"cluster"`
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Address string `yaml:"address"`
}

// ServiceConfig holds service configuration.
type ServiceConfig struct {
	Name       string                `yaml:"name"`
	Identifier string                `yaml:"identifier"`
	MinCount   *int                  `yaml:"min_count"`
	MaxCount   *int                  `yaml:"max_count"`
	Cooldown   *CooldownConfig       `yaml:"cooldown"`
	Schedule   *ScheduleConfig       `yaml:"schedule"`
	Metrics    []MetricBindingConfig `yaml:"metrics"`
	Policies   []PolicyConfig        `yaml:"policies"`
}

// ScheduleConfig holds schedule configuration.
type ScheduleConfig struct {
	Timezone string                `yaml:"timezone"`
	Entries  []ScheduleEntryConfig `yaml:"entries"`
}

// ScheduleEntryConfig holds a schedule entry.
type ScheduleEntryConfig struct {
	Days     []string `yaml:"days"`
	Start    string   `yaml:"start"`
	End      string   `yaml:"end"`
	MinCount int      `yaml:"min_count"`
	MaxCount int      `yaml:"max_count"`
}

// MetricBindingConfig holds metric binding configuration.
type MetricBindingConfig struct {
	Name   string `yaml:"name"`
	Source string `yaml:"source"`
	Query  string `yaml:"query"`  // for prometheus
	Metric string `yaml:"metric"` // for otlp
}

// PolicyConfig holds policy configuration.
type PolicyConfig struct {
	Type       string           `yaml:"type"`
	Metric     string           `yaml:"metric"`
	Expression string           `yaml:"expression"`
	Upscale    *ThresholdConfig `yaml:"upscale"`
	Downscale  *ThresholdConfig `yaml:"downscale"`
}

// ThresholdConfig holds threshold configuration for window policies.
type ThresholdConfig struct {
	Threshold    float64       `yaml:"threshold"`
	SustainedFor time.Duration `yaml:"sustained_for"`
	Step         int           `yaml:"step"`
}

// LoadFile loads configuration from a file.
func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return Parse(data)
}

// LoadFromEnvOrFile loads configuration from environment variables or a file.
// Precedence: NINJASCALE_CONFIG_BASE64, then NINJASCALE_CONFIG, then file path.
func LoadFromEnvOrFile(path string) (*Config, error) {
	if encoded := strings.TrimSpace(os.Getenv(EnvConfigYAMLBase64)); encoded != "" {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", EnvConfigYAMLBase64, err)
		}
		return Parse(data)
	}
	if raw := strings.TrimSpace(os.Getenv(EnvConfigYAML)); raw != "" {
		return Parse([]byte(raw))
	}
	return LoadFile(path)
}

// Parse parses configuration from YAML bytes.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 10 * time.Second
	}
	if cfg.Defaults.MinCount == 0 {
		cfg.Defaults.MinCount = 1
	}
	if cfg.Defaults.MaxCount == 0 {
		cfg.Defaults.MaxCount = 10
	}
	if cfg.Defaults.Cooldown.Upscale == 0 {
		cfg.Defaults.Cooldown.Upscale = 60 * time.Second
	}
	if cfg.Defaults.Cooldown.Downscale == 0 {
		cfg.Defaults.Cooldown.Downscale = 600 * time.Second
	}
	if cfg.Server.Address == "" {
		cfg.Server.Address = ":8080"
	}
	if cfg.Metrics.OTLP.GRPCPort == 0 {
		cfg.Metrics.OTLP.GRPCPort = 4317
	}
	if cfg.Metrics.OTLP.HTTPPort == 0 {
		cfg.Metrics.OTLP.HTTPPort = 4318
	}

	// Apply defaults to services
	for i := range cfg.Services {
		svc := &cfg.Services[i]
		if svc.MinCount == nil {
			svc.MinCount = &cfg.Defaults.MinCount
		}
		if svc.MaxCount == nil {
			svc.MaxCount = &cfg.Defaults.MaxCount
		}
		if svc.Cooldown == nil {
			svc.Cooldown = &cfg.Defaults.Cooldown
		}
	}
}

func validate(cfg *Config) error {
	if cfg.Adapter.Type == "" {
		return fmt.Errorf("adapter.type is required")
	}

	validAdapters := map[string]bool{"ecs": true, "memory": true}
	if !validAdapters[cfg.Adapter.Type] {
		return fmt.Errorf("unknown adapter type: %s", cfg.Adapter.Type)
	}

	if cfg.Adapter.Type == "ecs" {
		if cfg.Adapter.ECS.Region == "" {
			return fmt.Errorf("adapter.ecs.region is required")
		}
		if cfg.Adapter.ECS.Cluster == "" {
			return fmt.Errorf("adapter.ecs.cluster is required")
		}
	}

	if len(cfg.Services) == 0 {
		return fmt.Errorf("at least one service is required")
	}

	// Build map of valid prometheus sources
	promSources := make(map[string]bool)
	for i, pc := range cfg.Metrics.Prometheus {
		if pc.Name == "" {
			return fmt.Errorf("metrics.prometheus[%d]: name is required", i)
		}
		if pc.Address == "" {
			return fmt.Errorf("metrics.prometheus[%d]: address is required", i)
		}
		if promSources[pc.Name] {
			return fmt.Errorf("metrics.prometheus: duplicate source name: %s", pc.Name)
		}
		promSources[pc.Name] = true
	}

	timeRegex := regexp.MustCompile(`^\d{2}:\d{2}$`)
	dayMap := map[string]time.Weekday{
		"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday,
		"wed": time.Wednesday, "thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
	}

	for i, svc := range cfg.Services {
		if svc.Name == "" {
			return fmt.Errorf("service[%d]: name is required", i)
		}
		if svc.Identifier == "" {
			return fmt.Errorf("service[%d]: identifier is required", i)
		}
		if len(svc.Policies) == 0 {
			return fmt.Errorf("service[%d]: at least one policy is required", i)
		}
		if *svc.MinCount < 0 {
			return fmt.Errorf("service[%d]: min_count cannot be negative", i)
		}
		if *svc.MinCount > *svc.MaxCount {
			return fmt.Errorf("service[%d]: min_count (%d) cannot exceed max_count (%d)", i, *svc.MinCount, *svc.MaxCount)
		}

		// Validate metrics
		for j, m := range svc.Metrics {
			if m.Source == "otlp" {
				continue
			}
			if len(m.Source) > 11 && m.Source[:11] == "prometheus." {
				srcName := m.Source[11:]
				if !promSources[srcName] {
					return fmt.Errorf("service[%d].metrics[%d]: unknown source: %s", i, j, m.Source)
				}
			} else {
				return fmt.Errorf("service[%d].metrics[%d]: unknown source: %s", i, j, m.Source)
			}
		}

		// Validate schedule
		if svc.Schedule != nil {
			if svc.Schedule.Timezone != "" {
				if _, err := time.LoadLocation(svc.Schedule.Timezone); err != nil {
					return fmt.Errorf("service[%d].schedule: unknown timezone: %s", i, svc.Schedule.Timezone)
				}
			}
			for j, entry := range svc.Schedule.Entries {
				if !timeRegex.MatchString(entry.Start) {
					return fmt.Errorf("service[%d].schedule.entries[%d]: invalid start time format, expected HH:MM", i, j)
				}
				if !timeRegex.MatchString(entry.End) {
					return fmt.Errorf("service[%d].schedule.entries[%d]: invalid end time format, expected HH:MM", i, j)
				}
				for _, d := range entry.Days {
					if _, ok := dayMap[d]; !ok {
						return fmt.Errorf("service[%d].schedule.entries[%d]: invalid day: %s", i, j, d)
					}
				}
			}
		}

		// Collect metric names for expression validation
		metricNames := make([]string, len(svc.Metrics))
		for j, m := range svc.Metrics {
			metricNames[j] = m.Name
		}

		// Validate policies
		for j, p := range svc.Policies {
			switch p.Type {
			case "window":
				if p.Metric == "" {
					return fmt.Errorf("service[%d].policies[%d]: metric is required for window policy", i, j)
				}
			case "target":
				if p.Expression == "" {
					return fmt.Errorf("service[%d].policies[%d]: expression is required for target policy", i, j)
				}
				// Try to compile expression with metric names from service config
				if _, err := policy.CompileExpression(p.Expression, metricNames); err != nil {
					return fmt.Errorf("service[%d].policies[%d]: invalid expression: %w", i, j, err)
				}
			default:
				return fmt.Errorf("service[%d].policies[%d]: unknown policy type: %s", i, j, p.Type)
			}
		}
	}

	return nil
}

// Service represents a runtime service with compiled policies.
type Service struct {
	Name       string
	Identifier string
	MinCount   int
	MaxCount   int
	Cooldown   CooldownConfig
	Schedule   *schedule.Schedule
	Metrics    []MetricBinding
	Policies   []policy.Policy
}

// MetricBinding represents a runtime metric binding.
type MetricBinding struct {
	Name   string
	Source string
	Query  string
	Metric string
}

// BuildServices converts config services to runtime services.
func BuildServices(cfg *Config, promSources map[string]*metrics.PrometheusSource, store *metrics.MetricStore) ([]*Service, error) {
	dayMap := map[string]time.Weekday{
		"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday,
		"wed": time.Wednesday, "thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
	}

	services := make([]*Service, len(cfg.Services))
	for i, sc := range cfg.Services {
		svc := &Service{
			Name:       sc.Name,
			Identifier: sc.Identifier,
			MinCount:   *sc.MinCount,
			MaxCount:   *sc.MaxCount,
			Cooldown:   *sc.Cooldown,
		}

		// Build metrics
		metricNames := make([]string, len(sc.Metrics))
		for j, mc := range sc.Metrics {
			svc.Metrics = append(svc.Metrics, MetricBinding(mc))
			metricNames[j] = mc.Name
		}

		// Build schedule
		if sc.Schedule != nil {
			tz := "UTC"
			if sc.Schedule.Timezone != "" {
				tz = sc.Schedule.Timezone
			}
			entries := make([]schedule.Entry, len(sc.Schedule.Entries))
			for j, e := range sc.Schedule.Entries {
				days := make([]time.Weekday, len(e.Days))
				for k, d := range e.Days {
					days[k] = dayMap[d]
				}
				entries[j] = schedule.Entry{
					Days:     days,
					Start:    e.Start,
					End:      e.End,
					MinCount: e.MinCount,
					MaxCount: e.MaxCount,
				}
			}
			svc.Schedule = &schedule.Schedule{
				Timezone: tz,
				Entries:  entries,
			}
		}

		// Build policies
		for _, pc := range sc.Policies {
			var p policy.Policy
			var err error

			switch pc.Type {
			case "window":
				wp := &policy.WindowPolicy{
					Metric: pc.Metric,
				}
				if pc.Upscale != nil {
					wp.Upscale = policy.WindowThreshold{
						Threshold:    pc.Upscale.Threshold,
						SustainedFor: pc.Upscale.SustainedFor,
						Step:         pc.Upscale.Step,
					}
					if wp.Upscale.Step == 0 {
						wp.Upscale.Step = 1
					}
				}
				if pc.Downscale != nil {
					wp.Downscale = policy.WindowThreshold{
						Threshold:    pc.Downscale.Threshold,
						SustainedFor: pc.Downscale.SustainedFor,
						Step:         pc.Downscale.Step,
					}
					if wp.Downscale.Step == 0 {
						wp.Downscale.Step = 1
					}
				}
				p = wp

			case "target":
				p, err = policy.NewTargetPolicy(pc.Expression, metricNames)
				if err != nil {
					return nil, fmt.Errorf("compile target policy for %s: %w", sc.Name, err)
				}
			}

			svc.Policies = append(svc.Policies, p)
		}

		services[i] = svc
	}

	return services, nil
}
