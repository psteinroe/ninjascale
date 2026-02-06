<div align="center">
  <img src="docs/logo.png" alt="ninjascale" width="300">

  # ninjascale
  otel-autoscaler for the rest of us.
</div>

## Features

- **Target policies** - Expression-based scaling (e.g. `ceil(queue_depth / 10)`)
- **Window policies** - Threshold-based with sustained duration requirements
- **Metric sources** - OTLP (gRPC + HTTP) and Prometheus
- **Time-based schedules** - Override min/max bounds by day and time
- **Scale-to-zero** - With fast wake-up (bypasses upscale cooldown)
- **Independent cooldowns** - Separate upscale/downscale cooldown timers
- **Dry-run mode** - Test in production without actually scaling

## Usage

```
ninjascale --config ninjascale.yaml
ninjascale --config ninjascale.yaml --dry-run
```

## Configuration

```yaml
# How often ninjascale evaluates scaling policies.
reconcile_interval: 10s

# Enable to log scaling decisions without executing them.
# Can also be set via --dry-run CLI flag.
# dry_run: true

# Defaults applied to all services unless overridden per service.
defaults:
  min_count: 1
  max_count: 10
  cooldown:
    # Minimum time between consecutive scale-up actions.
    upscale: 60s
    # Minimum time between consecutive scale-down actions.
    downscale: 600s

# Metric sources ninjascale can pull data from.
metrics:
  # OTLP receiver accepts metrics pushed via gRPC or HTTP.
  otlp:
    enabled: true
    grpc_port: 4317
    http_port: 4318

  # Prometheus sources are queried via PromQL each reconcile tick.
  # Multiple sources can be configured.
  prometheus:
    - name: primary
      address: "http://prometheus:9090"
      # bearer_token_file: /var/run/secrets/token  # optional auth

# Adapter controls which infrastructure ninjascale manages.
adapter:
  # Supported types: ecs, memory (for testing).
  type: ecs
  ecs:
    region: eu-central-1
    cluster: production

# HTTP server exposes /healthz, /readyz, and /metrics (Prometheus format).
server:
  address: ":8080"

services:
  - name: api-worker
    # Identifier used by the adapter (e.g. ECS service name).
    identifier: api-worker-service

    # Per-service overrides for min/max and cooldowns.
    min_count: 2
    max_count: 20
    cooldown:
      upscale: 30s
      downscale: 300s

    # Time-based schedule overrides for min/max bounds.
    # Useful for reducing capacity during off-hours or weekends.
    schedule:
      timezone: "Europe/Berlin"
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

    # Metrics bound to this service. Each metric gets a name used in policies.
    # source is either "otlp" or "prometheus.<name>" referencing a source above.
    metrics:
      - name: queue_depth
        source: prometheus.primary
        query: "sum(sqs_queue_depth{queue='api-jobs'})"
      - name: queue_time_ms
        source: otlp
        metric: http.server.request.queue_time

    # Policies determine the desired instance count.
    # Multiple policies: upscale takes the highest, downscale takes the least aggressive.
    policies:
      # Window policy: triggers when a metric breaches a threshold for a sustained duration.
      # Scales by a fixed step size. Good for latency-based signals.
      - type: window
        metric: queue_time_ms
        upscale:
          threshold: 50          # scale up when metric > 50
          sustained_for: 10s     # ... for at least 10 seconds
          step: 2                # add 2 instances per trigger
        downscale:
          threshold: 25          # scale down when metric < 25
          sustained_for: 600s    # ... for at least 10 minutes
          step: 1                # remove 1 instance per trigger

      # Target policy: expression-based. Computes desired count directly from metrics.
      # Available functions: ceil, floor, max, min.
      - type: target
        expression: "ceil(queue_depth / 10)"
```

## Build

```
go build -o ninjascale ./cmd/ninjascale
```

## Docker

```
docker build -t ninjascale .
docker run -v $(pwd)/ninjascale.yaml:/etc/ninjascale/config.yaml ninjascale --config /etc/ninjascale/config.yaml
```
