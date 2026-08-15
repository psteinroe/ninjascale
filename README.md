<div align="center">
  <img src="docs/logo.png" alt="ninjascale" width="150">

  # ninjascale
  otel-autoscaler for the rest of us.
</div>

ninjascale is a metrics-based autoscaler for managed container platforms. It scales your services based on OTLP and Prometheus metrics. Define threshold or expression-based policies, add time-based schedules, and let it handle the rest.

## Features

- **Target policies** - Expression-based scaling (e.g. `ceil(queue_depth / 10)`)
- **Window policies** - Freshness-aware thresholds over consecutive completed metric buckets
- **Metric sources** - OTLP (gRPC + HTTP) and Prometheus
- **Time-based schedules** - Override min/max bounds by day and time
- **Independent cooldowns** - Separate upscale/downscale cooldown timers
- **Dry-run mode** - Test in production without actually scaling

## Usage

```
ninjascale --config ninjascale.yaml
ninjascale --config ninjascale.yaml --dry-run
```

You can also provide the config via environment variables. `NINJASCALE_CONFIG_BASE64` takes precedence over `NINJASCALE_CONFIG`, and both override `--config`.

```
export NINJASCALE_CONFIG="$(cat ninjascale.yaml)"
ninjascale
```

```
export NINJASCALE_CONFIG_BASE64="$(base64 < ninjascale.yaml)"
ninjascale
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

    # Metrics bound to this service. Policies use the local `name`; OTLP sends
    # the raw `metric`. Bindings are service-scoped, so local names may be reused.
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
      # Window policy: evaluates epoch-aligned completed buckets and scales by
      # a fixed step. sustained_for must be a positive exact multiple of
      # bucket_duration (10s by default).
      - type: window
        metric: queue_time_ms
        bucket_duration: 10s
        upscale:
          threshold: 50          # every bucket's latest sample must be > 50
          sustained_for: 20s     # two consecutive completed 10s buckets
          step: 2                # add 2 instances per trigger
        downscale:
          threshold: 25          # every bucket's latest sample must be < 25
          sustained_for: 600s    # 60 consecutive completed 10s buckets
          step: 1                # remove 1 instance per trigger

      # Target policy: expression-based. Computes desired count directly from metrics.
      # Available functions: ceil, floor, max, min.
      - type: target
        expression: "ceil(queue_depth / 10)"
```

### Window semantics

Window buckets are epoch-aligned half-open intervals: `[start, start + bucket_duration)`. At `12:00:20`, the latest eligible 10-second bucket is `[12:00:10, 12:00:20)`; the bucket beginning at `12:00:20` is still open. If a bucket contains multiple observations, its representative value is the sample with the latest event timestamp.

Every required bucket must exist, be contiguous, end at the newest expected completed bucket, and be newer than the policy's last successful scale event. Samples are never carried forward. Missing, stale, partial, gapped, or pre-reset data cannot upscale and explicitly holds the current count when downscale is configured. Comparisons are strict: equality satisfies neither `>` upscale nor `<` downscale.

OTLP gauge and sum points use `TimeUnixNano`; a zero timestamp uses receiver receipt time. Empty or unsupported OTLP payloads and empty Prometheus vectors remain missing. Prometheus queries must return exactly one vector series (or one scalar), and preserve the result timestamp. Target policies are intentionally different: they continue to use the latest available sample without completed-window freshness checks.

Metric bucket history is service-scoped and retained for the largest configured sustained window plus two bucket durations; direct store users default to 30 seconds. One latest sample per series remains available for target policies after older bucket history expires. The store also enforces hard limits of 10,000 samples per series and 100,000 samples total, evicting the oldest event-time samples first. An eviction can only make a window incomplete, so scaling fails closed rather than risking unbounded memory. Successful scaling records a policy-local cutoff rather than deleting shared history, so a fresh post-scale window is required in addition to normal cooldowns.

Window diagnostics are exposed as `ninjascale_metric_age_seconds`, `ninjascale_window_complete_buckets`, and `ninjascale_window_evaluations_total`.

## Build

```
go build -o ninjascale ./cmd/ninjascale
```

## Docker

```
docker build -t ninjascale .
docker run -v $(pwd)/ninjascale.yaml:/etc/ninjascale/config.yaml ninjascale --config /etc/ninjascale/config.yaml
```
