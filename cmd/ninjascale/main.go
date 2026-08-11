package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/psteinroe/ninjascale/internal/adapter"
	"github.com/psteinroe/ninjascale/internal/config"
	"github.com/psteinroe/ninjascale/internal/metrics"
	"github.com/psteinroe/ninjascale/internal/reconciler"
	"github.com/psteinroe/ninjascale/internal/server"
)

func main() {
	configPath := flag.String("config", "ninjascale.yaml", "path to config file")
	dryRun := flag.Bool("dry-run", false, "run without executing scaling actions")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	cfg, err := config.LoadFromEnvOrFile(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if *dryRun {
		cfg.DryRun = true
	}
	if err := run(cfg); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		cancel()
	}()

	store := metrics.NewMetricStore(metrics.WithRetention(config.MetricRetention(cfg)))

	promSources := make(map[string]*metrics.PrometheusSource)
	for _, pc := range cfg.Metrics.Prometheus {
		source, err := metrics.NewPrometheusSource(pc.Name, pc.Address, pc.BearerTokenFile)
		if err != nil {
			return err
		}
		promSources[pc.Name] = source
		slog.Info("prometheus source configured", "name", pc.Name, "address", pc.Address)
	}

	adp, err := adapter.New(cfg.Adapter)
	if err != nil {
		return err
	}
	slog.Info("adapter initialized", "type", cfg.Adapter.Type)

	// Services and aliases are complete before the receiver can accept data.
	services, err := config.BuildServices(cfg, promSources, store)
	if err != nil {
		return err
	}
	if cfg.Metrics.OTLP.Enabled {
		receiver := metrics.NewOTLPReceiver(cfg.Metrics.OTLP.GRPCPort, cfg.Metrics.OTLP.HTTPPort, store)
		for _, svc := range services {
			for _, binding := range svc.Metrics {
				if binding.Source == "otlp" {
					receiver.RegisterBinding(binding.Metric, metrics.MetricKey{Service: svc.Name, Name: binding.Name})
				}
			}
		}
		if err := receiver.Start(ctx); err != nil {
			return err
		}
		defer func() { _ = receiver.Stop(context.Background()) }()
		slog.Info("OTLP receiver started", "grpc_port", cfg.Metrics.OTLP.GRPCPort, "http_port", cfg.Metrics.OTLP.HTTPPort)
	}

	rec := reconciler.New(adp, services, store, promSources, reconciler.Options{
		Interval: cfg.ReconcileInterval,
		DryRun:   cfg.DryRun,
	})
	if cfg.DryRun {
		slog.Info("dry-run mode enabled, scaling actions will be logged but not executed")
	}

	srv := server.New(adp, cfg.Server.Address)
	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "error", err)
		}
	}()
	defer func() { _ = srv.Stop(context.Background()) }()
	slog.Info("HTTP server started", "address", cfg.Server.Address)

	slog.Info("starting reconciler", "interval", cfg.ReconcileInterval)
	return rec.Run(ctx)
}
