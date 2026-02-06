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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// CLI flag overrides config
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

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		cancel()
	}()

	// Initialize metric store
	store := metrics.NewMetricStore()

	// Initialize metric sources
	if cfg.Metrics.OTLP.Enabled {
		otlpReceiver := metrics.NewOTLPReceiver(
			cfg.Metrics.OTLP.GRPCPort,
			cfg.Metrics.OTLP.HTTPPort,
			store,
		)
		if err := otlpReceiver.Start(ctx); err != nil {
			return err
		}
		defer func() { _ = otlpReceiver.Stop(context.Background()) }()
		slog.Info("OTLP receiver started",
			"grpc_port", cfg.Metrics.OTLP.GRPCPort,
			"http_port", cfg.Metrics.OTLP.HTTPPort)
	}

	promSources := make(map[string]*metrics.PrometheusSource)
	for _, pc := range cfg.Metrics.Prometheus {
		source, err := metrics.NewPrometheusSource(pc.Name, pc.Address, pc.BearerTokenFile)
		if err != nil {
			return err
		}
		promSources[pc.Name] = source
		slog.Info("prometheus source configured", "name", pc.Name, "address", pc.Address)
	}

	// Initialize adapter
	adp, err := adapter.New(cfg.Adapter)
	if err != nil {
		return err
	}
	slog.Info("adapter initialized", "type", cfg.Adapter.Type)

	// Build services with compiled policies
	services, err := config.BuildServices(cfg, promSources, store)
	if err != nil {
		return err
	}

	// Initialize reconciler
	rec := reconciler.New(adp, services, store, promSources, reconciler.Options{
		Interval: cfg.ReconcileInterval,
		DryRun:   cfg.DryRun,
	})

	if cfg.DryRun {
		slog.Info("dry-run mode enabled, scaling actions will be logged but not executed")
	}

	// Initialize HTTP server
	srv := server.New(adp, cfg.Server.Address)
	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "error", err)
		}
	}()
	defer func() { _ = srv.Stop(context.Background()) }()
	slog.Info("HTTP server started", "address", cfg.Server.Address)

	// Run reconciler
	slog.Info("starting reconciler", "interval", cfg.ReconcileInterval)
	return rec.Run(ctx)
}
