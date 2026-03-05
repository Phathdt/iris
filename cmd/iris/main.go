package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"iris/internal/pipeline"
	"iris/pkg/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	flag.Parse()

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	logger.Info("loading configuration", "path", *configPath)
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger.Info("configuration loaded successfully")

	// Create pipeline
	logger.Info("creating pipeline")
	pl, err := pipeline.NewPipeline(*cfg)
	if err != nil {
		return fmt.Errorf("create pipeline: %w", err)
	}

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Handle shutdown signals in background
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run pipeline
	logger.Info("starting iris CDC pipeline")
	if err := pl.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("run pipeline: %w", err)
	}

	// Graceful shutdown
	logger.Info("shutting down pipeline")
	if err := pl.Close(); err != nil {
		return fmt.Errorf("close pipeline: %w", err)
	}

	logger.Info("iris shutdown complete")
	return nil
}
