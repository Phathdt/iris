package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"iris/internal/pipeline"
	"iris/pkg/config"

	"github.com/urfave/cli/v2"
)

func main() {
	cli.VersionPrinter = func(_ *cli.Context) {
		config.PrintVersion()
	}

	app := &cli.App{
		Name:    "iris",
		Usage:   "PostgreSQL to Redis CDC pipeline",
		Version: config.Version(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "Configuration file path",
			},
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "Enable verbose logging",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "run",
				Aliases: []string{"r"},
				Usage:   "Run the CDC pipeline",
				Action:  runPipeline,
			},
		},
		Action: runPipeline,
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func runPipeline(c *cli.Context) error {
	configPath := c.String("config")
	verbose := c.Bool("verbose")

	// Create logger
	logLevel := slog.LevelInfo
	if verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Load configuration
	logger.Info("loading configuration", "path", configPath)
	cfg, err := config.Load(configPath)
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
