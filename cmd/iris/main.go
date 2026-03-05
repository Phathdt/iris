package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"iris/internal/pipeline"
	"iris/pkg/config"
	"iris/pkg/logger"

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
				Usage: "Enable verbose logging (overrides config)",
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

	// Load configuration first
	tmpLog := logger.New("text", "info") // Temporary logger for config load
	tmpLog.Info("loading configuration", "path", configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Create logger with config (verbose flag overrides config level)
	logLevel := cfg.Logger.Level
	if verbose {
		logLevel = "debug"
	}
	log := logger.New(cfg.Logger.Format, logLevel)

	log.Info("configuration loaded successfully",
		"level", cfg.Logger.Level,
		"format", cfg.Logger.Format,
	)

	// Create pipeline
	log.Info("creating pipeline")
	pl, err := pipeline.NewPipeline(*cfg, log)
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
		log.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run pipeline
	log.Info("starting iris CDC pipeline")
	if err := pl.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("run pipeline: %w", err)
	}

	// Graceful shutdown
	log.Info("shutting down pipeline")
	if err := pl.Close(); err != nil {
		return fmt.Errorf("close pipeline: %w", err)
	}

	log.Info("iris shutdown complete")
	return nil
}
