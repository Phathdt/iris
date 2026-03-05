package pipeline

import (
	"context"
	"errors"
	"fmt"

	"iris/internal/encoder"
	postgresSource "iris/internal/source/postgres"
	wasmTransform "iris/internal/transform/wasm"
	"iris/pkg/cdc"
	"iris/pkg/config"
	"iris/pkg/logger"

	redisSink "iris/internal/sink/redis"
	nopTransform "iris/internal/transform/nop"
)

// Pipeline wires all CDC components together
type Pipeline struct {
	source    cdc.Source
	decoder   cdc.Decoder
	transform cdc.Transform
	encoder   cdc.Encoder
	sink      cdc.Sink
	logger    logger.Logger
}

// NewPipeline creates a new pipeline with all components wired together
func NewPipeline(cfg config.Config, log logger.Logger) (*Pipeline, error) {
	// Create logger with pipeline group
	logger := log.WithGroup("pipeline")

	// 1. Create source
	src, err := postgresSource.NewSource(postgresSource.Config{
		DSN:      cfg.Source.DSN,
		Tables:   cfg.Source.Tables,
		SlotName: cfg.Source.SlotName,
	})
	if err != nil {
		return nil, fmt.Errorf("create source: %w", err)
	}

	// 2. Create decoder
	dec := postgresSource.NewDecoder()

	// 3. Create transform (optional)
	var tf cdc.Transform = nopTransform.NewNoOp()
	if cfg.Transform != nil && cfg.Transform.Enabled {
		logger.Info("creating WASM transform", "path", cfg.Transform.Path)
		tf, err = wasmTransform.NewWASM(context.Background(), wasmTransform.Config{
			Path:              cfg.Transform.Path,
			FunctionName:      cfg.Transform.FunctionName,
			AllocFunctionName: cfg.Transform.AllocFunctionName,
			EnableLogging:     cfg.Transform.EnableLogging,
		})
		if err != nil {
			return nil, fmt.Errorf("create transform: %w", err)
		}
	}

	// 4. Create encoder
	enc := encoder.NewJSONEncoder()

	// 5. Create sink
	snk, err := redisSink.NewRedisSink(redisSink.Config{
		Addr:     cfg.Sink.Addr,
		Password: cfg.Sink.Password,
		DB:       cfg.Sink.DB,
		Key:      cfg.Sink.Key,
		MaxLen:   cfg.Sink.MaxLen,
	})
	if err != nil {
		return nil, fmt.Errorf("create sink: %w", err)
	}

	logger.Info("pipeline created successfully",
		"source", cfg.Source.Type,
		"transform", cfg.Transform != nil && cfg.Transform.Enabled,
		"sink", cfg.Sink.Type,
	)

	return &Pipeline{
		source:    src,
		decoder:   dec,
		transform: tf,
		encoder:   enc,
		sink:      snk,
		logger:    logger,
	}, nil
}

// Run starts the CDC pipeline: Source -> Decoder -> Transform -> Encoder -> Sink
func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.Info("starting CDC pipeline")

	// Start source and get raw event channel
	rawEvents, err := p.source.Start(ctx)
	if err != nil {
		return fmt.Errorf("start source: %w", err)
	}

	p.logger.Info("replication started, processing events")

	// Process events loop
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("context cancelled, stopping pipeline")
			return ctx.Err()
		case raw, ok := <-rawEvents:
			if !ok {
				p.logger.Info("source channel closed")
				return nil
			}

			// Handle error events from source
			if errData, ok := raw.Data.(error); ok {
				p.logger.Error("source error", "error", errData)
				continue
			}

			// 1. Decode raw event to unified Event
			event, err := p.decoder.Decode(raw)
			if err != nil {
				p.logger.Warn("decode error", "error", err)
				continue
			}
			if event == nil {
				// Decoder returned nil (e.g., relation/commit messages)
				continue
			}

			p.logger.Debug("decoded event",
				"source", event.Source,
				"table", event.Table,
				"op", event.Op,
			)

			// 2. Transform (filter/enrich/route)
			transformed, err := p.transform.Process(event)
			if err != nil {
				p.logger.Warn("transform error", "error", err)
				continue
			}
			if transformed == nil {
				// Event dropped by transform
				p.logger.Debug("event dropped by transform")
				continue
			}

			// 3. Encode to output format
			data, err := p.encoder.Encode(transformed)
			if err != nil {
				p.logger.Warn("encode error", "error", err)
				continue
			}

			// 4. Write to sink
			if err := p.sink.Write(ctx, data); err != nil {
				p.logger.Error("sink write error", "error", err)
				continue
			}

			p.logger.Debug("event written to sink", "table", event.Table, "op", event.Op)
		}
	}
}

// Close cleans up all pipeline resources
func (p *Pipeline) Close() error {
	p.logger.Info("closing pipeline")

	var errs []error

	if err := p.source.Close(); err != nil {
		errs = append(errs, fmt.Errorf("source close: %w", err))
	}

	if err := p.transform.Close(); err != nil {
		errs = append(errs, fmt.Errorf("transform close: %w", err))
	}

	if err := p.sink.Close(); err != nil {
		errs = append(errs, fmt.Errorf("sink close: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	p.logger.Info("pipeline closed successfully")
	return nil
}
