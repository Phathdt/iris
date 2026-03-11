package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"iris/internal/dlq"
	"iris/internal/sink"
	postgresSource "iris/internal/source/postgres"
	nopTransform "iris/internal/transform/nop"
	wasmTransform "iris/internal/transform/wasm"
	"iris/pkg/cdc"
	"iris/pkg/config"
	"iris/pkg/logger"
	"iris/pkg/observability"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Pipeline wires all CDC components together
type Pipeline struct {
	source      cdc.Source
	decoder     cdc.Decoder
	transform   cdc.Transform
	sink        cdc.Sink
	dlq         *dlq.DLQ
	maxAttempts int
	backoffMs   int
	logger      logger.Logger
	metrics     observability.Metrics
	tracer      trace.Tracer
}

// NewPipeline creates a new pipeline with all components wired together
func NewPipeline(cfg config.Config, log logger.Logger, metrics observability.Metrics) (*Pipeline, error) {
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

	// 4. Create sink using factory
	factory := sink.NewFactory()
	snk, err := factory.CreateSink(sink.Config{
		Type:            cfg.Sink.Type,
		Addr:            cfg.Sink.Addr,
		Password:        cfg.Sink.Password,
		DB:              cfg.Sink.DB,
		Key:             cfg.Sink.Key,
		TableStreamMap:  cfg.Mapping.TableStreamMap,
		MaxLen:          cfg.Sink.MaxLen,
		ApproximateTrim: cfg.Sink.ApproximateTrim,
		Brokers:         cfg.Sink.Brokers,
		TableTopicMap:   cfg.Mapping.TableStreamMap,
	})
	if err != nil {
		return nil, fmt.Errorf("create sink: %w", err)
	}

	// 5. Create DLQ sink if enabled
	var dlqSink *dlq.DLQ
	if cfg.DLQ != nil && cfg.DLQ.Enabled {
		dlqRawSink, err := factory.CreateSink(sink.Config{
			Type:            cfg.DLQ.Sink.Type,
			Addr:            cfg.DLQ.Sink.Addr,
			Password:        cfg.DLQ.Sink.Password,
			DB:              cfg.DLQ.Sink.DB,
			Key:             cfg.DLQ.Sink.Key,
			MaxLen:          cfg.DLQ.Sink.MaxLen,
			ApproximateTrim: cfg.DLQ.Sink.ApproximateTrim,
			Brokers:         cfg.DLQ.Sink.Brokers,
		})
		if err != nil {
			return nil, fmt.Errorf("create dlq sink: %w", err)
		}
		dlqSink = dlq.NewDLQ(dlqRawSink)
		logger.Info("DLQ enabled", "type", cfg.DLQ.Sink.Type)
	}

	logger.Info("pipeline created successfully",
		"source", cfg.Source.Type,
		"transform", cfg.Transform != nil && cfg.Transform.Enabled,
		"sink", cfg.Sink.Type,
	)

	// Apply defaults for retry config (in case Validate() was not called)
	maxAttempts := cfg.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoffMs := cfg.Retry.BackoffMs
	if backoffMs <= 0 {
		backoffMs = 100
	}

	return &Pipeline{
		source:      src,
		decoder:     dec,
		transform:   tf,
		sink:        snk,
		dlq:         dlqSink,
		maxAttempts: maxAttempts,
		backoffMs:   backoffMs,
		logger:      logger,
		metrics:     metrics,
		tracer:      otel.Tracer("iris.pipeline"),
	}, nil
}

// Run starts the CDC pipeline: Source -> Decoder -> Transform -> Sink
func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.Info("starting CDC pipeline")

	// Start source and get raw event channel
	rawEvents, err := p.source.Start(ctx)
	if err != nil {
		return fmt.Errorf("start source: %w", err)
	}

	p.logger.Info("replication started, processing events")

	// Process events loop
	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("context cancelled, stopping pipeline")
			loopErr = ctx.Err()
			break loop
		case raw, ok := <-rawEvents:
			if !ok {
				p.logger.Info("source channel closed")
				break loop
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

			// Record replication lag from event timestamp
			if !event.TS.IsZero() {
				lag := time.Since(event.TS).Seconds()
				p.metrics.SetReplicationLag(lag)
			}

			// Start root trace span for this event
			eventCtx, eventSpan := p.tracer.Start(ctx, "pipeline.process_event",
				trace.WithAttributes(
					attribute.String("table", event.Table),
					attribute.String("op", string(event.Op)),
				),
			)

			// 2. Transform with retry (time first attempt only)
			var transformed *cdc.Event
			var lastErr error
			_, tSpan := p.tracer.Start(eventCtx, "transform.process")
			for attempt := 1; attempt <= p.maxAttempts; attempt++ {
				transformStart := time.Now()
				transformed, lastErr = p.transform.Process(event)
				if attempt == 1 {
					p.metrics.ObserveTransformDuration(time.Since(transformStart).Seconds())
				}
				if lastErr == nil {
					break
				}
				p.logger.Warn("transform error", "error", lastErr, "attempt", attempt)
				if attempt < p.maxAttempts {
					if err := p.sleepWithContext(ctx, p.backoffMs); err != nil {
						tSpan.End()
						eventSpan.End()
						loopErr = ctx.Err()
						break loop
					}
				}
			}
			if lastErr != nil {
				tSpan.RecordError(lastErr)
				tSpan.SetStatus(codes.Error, lastErr.Error())
				tSpan.End()
				p.metrics.IncPipelineErrors("transform", "retry_exhausted")
				eventSpan.RecordError(lastErr)
				eventSpan.SetStatus(codes.Error, lastErr.Error())
				eventSpan.End()
				p.sendToDLQ(ctx, event, lastErr, "transform")
				continue
			}
			tSpan.End()
			if transformed == nil {
				p.logger.Debug("event dropped by transform")
				eventSpan.End()
				continue
			}

			// 3. Write to sink with retry (time first attempt only)
			lastErr = nil
			sinkCtx, sSpan := p.tracer.Start(eventCtx, "sink.write")
			for attempt := 1; attempt <= p.maxAttempts; attempt++ {
				sinkStart := time.Now()
				lastErr = p.sink.Write(sinkCtx, transformed)
				if attempt == 1 {
					p.metrics.ObserveSinkWriteDuration(time.Since(sinkStart).Seconds())
				}
				if lastErr == nil {
					break
				}
				p.logger.Warn("sink write error", "error", lastErr, "attempt", attempt)
				if attempt < p.maxAttempts {
					if err := p.sleepWithContext(ctx, p.backoffMs); err != nil {
						sSpan.End()
						eventSpan.End()
						loopErr = ctx.Err()
						break loop
					}
				}
			}
			if lastErr != nil {
				sSpan.RecordError(lastErr)
				sSpan.SetStatus(codes.Error, lastErr.Error())
				sSpan.End()
				p.metrics.IncPipelineErrors("sink", "retry_exhausted")
				eventSpan.RecordError(lastErr)
				eventSpan.SetStatus(codes.Error, lastErr.Error())
				eventSpan.End()
				p.sendToDLQ(ctx, event, lastErr, "sink")
				continue
			}
			sSpan.End()

			// 4. Ack offset to source (advances PG replication slot's confirmed_flush_lsn).
			//    PG persists this server-side, so no external offset store needed for PG sources.
			//    Note: Offset is only acked after successful sink write. Events routed to DLQ
			//    or dropped after retry exhaustion are NOT acked, so PG will re-deliver on restart.
			if event.Offset != nil {
				if err := p.source.Ack(*event.Offset); err != nil {
					p.logger.Warn("ack error", "error", err)
				}
			}

			// Record successful event processing
			p.metrics.IncEventsProcessed(event.Table, string(event.Op), "success")
			eventSpan.End()

			p.logger.Debug("event written to sink", "table", event.Table, "op", event.Op)
		}
	} // end loop

	return loopErr
}

// sendToDLQ sends a failed event to the DLQ sink, or logs and drops if DLQ is disabled
func (p *Pipeline) sendToDLQ(ctx context.Context, event *cdc.Event, err error, stage string) {
	p.metrics.IncPipelineErrors(stage, "dlq")
	if p.dlq != nil {
		if dlqErr := p.dlq.Send(ctx, event, err, stage, p.maxAttempts); dlqErr != nil {
			p.logger.Error("DLQ write failed, dropping event",
				"stage", stage, "original_error", err, "dlq_error", dlqErr,
				"table", event.Table, "op", event.Op,
			)
			return
		}
		p.logger.Warn("event sent to DLQ",
			"stage", stage, "error", err,
			"table", event.Table, "op", event.Op, "attempts", p.maxAttempts,
		)
		return
	}
	p.logger.Error("event failed after retries, dropping",
		"stage", stage, "error", err,
		"table", event.Table, "op", event.Op, "attempts", p.maxAttempts,
	)
}

// sleepWithContext sleeps for the given duration but returns early if context is cancelled
func (p *Pipeline) sleepWithContext(ctx context.Context, ms int) error {
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
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

	if p.dlq != nil {
		if err := p.dlq.Close(); err != nil {
			errs = append(errs, fmt.Errorf("dlq close: %w", err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	p.logger.Info("pipeline closed successfully")
	return nil
}
