package nop

import "iris/pkg/cdc"

// NoOpTransform is a passthrough transform that returns events unchanged.
// Useful for pipelines that don't need transformation.
type NoOpTransform struct{}

// NewNoOp creates a new NoOpTransform.
func NewNoOp() *NoOpTransform {
	return &NoOpTransform{}
}

// Process returns the event unchanged.
func (t *NoOpTransform) Process(event *cdc.Event) (*cdc.Event, error) {
	return event, nil
}

// Close is a no-op.
func (t *NoOpTransform) Close() error {
	return nil
}

// Ensure NoOpTransform implements cdc.Transform.
var _ cdc.Transform = (*NoOpTransform)(nil)
