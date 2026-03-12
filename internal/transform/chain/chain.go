package chain

import (
	"errors"
	"fmt"

	"iris/pkg/cdc"
)

// TransformChain applies multiple transforms sequentially.
// Each transform receives the output of the previous transform.
// If any transform returns nil (drop), the chain stops and event is dropped.
// If any transform returns error, the error propagates up.
type TransformChain struct {
	transforms []cdc.Transform
}

// NewTransformChain creates a new TransformChain from a list of transforms.
func NewTransformChain(transforms []cdc.Transform) *TransformChain {
	return &TransformChain{
		transforms: transforms,
	}
}

// Process applies each transform in sequence.
// Returns nil event with nil error if any transform drops the event.
// Returns error if any transform returns an error.
func (c *TransformChain) Process(event *cdc.Event) (*cdc.Event, error) {
	current := event

	for i, tf := range c.transforms {
		transformed, err := tf.Process(current)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("transform %d failed", i), err)
		}
		if transformed == nil {
			// Transform dropped the event
			return nil, nil
		}
		current = transformed
	}

	return current, nil
}

// Close closes all transforms in the chain.
// Collects all errors using errors.Join.
func (c *TransformChain) Close() error {
	var errs []error
	for i, tf := range c.transforms {
		if err := tf.Close(); err != nil {
			errs = append(errs, errors.Join(fmt.Errorf("transform %d close failed", i), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Ensure TransformChain implements cdc.Transform.
var _ cdc.Transform = (*TransformChain)(nil)
