package chain

import (
	"errors"
	"testing"

	"iris/pkg/cdc"
)

// mockTransform is a mock implementation of cdc.Transform for testing.
type mockTransform struct {
	name      string
	processFn func(event *cdc.Event) (*cdc.Event, error)
	closeFn   func() error
}

func (m *mockTransform) Process(event *cdc.Event) (*cdc.Event, error) {
	return m.processFn(event)
}

func (m *mockTransform) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

var _ cdc.Transform = (*mockTransform)(nil)

// newMockTransform creates a mock transform with a custom process function.
func newMockTransform(name string, processFn func(event *cdc.Event) (*cdc.Event, error)) *mockTransform {
	return &mockTransform{
		name:      name,
		processFn: processFn,
	}
}

func TestTransformChain_AllTransformsSucceed(t *testing.T) {
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			e.Table = e.Table + "_modified"
			return e, nil
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			e.Op = cdc.EventTypeUpdate
			return e, nil
		}),
	}

	chain := NewTransformChain(transforms)
	event := &cdc.Event{Table: "users", Op: cdc.EventTypeCreate}

	result, err := chain.Process(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected event, got nil")
	}
	if result.Table != "users_modified" {
		t.Errorf("expected table 'users_modified', got '%s'", result.Table)
	}
	if result.Op != cdc.EventTypeUpdate {
		t.Errorf("expected op 'update', got '%s'", result.Op)
	}
}

func TestTransformChain_MiddleTransformDrops(t *testing.T) {
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			return nil, nil // drop
		}),
		newMockTransform("t3", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
	}

	chain := NewTransformChain(transforms)
	event := &cdc.Event{Table: "users"}

	result, err := chain.Process(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil event (dropped), got %v", result)
	}
}

func TestTransformChain_TransformError(t *testing.T) {
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			return nil, errors.New("transform failed")
		}),
	}

	chain := NewTransformChain(transforms)
	event := &cdc.Event{Table: "users"}

	result, err := chain.Process(event)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if result != nil {
		t.Errorf("expected nil event on error, got %v", result)
	}
}

func TestTransformChain_CloseClosesAll(t *testing.T) {
	closeCount := 0
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
		newMockTransform("t3", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
	}

	// Wrap to count closes
	for i := range transforms {
		tf := transforms[i]
		transforms[i] = newMockTransform("wrapped", func(e *cdc.Event) (*cdc.Event, error) {
			return tf.Process(e)
		})
		transforms[i].(*mockTransform).closeFn = func() error {
			closeCount++
			return nil
		}
	}

	chain := NewTransformChain(transforms)
	err := chain.Close()

	if err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if closeCount != 3 {
		t.Errorf("expected 3 closes, got %d", closeCount)
	}
}

func TestTransformChain_CloseErrors(t *testing.T) {
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
	}

	transforms[0].(*mockTransform).closeFn = func() error {
		return errors.New("close failed 1")
	}
	transforms[1].(*mockTransform).closeFn = func() error {
		return errors.New("close failed 2")
	}

	chain := NewTransformChain(transforms)
	err := chain.Close()

	if err == nil {
		t.Fatal("expected close error, got nil")
	}
}

func TestTransformChain_EmptyTransforms(t *testing.T) {
	chain := NewTransformChain([]cdc.Transform{})
	event := &cdc.Event{Table: "users", Op: cdc.EventTypeCreate}

	result, err := chain.Process(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected event, got nil")
	}
	if result.Table != "users" {
		t.Errorf("expected table 'users', got '%s'", result.Table)
	}
}

func TestTransformChain_FirstTransformDrops(t *testing.T) {
	transforms := []cdc.Transform{
		newMockTransform("t1", func(e *cdc.Event) (*cdc.Event, error) {
			return nil, nil // drop
		}),
		newMockTransform("t2", func(e *cdc.Event) (*cdc.Event, error) {
			return e, nil
		}),
	}

	chain := NewTransformChain(transforms)
	event := &cdc.Event{Table: "users"}

	result, err := chain.Process(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil event (dropped), got %v", result)
	}
}
