package wasmtransform

// Config holds WASM transform configuration.
type Config struct {
	// Path to the WASM module file.
	Path string `json:"path" yaml:"path"`

	// FunctionName is the exported function to call (default: "handle").
	FunctionName string `json:"function_name,omitempty" yaml:"function_name,omitempty"`

	// AllocFunctionName is the memory allocation function (default: "alloc").
	AllocFunctionName string `json:"alloc_function_name,omitempty" yaml:"alloc_function_name,omitempty"`

	// EnableLogging enables host function logging.
	EnableLogging bool `json:"enable_logging,omitempty" yaml:"enable_logging,omitempty"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		FunctionName:      "handle",
		AllocFunctionName: "alloc",
		EnableLogging:     false,
	}
}

// WithPath sets the WASM module path.
func (c Config) WithPath(path string) Config {
	c.Path = path
	return c
}

// WithFunctionName sets the transform function name.
func (c Config) WithFunctionName(name string) Config {
	c.FunctionName = name
	return c
}

// WithAllocFunctionName sets the allocation function name.
func (c Config) WithAllocFunctionName(name string) Config {
	c.AllocFunctionName = name
	return c
}

// WithLogging enables or disables host function logging.
func (c Config) WithLogging(enabled bool) Config {
	c.EnableLogging = enabled
	return c
}
