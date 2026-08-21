package sandbox

import (
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
	"github.com/compforge/agentd/agentlet/internal/sandbox/hostel"
)

// Config is the Agentlet-facing configuration for its default Sandbox Engine.
// Concrete provider configuration stays behind this package boundary.
type Config struct {
	Endpoint       string
	RequestTimeout time.Duration
}

func NewEngine(config Config) (engine.Engine, error) {
	return hostel.NewEngine(hostel.EngineConfig{
		URL: config.Endpoint, RequestTimeout: config.RequestTimeout,
	})
}
