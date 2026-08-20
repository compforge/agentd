package main

import (
	"log/slog"
	"os"

	"github.com/compforge/agentd/agentd"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := agentd.Run(logger); err != nil {
		logger.Error("agentd stopped", "error", err)
		os.Exit(1)
	}
}
