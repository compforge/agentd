package main

import (
	"log/slog"
	"os"

	"github.com/compforge/agentd/agentlet"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := agentlet.Run(logger); err != nil {
		logger.Error("agentlet stopped", "error", err)
		os.Exit(1)
	}
}
