package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/compforge/agentd/agentd"
	"github.com/compforge/agentd/internal/buildinfo"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println(buildinfo.String("agentd"))
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("starting agentd",
		"version", buildinfo.Version,
		"revision", buildinfo.Revision,
		"build_time", buildinfo.BuildTime,
	)
	if err := agentd.Run(logger); err != nil {
		logger.Error("agentd stopped", "error", err)
		os.Exit(1)
	}
}
