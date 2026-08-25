package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/compforge/agentd/agentlet"
	"github.com/compforge/agentd/internal/buildinfo"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println(buildinfo.String("agentlet"))
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("starting agentlet",
		"version", buildinfo.Version,
		"revision", buildinfo.Revision,
		"build_time", buildinfo.BuildTime,
	)
	if err := agentlet.Run(logger); err != nil {
		logger.Error("agentlet stopped", "error", err)
		os.Exit(1)
	}
}
