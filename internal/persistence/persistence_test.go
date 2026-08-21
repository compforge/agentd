package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenFallsBackToSQLite(t *testing.T) {
	backend, err := Open(context.Background(), Config{
		SQLitePath: filepath.Join(t.TempDir(), "agentlet.db"), OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if backend.Provider != "sqlite" {
		t.Fatalf("Provider = %q, want sqlite", backend.Provider)
	}
	if backend.Database == nil || backend.Ledger == nil || backend.Checkpoints == nil {
		t.Fatal("SQLite backend did not initialize all stores")
	}
}
