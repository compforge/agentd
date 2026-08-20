package gorm

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGORMLockerCoordinatesReplicasAndAllowsTakeover(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGORM(database); err != nil {
		t.Fatal(err)
	}
	first, _ := NewGORMLocker(database, "replica-a")
	second, _ := NewGORMLocker(database, "replica-b")
	token, err := first.Lock(context.Background(), "worker-pool", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Lock(context.Background(), "worker-pool", time.Minute); !errors.Is(err, controllock.ErrLocked) {
		t.Fatalf("second Lock error = %v, want ErrLocked", err)
	}
	if err := first.Unlock(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Lock(context.Background(), "worker-pool", time.Minute); err != nil {
		t.Fatalf("Lock after release: %v", err)
	}
}
