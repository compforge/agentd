package agentd

import (
	"testing"
	"time"
)

func TestLoadConfigDefaultsToSQLite(t *testing.T) {
	t.Setenv("AGENTD_MYSQL_DSN", "")
	t.Setenv("AGENTD_SQLITE_PATH", "")
	t.Setenv("AGENTD_CONTROL_ADDRESS", "")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.mysqlDSN != "" || config.sqlitePath != "agentd.db" {
		t.Fatalf("storage config = mysql %q, sqlite %q", config.mysqlDSN, config.sqlitePath)
	}
	if config.address != "0.0.0.0:8082" {
		t.Fatalf("address = %q", config.address)
	}
}

func TestLoadConfigAllowsEnvironmentOverrides(t *testing.T) {
	t.Setenv("AGENTD_WORKER_CAPACITY", "8")
	t.Setenv("AGENTD_WORKER_MIN_IDLE", "2")
	t.Setenv("AGENTD_WORKER_IDLE_TTL", "20m")
	t.Setenv("AGENTD_WORKER_CREATE_BATCH_SIZE", "3")
	t.Setenv("AGENTD_WORKER_POD_TEMPLATE_FILE", "/tmp/worker.yaml")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.workerCapacity != 8 || config.workerMinIdle != 2 || config.workerCreateBatchSize != 3 {
		t.Fatalf("worker capacity config = %+v", config)
	}
	if config.workerIdleTTL != 20*time.Minute || config.workerPodTemplateFile != "/tmp/worker.yaml" {
		t.Fatalf("worker runtime config = %+v", config)
	}
}
