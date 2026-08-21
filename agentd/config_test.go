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
	if config.address != "0.0.0.0:8020" {
		t.Fatalf("address = %q", config.address)
	}
	if config.workerPort != 8019 {
		t.Fatalf("worker port = %d", config.workerPort)
	}
	if config.workerMinCount != 1 || config.workerMinIdle != 0 {
		t.Fatalf("Worker floors = count %d, idle %d", config.workerMinCount, config.workerMinIdle)
	}
}

func TestLoadConfigAllowsEnvironmentOverrides(t *testing.T) {
	t.Setenv("AGENTD_WORKER_CAPACITY", "8")
	t.Setenv("AGENTD_WORKER_MIN_COUNT", "3")
	t.Setenv("AGENTD_WORKER_MIN_IDLE", "2")
	t.Setenv("AGENTD_WORKER_IDLE_TTL", "20m")
	t.Setenv("AGENTD_WORKER_CREATE_BATCH_SIZE", "3")
	t.Setenv("AGENTD_WORKER_POD_TEMPLATE_FILE", "/tmp/worker.yaml")
	t.Setenv("AGENTD_WORKER_LIFECYCLER_INTERVAL", "7s")
	t.Setenv("AGENTD_WORKER_CONTROLLER_REQUEST_TIMEOUT", "25s")
	t.Setenv("AGENTD_WORKER_CONTROLLER_LEASE_TTL", "40s")
	t.Setenv("AGENTD_WORKER_GC_INTERVAL", "2m")
	t.Setenv("AGENTD_WORKER_GC_DELETE_BATCH_SIZE", "12")
	t.Setenv("AGENTD_WORKER_RECORD_GC_INTERVAL", "3h")
	t.Setenv("AGENTD_WORKER_RECORD_GC_REQUEST_TIMEOUT", "45s")
	t.Setenv("AGENTD_WORKER_RECORD_RETENTION", "336h")
	t.Setenv("AGENTD_WORKER_RECORD_GC_BATCH_SIZE", "25")
	t.Setenv("AGENTD_SESSION_OBSERVER_INTERVAL", "9s")
	t.Setenv("AGENTD_SESSION_OBSERVER_REQUEST_TIMEOUT", "4s")
	t.Setenv("AGENTD_SESSION_OBSERVER_CONCURRENCY", "6")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.workerCapacity != 8 || config.workerMinCount != 3 || config.workerMinIdle != 2 ||
		config.workerCreateBatchSize != 3 {
		t.Fatalf("worker capacity config = %+v", config)
	}
	if config.workerIdleTTL != 20*time.Minute || config.workerPodTemplateFile != "/tmp/worker.yaml" {
		t.Fatalf("worker config = %+v", config)
	}
	if config.workerLifecyclerInterval != 7*time.Second ||
		config.workerControllerTimeout != 25*time.Second ||
		config.workerControllerLeaseTTL != 40*time.Second ||
		config.workerGCInterval != 2*time.Minute || config.workerGCDeleteBatchSize != 12 {
		t.Fatalf("worker controller config = %+v", config)
	}
	if config.workerRecordGCInterval != 3*time.Hour ||
		config.workerRecordGCTimeout != 45*time.Second ||
		config.workerRecordRetention != 14*24*time.Hour || config.workerRecordGCBatchSize != 25 {
		t.Fatalf("worker record GC config = %+v", config)
	}
	if config.sessionObserverInterval != 9*time.Second ||
		config.sessionObserverTimeout != 4*time.Second || config.sessionObserverConcurrency != 6 {
		t.Fatalf("Session Observer config = %+v", config)
	}
}

func TestLoadConfigRequiresControllerLeaseToOutliveRequest(t *testing.T) {
	t.Setenv("AGENTD_WORKER_SOURCE", "kubernetes")
	t.Setenv("AGENTD_WORKER_CONTROLLER_REQUEST_TIMEOUT", "30s")
	t.Setenv("AGENTD_WORKER_CONTROLLER_LEASE_TTL", "20s")

	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() error = nil, want invalid Worker controller lease error")
	}
}

func TestLoadConfigRequiresAtLeastOneWorker(t *testing.T) {
	t.Setenv("AGENTD_WORKER_MIN_COUNT", "0")

	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() accepted zero minimum Workers")
	}
}
