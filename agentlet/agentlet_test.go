package agentlet

import "testing"

func TestLoadConfigUsesWorkerCapacity(t *testing.T) {
	t.Setenv("AGENTD_WORKER_CAPACITY", "3")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.workCapacity != 3 {
		t.Fatalf("work capacity = %d, want 3", config.workCapacity)
	}
}

func TestLoadConfigRejectsInvalidWorkerCapacity(t *testing.T) {
	t.Setenv("AGENTD_WORKER_CAPACITY", "0")

	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() accepted zero Worker capacity")
	}
}
