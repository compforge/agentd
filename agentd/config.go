package agentd

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

type config struct {
	address               string
	mysqlDSN              string
	sqlitePath            string
	maxOpenConns          int
	maxIdleConns          int
	connMaxLifetime       time.Duration
	storageTimeout        time.Duration
	observationTimeout    time.Duration
	workerSource          string
	workerNamespace       string
	workerSelector        string
	workerPort            int
	workerMaxRuns         int
	workerMinIdle         int
	workerIdleTTL         time.Duration
	workerCreateBatchSize int
	workerPodTemplateFile string
	observerInterval      time.Duration
	observerTimeout       time.Duration
	readTimeout           time.Duration
	writeTimeout          time.Duration
	idleTimeout           time.Duration
	shutdownTimeout       time.Duration
}

func loadConfig() (config, error) {
	inClusterNamespace := readServiceAccountNamespace()
	workerSource := ""
	if inClusterNamespace != "" {
		workerSource = "kubernetes"
	}
	value := config{
		address:               envOr("AGENTD_CONTROL_ADDRESS", "0.0.0.0:8082"),
		mysqlDSN:              os.Getenv("AGENTD_MYSQL_DSN"),
		sqlitePath:            envOr("AGENTD_SQLITE_PATH", "agentd.db"),
		maxOpenConns:          32,
		maxIdleConns:          8,
		connMaxLifetime:       30 * time.Minute,
		storageTimeout:        5 * time.Second,
		observationTimeout:    15 * time.Second,
		workerSource:          envOr("AGENTD_WORKER_SOURCE", workerSource),
		workerNamespace:       envOr("AGENTD_WORKER_NAMESPACE", envOr("POD_NAMESPACE", inClusterNamespace)),
		workerSelector:        envOr("AGENTD_WORKER_LABEL_SELECTOR", "app.kubernetes.io/name=agentlet"),
		workerPort:            8081,
		workerMaxRuns:         1,
		workerMinIdle:         0,
		workerIdleTTL:         10 * time.Minute,
		workerCreateBatchSize: 2,
		workerPodTemplateFile: envOr("AGENTD_WORKER_POD_TEMPLATE_FILE", "/etc/agentd/worker-template/pod-template.yaml"),
		observerInterval:      5 * time.Second,
		observerTimeout:       5 * time.Second,
		readTimeout:           30 * time.Second,
		writeTimeout:          30 * time.Second,
		idleTimeout:           2 * time.Minute,
		shutdownTimeout:       15 * time.Second,
	}
	if value.workerNamespace == "" {
		value.workerNamespace = "default"
	}
	var err error
	if value.maxOpenConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_OPEN_CONNS", value.maxOpenConns); err != nil {
		return config{}, err
	}
	if value.maxIdleConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_IDLE_CONNS", value.maxIdleConns); err != nil {
		return config{}, err
	}
	if value.maxIdleConns > value.maxOpenConns {
		return config{}, errors.New("AGENTD_MYSQL_MAX_IDLE_CONNS must not exceed AGENTD_MYSQL_MAX_OPEN_CONNS")
	}
	if value.workerPort, err = positiveIntEnv("AGENTD_WORKER_PORT", value.workerPort); err != nil {
		return config{}, err
	}
	if value.workerPort > 65535 {
		return config{}, errors.New("AGENTD_WORKER_PORT must not exceed 65535")
	}
	if value.workerMaxRuns, err = positiveIntEnv("AGENTD_WORKER_MAX_RUNS", value.workerMaxRuns); err != nil {
		return config{}, err
	}
	if value.workerMinIdle, err = nonNegativeIntEnv("AGENTD_WORKER_MIN_IDLE", value.workerMinIdle); err != nil {
		return config{}, err
	}
	if value.workerCreateBatchSize, err = positiveIntEnv(
		"AGENTD_WORKER_CREATE_BATCH_SIZE", value.workerCreateBatchSize,
	); err != nil {
		return config{}, err
	}
	durations := []struct {
		name  string
		value *time.Duration
	}{
		{"AGENTD_MYSQL_CONN_MAX_LIFETIME", &value.connMaxLifetime},
		{"AGENTD_STORAGE_OPERATION_TIMEOUT", &value.storageTimeout},
		{"AGENTD_WORKER_OBSERVATION_TIMEOUT", &value.observationTimeout},
		{"AGENTD_WORKER_OBSERVER_INTERVAL", &value.observerInterval},
		{"AGENTD_WORKER_OBSERVER_REQUEST_TIMEOUT", &value.observerTimeout},
		{"AGENTD_WORKER_IDLE_TTL", &value.workerIdleTTL},
		{"AGENTD_HTTP_READ_TIMEOUT", &value.readTimeout},
		{"AGENTD_HTTP_WRITE_TIMEOUT", &value.writeTimeout},
		{"AGENTD_HTTP_IDLE_TIMEOUT", &value.idleTimeout},
		{"AGENTD_SHUTDOWN_TIMEOUT", &value.shutdownTimeout},
	}
	for _, item := range durations {
		parsed, err := durationEnv(item.name, *item.value)
		if err != nil {
			return config{}, err
		}
		*item.value = parsed
	}
	switch value.workerSource {
	case "":
	case "kubernetes":
		if value.observationTimeout <= value.observerInterval {
			return config{}, errors.New("AGENTD_WORKER_OBSERVATION_TIMEOUT must exceed AGENTD_WORKER_OBSERVER_INTERVAL")
		}
	default:
		return config{}, fmt.Errorf("unsupported AGENTD_WORKER_SOURCE %q", value.workerSource)
	}
	return value, nil
}

func readServiceAccountNamespace() string {
	raw, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func positiveIntEnv(name string, fallback int) (int, error) {
	value, set, err := intEnv(name)
	if err != nil || set && value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	if !set {
		return fallback, nil
	}
	return value, nil
}

func nonNegativeIntEnv(name string, fallback int) (int, error) {
	value, set, err := intEnv(name)
	if err != nil || set && value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	if !set {
		return fallback, nil
	}
	return value, nil
}

func intEnv(name string) (int, bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.Atoi(raw)
	return value, true, err
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
