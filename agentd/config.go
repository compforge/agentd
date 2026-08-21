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
	address                      string
	mysqlDSN                     string
	sqlitePath                   string
	maxOpenConns                 int
	maxIdleConns                 int
	connMaxLifetime              time.Duration
	storageTimeout               time.Duration
	observationTimeout           time.Duration
	workerSource                 string
	workerNamespace              string
	workerSelector               string
	workerPort                   int
	workerCapacity               int
	workerMinCount               int
	workerMinIdle                int
	workerIdleTTL                time.Duration
	workerCreateBatchSize        int
	workerPodTemplateFile        string
	workerLifecyclerInterval     time.Duration
	workerControllerTimeout      time.Duration
	workerControllerLeaseTTL     time.Duration
	workerGCInterval             time.Duration
	workerGCDeleteBatchSize      int
	workerRecordGCInterval       time.Duration
	workerRecordGCTimeout        time.Duration
	workerRecordRetention        time.Duration
	workerRecordGCBatchSize      int
	observerInterval             time.Duration
	observerTimeout              time.Duration
	sessionObserverInterval      time.Duration
	sessionObserverTimeout       time.Duration
	sessionObserverConcurrency   int
	sessionReconcilerInterval    time.Duration
	sessionReconcilerTimeout     time.Duration
	sessionReconcilerConcurrency int
	connectorRequestTimeout      time.Duration
	connectorDialTimeout         time.Duration
	connectorHeaderTimeout       time.Duration
	connectorIdleConnTimeout     time.Duration
	connectorMaxIdleConns        int
	connectorMaxIdleConnsPerHost int
	readTimeout                  time.Duration
	idleTimeout                  time.Duration
	shutdownTimeout              time.Duration
	eventPollInterval            time.Duration
}

func loadConfig() (config, error) {
	inClusterNamespace := readServiceAccountNamespace()
	workerSource := ""
	if inClusterNamespace != "" {
		workerSource = "kubernetes"
	}
	value := config{
		address:                      envOr("AGENTD_CONTROL_ADDRESS", "0.0.0.0:8020"),
		mysqlDSN:                     os.Getenv("AGENTD_MYSQL_DSN"),
		sqlitePath:                   envOr("AGENTD_SQLITE_PATH", "agentd.db"),
		maxOpenConns:                 32,
		maxIdleConns:                 8,
		connMaxLifetime:              30 * time.Minute,
		storageTimeout:               5 * time.Second,
		observationTimeout:           15 * time.Second,
		workerSource:                 envOr("AGENTD_WORKER_SOURCE", workerSource),
		workerNamespace:              envOr("AGENTD_WORKER_NAMESPACE", envOr("POD_NAMESPACE", inClusterNamespace)),
		workerSelector:               envOr("AGENTD_WORKER_LABEL_SELECTOR", "app.kubernetes.io/name=agentlet"),
		workerPort:                   8019,
		workerCapacity:               1,
		workerMinCount:               1,
		workerMinIdle:                0,
		workerIdleTTL:                10 * time.Minute,
		workerCreateBatchSize:        2,
		workerPodTemplateFile:        envOr("AGENTD_WORKER_POD_TEMPLATE_FILE", "/etc/agentd/worker-template/pod-template.yaml"),
		workerLifecyclerInterval:     5 * time.Second,
		workerControllerTimeout:      20 * time.Second,
		workerControllerLeaseTTL:     30 * time.Second,
		workerGCInterval:             time.Minute,
		workerGCDeleteBatchSize:      10,
		workerRecordGCInterval:       time.Hour,
		workerRecordGCTimeout:        20 * time.Second,
		workerRecordRetention:        7 * 24 * time.Hour,
		workerRecordGCBatchSize:      10,
		observerInterval:             5 * time.Second,
		observerTimeout:              5 * time.Second,
		sessionObserverInterval:      5 * time.Second,
		sessionObserverTimeout:       5 * time.Second,
		sessionObserverConcurrency:   8,
		sessionReconcilerInterval:    5 * time.Second,
		sessionReconcilerTimeout:     30 * time.Second,
		sessionReconcilerConcurrency: 8,
		connectorRequestTimeout:      30 * time.Second,
		connectorDialTimeout:         5 * time.Second,
		connectorHeaderTimeout:       10 * time.Second,
		connectorIdleConnTimeout:     90 * time.Second,
		connectorMaxIdleConns:        100,
		connectorMaxIdleConnsPerHost: 32,
		readTimeout:                  30 * time.Second,
		idleTimeout:                  2 * time.Minute,
		shutdownTimeout:              15 * time.Second,
		eventPollInterval:            500 * time.Millisecond,
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
	if value.workerCapacity, err = positiveIntEnv("AGENTD_WORKER_CAPACITY", value.workerCapacity); err != nil {
		return config{}, err
	}
	if value.workerMinCount, err = positiveIntEnv("AGENTD_WORKER_MIN_COUNT", value.workerMinCount); err != nil {
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
	if value.workerGCDeleteBatchSize, err = positiveIntEnv(
		"AGENTD_WORKER_GC_DELETE_BATCH_SIZE", value.workerGCDeleteBatchSize,
	); err != nil {
		return config{}, err
	}
	if value.workerRecordGCBatchSize, err = positiveIntEnv(
		"AGENTD_WORKER_RECORD_GC_BATCH_SIZE", value.workerRecordGCBatchSize,
	); err != nil {
		return config{}, err
	}
	if value.sessionObserverConcurrency, err = positiveIntEnv(
		"AGENTD_SESSION_OBSERVER_CONCURRENCY", value.sessionObserverConcurrency,
	); err != nil {
		return config{}, err
	}
	if value.sessionReconcilerConcurrency, err = positiveIntEnv(
		"AGENTD_SESSION_RECONCILER_CONCURRENCY", value.sessionReconcilerConcurrency,
	); err != nil {
		return config{}, err
	}
	if value.connectorMaxIdleConns, err = positiveIntEnv(
		"AGENTD_CONNECTOR_MAX_IDLE_CONNS", value.connectorMaxIdleConns,
	); err != nil {
		return config{}, err
	}
	if value.connectorMaxIdleConnsPerHost, err = positiveIntEnv(
		"AGENTD_CONNECTOR_MAX_IDLE_CONNS_PER_HOST", value.connectorMaxIdleConnsPerHost,
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
		{"AGENTD_SESSION_OBSERVER_INTERVAL", &value.sessionObserverInterval},
		{"AGENTD_SESSION_OBSERVER_REQUEST_TIMEOUT", &value.sessionObserverTimeout},
		{"AGENTD_SESSION_RECONCILER_INTERVAL", &value.sessionReconcilerInterval},
		{"AGENTD_SESSION_RECONCILER_REQUEST_TIMEOUT", &value.sessionReconcilerTimeout},
		{"AGENTD_WORKER_IDLE_TTL", &value.workerIdleTTL},
		{"AGENTD_WORKER_LIFECYCLER_INTERVAL", &value.workerLifecyclerInterval},
		{"AGENTD_WORKER_CONTROLLER_REQUEST_TIMEOUT", &value.workerControllerTimeout},
		{"AGENTD_WORKER_CONTROLLER_LEASE_TTL", &value.workerControllerLeaseTTL},
		{"AGENTD_WORKER_GC_INTERVAL", &value.workerGCInterval},
		{"AGENTD_WORKER_RECORD_GC_INTERVAL", &value.workerRecordGCInterval},
		{"AGENTD_WORKER_RECORD_GC_REQUEST_TIMEOUT", &value.workerRecordGCTimeout},
		{"AGENTD_WORKER_RECORD_RETENTION", &value.workerRecordRetention},
		{"AGENTD_CONNECTOR_REQUEST_TIMEOUT", &value.connectorRequestTimeout},
		{"AGENTD_CONNECTOR_DIAL_TIMEOUT", &value.connectorDialTimeout},
		{"AGENTD_CONNECTOR_RESPONSE_HEADER_TIMEOUT", &value.connectorHeaderTimeout},
		{"AGENTD_CONNECTOR_IDLE_CONN_TIMEOUT", &value.connectorIdleConnTimeout},
		{"AGENTD_HTTP_READ_TIMEOUT", &value.readTimeout},
		{"AGENTD_HTTP_IDLE_TIMEOUT", &value.idleTimeout},
		{"AGENTD_SHUTDOWN_TIMEOUT", &value.shutdownTimeout},
		{"AGENTD_EVENT_STREAM_POLL_INTERVAL", &value.eventPollInterval},
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
		if value.workerControllerLeaseTTL <= value.workerControllerTimeout {
			return config{}, errors.New("AGENTD_WORKER_CONTROLLER_LEASE_TTL must exceed AGENTD_WORKER_CONTROLLER_REQUEST_TIMEOUT")
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
