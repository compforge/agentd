package agentd

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/compforge/agentd/agentd/internal/api"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	control "github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	sessionobserver "github.com/compforge/agentd/agentd/internal/session/observer"
	"github.com/compforge/agentd/agentd/internal/worker"
	controlgc "github.com/compforge/agentd/agentd/internal/worker/gc"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/compforge/agentd/internal/persistence"
)

func Run(logger *slog.Logger) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	storage, err := persistence.Open(context.Background(), persistence.Config{
		MySQLDSN: config.mysqlDSN, SQLitePath: config.sqlitePath,
		OperationTimeout: config.storageTimeout, MaxOpenConns: config.maxOpenConns,
		MaxIdleConns: config.maxIdleConns, ConnMaxLifetime: config.connMaxLifetime,
	})
	if err != nil {
		return err
	}
	defer storage.Close()
	repository, err := gormrepo.NewGORM(storage.Database)
	if err != nil {
		return err
	}
	recordGC, err := controlgc.NewRecordGC(repository, controlgc.RecordConfig{
		Interval: config.workerRecordGCInterval, RequestTimeout: config.workerRecordGCTimeout,
		Retention: config.workerRecordRetention, BatchSize: config.workerRecordGCBatchSize,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	workerControllers, err := worker.New(worker.Config{
		Source: config.workerSource, Namespace: config.workerNamespace, Selector: config.workerSelector,
		Port: config.workerPort, Capacity: config.workerCapacity,
		MinCount: config.workerMinCount, MinIdle: config.workerMinIdle,
		IdleTTL: config.workerIdleTTL, CreateBatchSize: config.workerCreateBatchSize,
		PodTemplateFile: config.workerPodTemplateFile, LifecyclerInterval: config.workerLifecyclerInterval,
		ControllerTimeout: config.workerControllerTimeout, ControllerLeaseTTL: config.workerControllerLeaseTTL,
		GCInterval: config.workerGCInterval, GCDeleteBatchSize: config.workerGCDeleteBatchSize,
		ObserverInterval: config.observerInterval, ObserverTimeout: config.observerTimeout,
	}, storage.Database, repository, logger)
	if err != nil {
		return err
	}
	var demandNotifier control.DemandNotifier
	if workerControllers != nil {
		demandNotifier = workerControllers
	}
	controlService, err := control.New(repository, config.observationTimeout, demandNotifier)
	if err != nil {
		return err
	}
	if workerControllers != nil {
		if err := workerControllers.AttachObserver(controlService); err != nil {
			return err
		}
	}
	agentletConnector, err := connector.New(connector.Config{
		RequestTimeout:        config.connectorRequestTimeout,
		DialTimeout:           config.connectorDialTimeout,
		ResponseHeaderTimeout: config.connectorHeaderTimeout,
		IdleConnTimeout:       config.connectorIdleConnTimeout,
		MaxIdleConns:          config.connectorMaxIdleConns,
		MaxIdleConnsPerHost:   config.connectorMaxIdleConnsPerHost,
	})
	if err != nil {
		return err
	}
	defer agentletConnector.CloseIdleConnections()
	sessionSource, err := sessionobserver.NewAgentletSource(controlService, agentletConnector)
	if err != nil {
		return err
	}
	sessionObserver, err := sessionobserver.New(sessionSource, controlService, sessionobserver.Config{
		Interval: config.sessionObserverInterval, RequestTimeout: config.sessionObserverTimeout,
		Concurrency: config.sessionObserverConcurrency, Logger: logger,
	})
	if err != nil {
		return err
	}

	httpServer := hertzserver.Default(
		hertzserver.WithHostPorts(config.address),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithReadTimeout(config.readTimeout),
		// SSE responses inherit their lifetime from the client context.
		hertzserver.WithWriteTimeout(0),
		hertzserver.WithIdleTimeout(config.idleTimeout),
		hertzserver.WithMaxRequestBodySize(1<<20),
		hertzserver.WithSenseClientDisconnection(true),
	)
	api.New(
		controlService, managedevent.NewLog(storage.Ledger), agentletConnector, logger,
		api.WithEventPollInterval(config.eventPollInterval),
	).Register(httpServer.Engine)

	processCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("agentd listening", "address", config.address)
		serveErr <- httpServer.Run()
	}()
	if workerControllers != nil {
		workerControllers.Run(processCtx)
	}
	go sessionObserver.Run(processCtx)
	go recordGC.Run(processCtx)
	select {
	case err := <-serveErr:
		if err == nil {
			return nil
		}
		return fmt.Errorf("serve agentd HTTP: %w", err)
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down agentd HTTP: %w", err)
		}
		return nil
	}
}
