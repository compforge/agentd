package agentd

import (
	"context"
	"fmt"
	"log/slog"

	agentledger "github.com/compforge/agent-ledger/go"
	controlgc "github.com/compforge/agentd/agentd/internal/gc"
	controlk8s "github.com/compforge/agentd/agentd/internal/k8s"
	controllifecycle "github.com/compforge/agentd/agentd/internal/lifecycle"
	"github.com/compforge/agentd/agentd/internal/observer"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	control "github.com/compforge/agentd/agentd/internal/service"
	"gorm.io/gorm"
)

// workerControllers is the composition unit for the Kubernetes Worker control
// loops. Construction is split before and after Service because Lifecycler
// wakes from Service demand while Observer writes facts back through Service.
type workerControllers struct {
	kubernetesClient *controlk8s.Client
	lifecycler       *controllifecycle.Lifecycler
	podGC            *controlgc.PodGC
	observer         *observer.Observer
}

func buildWorkerControllers(
	config config,
	database *gorm.DB,
	repository repo.Repository,
	logger *slog.Logger,
) (*workerControllers, error) {
	if config.workerSource == "" {
		return nil, nil
	}
	kubernetesClient, err := controlk8s.NewInCluster(controlk8s.Config{
		Namespace: config.workerNamespace, LabelSelector: config.workerSelector,
		RequestTimeout: config.observerTimeout, QPS: 5, Burst: 10,
	})
	if err != nil {
		return nil, err
	}
	template, err := controlk8s.LoadPodTemplate(config.workerPodTemplateFile)
	if err != nil {
		return nil, err
	}
	provisioner, err := controlk8s.NewWorkerProvisioner(kubernetesClient, template)
	if err != nil {
		return nil, err
	}
	locker, err := gormrepo.NewGORMLocker(database, "agentd/"+agentledger.NewID())
	if err != nil {
		return nil, err
	}
	lifecycler, err := controllifecycle.New(repository, locker, provisioner, controllifecycle.Config{
		Interval: config.workerLifecyclerInterval, RequestTimeout: config.workerControllerTimeout,
		LeaseTTL: config.workerControllerLeaseTTL, WorkerCapacity: config.workerCapacity,
		MinIdleWorkers: config.workerMinIdle, CreateBatchSize: config.workerCreateBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	podGC, err := controlgc.NewPodGC(repository, locker, kubernetesClient, provisioner, controlgc.PodConfig{
		Interval: config.workerGCInterval, RequestTimeout: config.workerControllerTimeout,
		LeaseTTL: config.workerControllerLeaseTTL, IdleTTL: config.workerIdleTTL,
		MinIdleWorkers: config.workerMinIdle, DeleteBatchSize: config.workerGCDeleteBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &workerControllers{
		kubernetesClient: kubernetesClient,
		lifecycler:       lifecycler,
		podGC:            podGC,
	}, nil
}

func (c *workerControllers) attachObserver(config config, sink *control.Service, logger *slog.Logger) error {
	source, err := observer.NewKubernetesSource(c.kubernetesClient, config.workerPort, config.workerCapacity)
	if err != nil {
		return err
	}
	c.observer, err = observer.New(source, sink, observer.Config{
		Interval: config.observerInterval, RequestTimeout: config.observerTimeout, Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("build Worker Observer: %w", err)
	}
	return nil
}

func (c *workerControllers) Run(ctx context.Context) {
	go c.observer.Run(ctx)
	go c.lifecycler.Run(ctx)
	go c.podGC.Run(ctx)
}
