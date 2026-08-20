package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	controlgc "github.com/compforge/agentd/agentd/internal/worker/gc"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	controllifecycle "github.com/compforge/agentd/agentd/internal/worker/lifecycle"
	"github.com/compforge/agentd/agentd/internal/worker/observer"
	"gorm.io/gorm"
)

type Config struct {
	Source             string
	Namespace          string
	Selector           string
	Port               int
	Capacity           int
	MinIdle            int
	IdleTTL            time.Duration
	CreateBatchSize    int
	PodTemplateFile    string
	LifecyclerInterval time.Duration
	ControllerTimeout  time.Duration
	ControllerLeaseTTL time.Duration
	GCInterval         time.Duration
	GCDeleteBatchSize  int
	ObserverInterval   time.Duration
	ObserverTimeout    time.Duration
}

// Controllers is the composition unit for the Kubernetes Worker control
// loops. Construction is split before and after Service because Lifecycler
// wakes from Service demand while Observer writes facts back through Service.
type Controllers struct {
	config           Config
	logger           *slog.Logger
	kubernetesClient *controlk8s.Client
	lifecycler       *controllifecycle.Lifecycler
	podGC            *controlgc.PodGC
	observer         *observer.Observer
}

func New(
	config Config,
	database *gorm.DB,
	repository repo.Repository,
	logger *slog.Logger,
) (*Controllers, error) {
	if config.Source == "" {
		return nil, nil
	}
	kubernetesClient, err := controlk8s.NewInCluster(controlk8s.Config{
		Namespace: config.Namespace, LabelSelector: config.Selector,
		RequestTimeout: config.ObserverTimeout, QPS: 5, Burst: 10,
	})
	if err != nil {
		return nil, err
	}
	template, err := controlk8s.LoadPodTemplate(config.PodTemplateFile)
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
		Interval: config.LifecyclerInterval, RequestTimeout: config.ControllerTimeout,
		LeaseTTL: config.ControllerLeaseTTL, WorkerCapacity: config.Capacity,
		MinIdleWorkers: config.MinIdle, CreateBatchSize: config.CreateBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	podGC, err := controlgc.NewPodGC(repository, locker, kubernetesClient, provisioner, controlgc.PodConfig{
		Interval: config.GCInterval, RequestTimeout: config.ControllerTimeout,
		LeaseTTL: config.ControllerLeaseTTL, IdleTTL: config.IdleTTL,
		MinIdleWorkers: config.MinIdle, DeleteBatchSize: config.GCDeleteBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &Controllers{
		config:           config,
		logger:           logger,
		kubernetesClient: kubernetesClient,
		lifecycler:       lifecycler,
		podGC:            podGC,
	}, nil
}

func (c *Controllers) AttachObserver(sink observer.Sink) error {
	source, err := observer.NewKubernetesSource(c.kubernetesClient, c.config.Port, c.config.Capacity)
	if err != nil {
		return err
	}
	c.observer, err = observer.New(source, sink, observer.Config{
		Interval: c.config.ObserverInterval, RequestTimeout: c.config.ObserverTimeout, Logger: c.logger,
	})
	if err != nil {
		return fmt.Errorf("build Worker Observer: %w", err)
	}
	return nil
}

func (c *Controllers) NotifyDemand() {
	c.lifecycler.NotifyDemand()
}

func (c *Controllers) Run(ctx context.Context) {
	go c.observer.Run(ctx)
	go c.lifecycler.Run(ctx)
	go c.podGC.Run(ctx)
}
