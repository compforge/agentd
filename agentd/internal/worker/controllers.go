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
	"github.com/compforge/agentd/agentd/internal/worker/observer"
	workerreconciler "github.com/compforge/agentd/agentd/internal/worker/reconciler"
	"gorm.io/gorm"
)

type Config struct {
	Source             string
	Namespace          string
	Selector           string
	Port               int
	Capacity           int
	MinCount           int
	MinIdle            int
	IdleTTL            time.Duration
	CreateBatchSize    int
	PodTemplateFile    string
	ReconcilerInterval time.Duration
	ControllerTimeout  time.Duration
	ControllerLeaseTTL time.Duration
	GCInterval         time.Duration
	GCDeleteBatchSize  int
	ObserverInterval   time.Duration
	ObserverTimeout    time.Duration
}

// Controllers is the composition unit for the Kubernetes Worker control
// loops. Construction is split before and after Service because Observer writes
// facts back through Service.
type Controllers struct {
	config           Config
	logger           *slog.Logger
	kubernetesClient *controlk8s.Client
	reconciler       *workerreconciler.Reconciler
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
	workerReconciler, err := workerreconciler.New(
		repository, locker, kubernetesClient, provisioner, workerreconciler.Config{
			Interval: config.ReconcilerInterval, RequestTimeout: config.ControllerTimeout,
			LeaseTTL: config.ControllerLeaseTTL, WorkerCapacity: config.Capacity,
			MinWorkers: config.MinCount, MinIdleWorkers: config.MinIdle, CreateBatchSize: config.CreateBatchSize,
			Logger: logger,
		})
	if err != nil {
		return nil, err
	}
	podGC, err := controlgc.NewPodGC(repository, locker, kubernetesClient, provisioner, controlgc.PodConfig{
		Interval: config.GCInterval, RequestTimeout: config.ControllerTimeout,
		LeaseTTL: config.ControllerLeaseTTL, IdleTTL: config.IdleTTL,
		MinWorkers: config.MinCount, MinIdleWorkers: config.MinIdle, DeleteBatchSize: config.GCDeleteBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &Controllers{
		config:           config,
		logger:           logger,
		kubernetesClient: kubernetesClient,
		reconciler:       workerReconciler,
		podGC:            podGC,
	}, nil
}

func (c *Controllers) AttachObserver(sink observer.Sink, notifier observer.Notifier) error {
	podInformer := c.kubernetesClient.NewAgentletPodInformer()
	source, err := observer.NewKubernetesSource(podInformer, c.config.Port, c.config.Capacity)
	if err != nil {
		return err
	}
	c.observer, err = observer.New(source, sink, notifier, observer.Config{
		Interval: c.config.ObserverInterval, RequestTimeout: c.config.ObserverTimeout, Logger: c.logger,
	})
	if err != nil {
		return fmt.Errorf("build Worker Observer: %w", err)
	}
	return nil
}

func (c *Controllers) Notify() {
	if c == nil {
		return
	}
	c.reconciler.Notify()
}

func (c *Controllers) Run(ctx context.Context) {
	go c.observer.Run(ctx)
	go c.reconciler.Run(ctx)
	go c.podGC.Run(ctx)
}
