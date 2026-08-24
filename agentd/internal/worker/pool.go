package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	controlgc "github.com/compforge/agentd/agentd/internal/worker/gc"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	"github.com/compforge/agentd/agentd/internal/worker/observer"
	workerreconciler "github.com/compforge/agentd/agentd/internal/worker/reconciler"
	"gorm.io/gorm"
)

const (
	poolLockResource       = "worker-pool:capacity"
	poolLockRetryInterval  = 50 * time.Millisecond
	poolLockReleaseTimeout = 5 * time.Second
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

// Pool is the sole owner of Worker capacity changes. Creation and reclamation
// share one control loop and one lease so independent schedules cannot starve
// either side. Observer remains independent because it only projects facts.
type Pool struct {
	config           Config
	logger           *slog.Logger
	locker           controllock.LeaseLocker
	kubernetesClient *controlk8s.Client
	reconciler       *workerreconciler.Reconciler
	podGC            *controlgc.PodGC
	observer         *observer.Observer
	notifications    chan struct{}
}

func New(
	config Config,
	database *gorm.DB,
	repository repo.Repository,
	logger *slog.Logger,
) (*Pool, error) {
	if config.Source == "" {
		return nil, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := validateConfig(config); err != nil {
		return nil, err
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
		repository, kubernetesClient, provisioner, workerreconciler.Config{
			WorkerCapacity: config.Capacity, MinWorkers: config.MinCount,
			MinIdleWorkers: config.MinIdle, CreateBatchSize: config.CreateBatchSize,
			Logger: logger,
		})
	if err != nil {
		return nil, err
	}
	podGC, err := controlgc.NewPodGC(repository, kubernetesClient, provisioner, controlgc.PodConfig{
		IdleTTL: config.IdleTTL, MinWorkers: config.MinCount,
		MinIdleWorkers: config.MinIdle, DeleteBatchSize: config.GCDeleteBatchSize,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &Pool{
		config:           config,
		logger:           logger,
		locker:           locker,
		kubernetesClient: kubernetesClient,
		reconciler:       workerReconciler,
		podGC:            podGC,
		notifications:    make(chan struct{}, 1),
	}, nil
}

func validateConfig(config Config) error {
	if config.ReconcilerInterval <= 0 || config.ControllerTimeout <= 0 ||
		config.ControllerLeaseTTL <= 0 || config.GCInterval <= 0 {
		return fmt.Errorf("create Worker Pool: controller durations must be positive")
	}
	if config.ControllerLeaseTTL/3 <= 0 {
		return fmt.Errorf("create Worker Pool: controller lease TTL is too short for heartbeat renewal")
	}
	return nil
}

func (p *Pool) AttachObserver(sink observer.Sink, notifier observer.Notifier) error {
	podInformer := p.kubernetesClient.NewAgentletPodInformer()
	source, err := observer.NewKubernetesSource(podInformer, p.config.Port, p.config.Capacity)
	if err != nil {
		return err
	}
	p.observer, err = observer.New(source, sink, notifier, observer.Config{
		Interval: p.config.ObserverInterval, RequestTimeout: p.config.ObserverTimeout, Logger: p.logger,
	})
	if err != nil {
		return fmt.Errorf("build Worker Observer: %w", err)
	}
	return nil
}

// Notify requests an early capacity pass. Notifications are coalesced because
// Worker rows in the database remain the source of truth.
func (p *Pool) Notify() {
	if p == nil {
		return
	}
	select {
	case p.notifications <- struct{}{}:
	default:
	}
}

func (p *Pool) Run(ctx context.Context) {
	if p.observer != nil {
		go p.observer.Run(ctx)
	}
	go p.run(ctx)
}

func (p *Pool) run(ctx context.Context) {
	p.reconcileWithTimeout(ctx, true)
	capacityTicker := time.NewTicker(p.config.ReconcilerInterval)
	gcTicker := time.NewTicker(p.config.GCInterval)
	defer capacityTicker.Stop()
	defer gcTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-capacityTicker.C:
			p.reconcileWithTimeout(ctx, false)
		case <-gcTicker.C:
			p.reconcileWithTimeout(ctx, true)
		case <-p.notifications:
			p.reconcileWithTimeout(ctx, false)
		}
	}
}

// Reconcile serializes all capacity decisions under one lease. A full pass
// reclaims first, then plans replacement and warm capacity from the resulting
// durable state.
//
// +spec=`Worker creation and reclamation share one capacity lease and one control loop; a scheduled pass waits for lease contention instead of treating it as success`
// +case:id=worker_pool_lock_contention,desc=`A contended full pass eventually reclaims expired idle capacity and realizes the remaining desired capacity`
// +why=`Lease TTL bounds dead-holder takeover rather than controller duration; heartbeat renewal keeps a long planning section owned, while Kubernetes effects remain outside the lease`
// +link=agentd/docs/agentd.md
func (p *Pool) Reconcile(ctx context.Context, full bool) error {
	var gcActions controlgc.Actions
	var gcPlanErr error
	var workers []model.Worker
	var capacityPlanErr error
	leaseErr := controllock.WithLease(ctx, p.locker, poolLockResource, controllock.LeaseOptions{
		TTL:               p.config.ControllerLeaseTTL,
		RetryInterval:     poolLockRetryInterval,
		HeartbeatInterval: p.config.ControllerLeaseTTL / 3,
		ReleaseTimeout:    poolLockReleaseTimeout,
		OnAcquired: func(waited time.Duration) {
			if waited >= poolLockRetryInterval {
				p.logger.InfoContext(ctx, "acquired Worker Pool lease after contention", "wait", waited)
			}
		},
	}, func(leaseCtx context.Context) error {
		if full {
			gcActions, gcPlanErr = p.podGC.Plan(leaseCtx)
		}
		workers, capacityPlanErr = p.reconciler.Plan(leaseCtx)
		return nil
	})
	if leaseErr != nil {
		return errors.Join(gcPlanErr, capacityPlanErr, fmt.Errorf("hold Worker Pool lease: %w", leaseErr))
	}

	// Kubernetes effects are idempotent and deliberately happen after unlock.
	// Attempt both sides so one failed effect does not suppress unrelated work.
	gcApplyErr := p.podGC.Apply(ctx, gcActions)
	capacityApplyErr := p.reconciler.Apply(ctx, workers)
	return errors.Join(gcPlanErr, capacityPlanErr, gcApplyErr, capacityApplyErr)
}

func (p *Pool) reconcileWithTimeout(ctx context.Context, full bool) {
	reconcileCtx, cancel := context.WithTimeout(ctx, p.config.ControllerTimeout)
	defer cancel()
	if err := p.Reconcile(reconcileCtx, full); err != nil && ctx.Err() == nil {
		p.logger.ErrorContext(ctx, "reconcile Worker Pool", "full", full, "error", err)
	}
}
