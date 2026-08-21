package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	sessionwork "github.com/compforge/agentd/agentlet/internal/work"
)

type Service struct {
	repository Repository
	events     *EventLog
	harness    Harness
	logger     *slog.Logger
	works      *sessionwork.Manager
	ctx        context.Context
	cancel     context.CancelFunc

	mu                sync.Mutex
	workerSet         sync.WaitGroup
	workCapacity      int
	reconcileInterval time.Duration
	started           bool
	closing           bool
}

type Option func(*Service)

func WithLogger(logger *slog.Logger) Option {
	return func(executionService *Service) {
		if logger != nil {
			executionService.logger = logger
		}
	}
}

func WithReconcileInterval(interval time.Duration) Option {
	return func(executionService *Service) {
		executionService.reconcileInterval = interval
	}
}

func WithWorkCapacity(capacity int) Option {
	return func(executionService *Service) {
		if capacity > 0 {
			executionService.workCapacity = capacity
		}
	}
}

func New(repository Repository, events *EventLog, harness Harness, options ...Option) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	executionService := &Service{
		repository: repository, events: events, harness: harness, ctx: ctx, cancel: cancel,
		logger: slog.Default(), workCapacity: 1,
	}
	for _, option := range options {
		option(executionService)
	}
	executionService.works = sessionwork.NewManager(executionService.workCapacity)
	return executionService
}

func (a *Service) Start(ctx context.Context) error {
	if a.reconcileInterval <= 0 {
		return errors.New("start service: reconcile interval must be positive")
	}
	a.mu.Lock()
	if a.started || a.closing {
		a.mu.Unlock()
		return errors.New("start service: service is already started or closing")
	}
	a.started = true
	a.mu.Unlock()
	if err := a.Recover(ctx); err != nil {
		a.mu.Lock()
		a.started = false
		a.mu.Unlock()
		return fmt.Errorf("initial session reconciliation: %w", err)
	}

	a.workerSet.Add(1)
	go a.reconcileLoop()
	return nil
}

func (a *Service) reconcileLoop() {
	defer a.workerSet.Done()
	ticker := time.NewTicker(a.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if err := a.Recover(a.ctx); err != nil && a.ctx.Err() == nil {
				a.logger.Error("reconcile durable session inputs", "error", err)
			}
		}
	}
}

func (a *Service) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	for _, work := range a.works.Snapshots() {
		if work.Active {
			a.harness.Interrupt(work.Spec.Session.ID)
		}
	}
	a.cancel()
	a.mu.Unlock()
	done := make(chan struct{})
	go func() {
		a.workerSet.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shut down service workers: %w", ctx.Err())
	}
}

func (a *Service) CreateAgent(ctx context.Context, value Agent) (Agent, error) {
	if strings.TrimSpace(value.Name) == "" || strings.TrimSpace(value.ModelID) == "" {
		return Agent{}, invalid("agent name and model are required")
	}
	now := time.Now().UTC()
	value.ID = newID("agent")
	value.Version = 1
	value.CreatedAt = now
	value.UpdatedAt = now
	if value.Metadata == nil {
		value.Metadata = map[string]string{}
	}
	if value.Tools == nil {
		value.Tools = []map[string]any{}
	}
	if err := a.repository.PutAgent(ctx, value); err != nil {
		return Agent{}, fmt.Errorf("create agent: %w", err)
	}
	return value, nil
}

func (a *Service) GetAgent(ctx context.Context, id string) (Agent, error) {
	return a.repository.GetAgent(ctx, id)
}

func (a *Service) ListAgents(ctx context.Context) ([]Agent, error) {
	return a.repository.ListAgents(ctx)
}

func (a *Service) CreateEnvironment(ctx context.Context, value Environment) (Environment, error) {
	if strings.TrimSpace(value.Name) == "" {
		return Environment{}, invalid("environment name is required")
	}
	now := time.Now().UTC()
	value.ID = newID("env")
	value.CreatedAt = now
	value.UpdatedAt = now
	if value.Metadata == nil {
		value.Metadata = map[string]string{}
	}
	if err := a.repository.PutEnvironment(ctx, value); err != nil {
		return Environment{}, fmt.Errorf("create environment: %w", err)
	}
	return value, nil
}

func (a *Service) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	return a.repository.GetEnvironment(ctx, id)
}

func (a *Service) ListEnvironments(ctx context.Context) ([]Environment, error) {
	return a.repository.ListEnvironments(ctx)
}

func (a *Service) CreateSession(ctx context.Context, agentID string, version int64, environmentID, title string, metadata map[string]string) (Session, error) {
	agent, err := a.repository.GetAgent(ctx, agentID)
	if err != nil {
		return Session{}, fmt.Errorf("resolve session agent: %w", err)
	}
	if version != 0 && version != agent.Version {
		return Session{}, fmt.Errorf("%w: agent version %d is unavailable", ErrConflict, version)
	}
	if _, err := a.repository.GetEnvironment(ctx, environmentID); err != nil {
		return Session{}, fmt.Errorf("resolve session environment: %w", err)
	}
	now := time.Now().UTC()
	session := Session{
		ID:            newID("session"),
		Agent:         agent,
		EnvironmentID: environmentID,
		Title:         title,
		Metadata:      metadata,
		Control: ControlState{
			Status: "idle", Harness: a.harness.Name(), HarnessVersion: a.harness.Version(), ResumeRevision: 0,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if session.Metadata == nil {
		session.Metadata = map[string]string{}
	}
	resumeRef, err := a.harness.PrepareSession(ctx, executionSession(session))
	if err != nil {
		return Session{}, fmt.Errorf("prepare session harness state: %w", err)
	}
	session.Control.ResumeRef = resumeRef
	if err := a.repository.PutSession(ctx, session); err != nil {
		return Session{}, fmt.Errorf("create session: %w", err)
	}
	return session, nil
}

func (a *Service) GetSession(ctx context.Context, id string) (Session, error) {
	return a.repository.GetSession(ctx, id)
}

func (a *Service) ListSessions(ctx context.Context) ([]Session, error) {
	return a.repository.ListSessions(ctx)
}

func (a *Service) Wake(ctx context.Context, sessionID string) error {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if session.Control.Status == "terminated" {
		return fmt.Errorf("%w: session %s is terminated", ErrConflict, sessionID)
	}
	pending, err := a.events.UnprocessedUserMessages(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("list pending inputs: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	if err := a.ensureWork(session); err != nil {
		return err
	}
	return a.enqueue(session)
}

func (a *Service) Interrupt(ctx context.Context, sessionID string) error {
	if _, err := a.repository.GetSession(ctx, sessionID); err != nil {
		return err
	}
	a.harness.Interrupt(sessionID)
	return nil
}

// Recover reconciles durable inputs left by a replaced process or stopped worker.
//
// +spec=`A persisted user input survives process replacement or a transient worker failure and is completed exactly once before the session accepts later input`
// +case:id=recover_committed_input,desc=`replace agentd after the harness commits an input but before it emits output`,input=`send one user message, stop the first process, recover, then send a second message`,expect=`one output per input; session returns to idle; harness revision advances once per input`,forbid=`duplicate user input, duplicate harness state, or duplicate agent output`
// +case:id=reconcile_transient_worker_failure,desc=`a durable event append fails while a worker projects harness output`,expect=`the worker failure is logged; the pending input is retried without restarting agentd; only one durable output is projected`,forbid=`stuck running state or duplicate output`
// +link=agentd/docs/agentlet.md
func (a *Service) Recover(ctx context.Context) error {
	sessions, err := a.repository.ListSessions(ctx)
	if err != nil {
		return fmt.Errorf("list sessions: %w", err)
	}
	for _, session := range sessions {
		if session.Control.Status == "terminated" {
			continue
		}
		active, err := a.workActive(session.ID)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		pending, err := a.events.UnprocessedUserMessages(ctx, session.ID)
		if err != nil {
			return fmt.Errorf("list pending inputs for session %q: %w", session.ID, err)
		}
		if len(pending) == 0 {
			if session.Control.Status == "running" || session.Control.Status == "rescheduling" {
				transition(&session, "idle")
				if err := a.repository.PutSession(ctx, session); err != nil {
					return fmt.Errorf("repair idle session %q: %w", session.ID, err)
				}
			}
			continue
		}
		if session.Control.Status != "rescheduling" {
			transition(&session, "rescheduling")
			if err := a.repository.PutSession(ctx, session); err != nil {
				return fmt.Errorf("reschedule session %q: %w", session.ID, err)
			}
		}
		inputID, _ := pending[0]["id"].(string)
		if err := a.events.AppendExecution(ctx, session.ID, NewTurnEvent(inputID, "session.status_rescheduled", nil)); err != nil {
			return fmt.Errorf("record rescheduled session %q: %w", session.ID, err)
		}
		if err := a.ensureWork(session); err != nil {
			return fmt.Errorf("reserve recovered Session %q Work: %w", session.ID, err)
		}
		if err := a.enqueue(session); err != nil {
			return fmt.Errorf("enqueue recovered Session %q: %w", session.ID, err)
		}
	}
	return nil
}

func (a *Service) workActive(sessionID string) (bool, error) {
	snapshot, err := a.works.Snapshot(sessionID)
	if errors.Is(err, sessionwork.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read Session %q Work: %w", sessionID, err)
	}
	return snapshot.Active, nil
}

func (a *Service) ensureWork(session Session) error {
	_, _, err := a.works.Ensure(sessionwork.Spec{
		AssignmentID: session.Control.AssignmentID,
		Session:      executionSession(session),
	})
	return translateWorkError(session.ID, err)
}

func (a *Service) enqueue(session Session) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing {
		return fmt.Errorf("%w: Agentlet is shutting down", ErrConflict)
	}
	resident, start, err := a.works.Wake(session.ID, session.Control.AssignmentID)
	if err != nil {
		return translateWorkError(session.ID, err)
	}
	if !start {
		return nil
	}
	a.workerSet.Add(1)
	go a.runWorker(session.ID, session.Control.AssignmentID, resident)
	return nil
}

func (a *Service) runWorker(sessionID, assignmentID string, resident *sessionwork.Work) {
	finished := false
	defer a.workerSet.Done()
	defer func() {
		if finished {
			return
		}
		if err := resident.Stop(assignmentID); err != nil {
			a.logger.Warn("stop Session Work", "session_id", sessionID, "error", err)
		}
	}()
	for {
		if a.ctx.Err() != nil {
			return
		}
		pending, err := a.events.UnprocessedUserMessages(a.ctx, sessionID)
		if err != nil {
			a.handleWorkerFailure(sessionID, fmt.Errorf("list pending inputs: %w", err))
			return
		}
		if len(pending) > 0 {
			keepRunning, err := a.process(sessionID, assignmentID, resident, pending[0])
			if err != nil {
				a.handleWorkerFailure(sessionID, err)
				return
			}
			if !keepRunning {
				if err := resident.Stop(assignmentID); err != nil {
					a.logger.Warn("stop terminal Session Work", "session_id", sessionID, "error", err)
				}
				a.evictInactiveWork(sessionID, assignmentID)
				finished = true
				return
			}
			continue
		}
		if resident.FinishPass() {
			continue
		}
		finished = true
		a.evictInactiveWork(sessionID, assignmentID)
		return
	}
}

func (a *Service) evictInactiveWork(sessionID, assignmentID string) {
	err := a.works.Delete(sessionID, assignmentID)
	// A concurrent wake or replacement owns the resident Work now; leaving it
	// in place is the correct outcome, not a cleanup failure.
	if err == nil || errors.Is(err, sessionwork.ErrNotFound) ||
		errors.Is(err, sessionwork.ErrActive) || errors.Is(err, sessionwork.ErrAssignmentConflict) {
		return
	}
	a.logger.Warn("release inactive Session Work", "session_id", sessionID, "error", err)
}

func translateWorkError(sessionID string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, sessionwork.ErrCapacity):
		return fmt.Errorf("%w: Session %q: %v", ErrCapacity, sessionID, err)
	case errors.Is(err, sessionwork.ErrNotFound):
		return fmt.Errorf("%w: Session %q Work", ErrNotFound, sessionID)
	case errors.Is(err, sessionwork.ErrAssignmentConflict), errors.Is(err, sessionwork.ErrActive):
		return fmt.Errorf("%w: Session %q: %v", ErrConflict, sessionID, err)
	default:
		return fmt.Errorf("manage Session %q Work: %w", sessionID, err)
	}
}

func (a *Service) handleWorkerFailure(sessionID string, workerErr error) {
	if a.ctx.Err() != nil {
		return
	}
	a.logger.Error("session worker stopped", "session_id", sessionID, "error", workerErr)
	if err := a.markRescheduling(a.ctx, sessionID); err != nil {
		a.logger.Error("mark failed session for reconciliation", "session_id", sessionID, "error", err)
	}
}

func (a *Service) markRescheduling(ctx context.Context, sessionID string) error {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("load session: %w", err)
	}
	if session.Control.Status == "terminated" || session.Control.Status == "rescheduling" {
		return nil
	}
	transition(&session, "rescheduling")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return fmt.Errorf("persist rescheduling state: %w", err)
	}
	return nil
}

func (a *Service) process(
	sessionID string,
	assignmentID string,
	resident *sessionwork.Work,
	input ManagedEvent,
) (bool, error) {
	ctx := a.ctx
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return false, fmt.Errorf("load session: %w", err)
	}
	if session.Control.AssignmentID != assignmentID {
		return false, fmt.Errorf("%w: Session %q Assignment changed", ErrConflict, sessionID)
	}
	transition(&session, "running")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return false, fmt.Errorf("persist running state: %w", err)
	}
	inputID, _ := input["id"].(string)
	if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.status_running", nil)); err != nil {
		return false, fmt.Errorf("record running state: %w", err)
	}

	var outputErr error
	var outputMu sync.Mutex
	result, runErr := a.harness.Run(ctx, executionSession(session), TurnInput{ID: inputID, Text: textContent(input)}, func(event ManagedEvent) error {
		err := a.events.AppendExecution(ctx, sessionID, event)
		outputMu.Lock()
		if outputErr == nil {
			outputErr = err
		}
		outputMu.Unlock()
		return err
	})
	outputMu.Lock()
	persistErr := outputErr
	outputMu.Unlock()
	if persistErr != nil {
		return false, fmt.Errorf("persist harness output: %w", persistErr)
	}
	resumeRef := session.Control.ResumeRef
	if result.ResumeRef != "" {
		resumeRef = result.ResumeRef
	}
	if err := resident.UpdateResume(assignmentID, resumeRef, result.ResumeRevision); err != nil {
		return false, translateWorkError(sessionID, err)
	}
	resume := resident.Snapshot().Spec.Session
	session.Control.ResumeRef = resume.ResumeRef
	session.Control.ResumeRevision = resume.ResumeRevision
	if errors.Is(runErr, ErrUnsafeRecovery) {
		if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "unsafe_recovery", "message": runErr.Error()},
		})); err != nil {
			return false, fmt.Errorf("record unsafe recovery: %w", err)
		}
		transition(&session, "terminated")
		if err := a.repository.PutSession(ctx, session); err != nil {
			return false, fmt.Errorf("persist terminated state: %w", err)
		}
		if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.status_terminated", nil)); err != nil {
			return false, fmt.Errorf("record terminated state: %w", err)
		}
		if err := a.events.MarkProcessed(ctx, sessionID, inputID); err != nil {
			return false, fmt.Errorf("mark unsafe input processed: %w", err)
		}
		// Termination applies to the whole session. Leave later inputs pending for
		// manual reconciliation instead of letting this worker revive the session.
		return false, nil
	}

	stopReason := map[string]any{"type": "end_turn"}
	if runErr != nil {
		if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "runtime_error", "message": runErr.Error()},
		})); err != nil {
			return false, fmt.Errorf("record harness error: %w", err)
		}
		stopReason = map[string]any{"type": "retries_exhausted"}
	}
	if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.status_idle", map[string]any{"stop_reason": stopReason})); err != nil {
		return false, fmt.Errorf("record idle state: %w", err)
	}
	if err := a.events.MarkProcessed(ctx, sessionID, inputID); err != nil {
		return false, fmt.Errorf("mark input processed: %w", err)
	}
	transition(&session, "idle")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return false, fmt.Errorf("persist idle state: %w", err)
	}
	return true, nil
}

func transition(session *Session, status string) {
	session.Control.Status = status
	session.Control.Revision++
	session.UpdatedAt = time.Now().UTC()
}

func textContent(event ManagedEvent) string {
	items, _ := event["content"].([]any)
	if items == nil {
		if typed, ok := event["content"].([]map[string]any); ok {
			items = make([]any, len(typed))
			for index := range typed {
				items[index] = typed[index]
			}
		}
	}
	var parts []string
	for _, item := range items {
		if block, ok := item.(map[string]any); ok && block["type"] == "text" {
			if text, ok := block["text"].(string); ok {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func newID(prefix string) string {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		panic(fmt.Sprintf("generate %s id: %v", prefix, err))
	}
	return prefix + "_" + hex.EncodeToString(bytes)
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrConflict, message)
}
