package app

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
)

type App struct {
	repository Repository
	events     *EventLog
	harness    Harness
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc

	mu                sync.Mutex
	workers           map[string]*workerState
	workerSet         sync.WaitGroup
	reconcileInterval time.Duration
	started           bool
	closing           bool
}

type workerState struct {
	wake bool
}

type Option func(*App)

func WithLogger(logger *slog.Logger) Option {
	return func(application *App) {
		if logger != nil {
			application.logger = logger
		}
	}
}

func WithReconcileInterval(interval time.Duration) Option {
	return func(application *App) {
		application.reconcileInterval = interval
	}
}

func New(repository Repository, events *EventLog, harness Harness, options ...Option) *App {
	ctx, cancel := context.WithCancel(context.Background())
	application := &App{
		repository: repository, events: events, harness: harness, ctx: ctx, cancel: cancel,
		logger: slog.Default(), workers: make(map[string]*workerState),
	}
	for _, option := range options {
		option(application)
	}
	return application
}

func (a *App) Start(ctx context.Context) error {
	if a.reconcileInterval <= 0 {
		return errors.New("start application: reconcile interval must be positive")
	}
	a.mu.Lock()
	if a.started || a.closing {
		a.mu.Unlock()
		return errors.New("start application: application is already started or closing")
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

func (a *App) reconcileLoop() {
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

func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	for sessionID := range a.workers {
		a.harness.Interrupt(sessionID)
	}
	a.mu.Unlock()
	a.cancel()
	done := make(chan struct{})
	go func() {
		a.workerSet.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shut down application workers: %w", ctx.Err())
	}
}

func (a *App) CreateAgent(ctx context.Context, value Agent) (Agent, error) {
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

func (a *App) GetAgent(ctx context.Context, id string) (Agent, error) {
	return a.repository.GetAgent(ctx, id)
}

func (a *App) ListAgents(ctx context.Context) ([]Agent, error) {
	return a.repository.ListAgents(ctx)
}

func (a *App) CreateEnvironment(ctx context.Context, value Environment) (Environment, error) {
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

func (a *App) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	return a.repository.GetEnvironment(ctx, id)
}

func (a *App) ListEnvironments(ctx context.Context) ([]Environment, error) {
	return a.repository.ListEnvironments(ctx)
}

func (a *App) CreateSession(ctx context.Context, agentID string, version int64, environmentID, title string, metadata map[string]string) (Session, error) {
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
			Status: "idle", Harness: a.harness.Name(), HarnessVersion: a.harness.Version(), ResumeRevision: -1,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if session.Metadata == nil {
		session.Metadata = map[string]string{}
	}
	resumeRef, err := a.harness.PrepareSession(ctx, session)
	if err != nil {
		return Session{}, fmt.Errorf("prepare session harness state: %w", err)
	}
	session.Control.ResumeRef = resumeRef
	if err := a.repository.PutSession(ctx, session); err != nil {
		return Session{}, fmt.Errorf("create session: %w", err)
	}
	return session, nil
}

func (a *App) GetSession(ctx context.Context, id string) (Session, error) {
	return a.repository.GetSession(ctx, id)
}

func (a *App) ListSessions(ctx context.Context) ([]Session, error) {
	return a.repository.ListSessions(ctx)
}

func (a *App) SendEvents(ctx context.Context, sessionID string, incoming []IncomingEvent) ([]ManagedEvent, error) {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if err := validateIncoming(incoming); err != nil {
		return nil, err
	}
	if session.Control.Status == "terminated" {
		return nil, fmt.Errorf("%w: session %s is terminated", ErrConflict, sessionID)
	}
	accepted := make([]ManagedEvent, 0, len(incoming))
	for _, item := range incoming {
		switch item.Type {
		case "user.message":
			event := NewManagedEvent(item.Type, map[string]any{"content": item.Content})
			event["processed_at"] = nil
			if err := a.events.Append(ctx, sessionID, event); err != nil {
				return nil, err
			}
			accepted = append(accepted, event)
			transition(&session, "running")
			if err := a.repository.PutSession(ctx, session); err != nil {
				return nil, err
			}
			a.enqueue(sessionID)
		case "user.interrupt":
			event := NewManagedEvent(item.Type, nil)
			if err := a.events.Append(ctx, sessionID, event); err != nil {
				return nil, err
			}
			accepted = append(accepted, event)
			a.harness.Interrupt(sessionID)
		}
	}
	return accepted, nil
}

func validateIncoming(events []IncomingEvent) error {
	if len(events) == 0 {
		return invalid("events must not be empty")
	}
	for _, event := range events {
		switch event.Type {
		case "user.message":
			if len(event.Content) == 0 {
				return invalid("user.message content must not be empty")
			}
			for _, block := range event.Content {
				if block["type"] != "text" {
					return fmt.Errorf("%w: user.message content type %q", ErrUnsupported, block["type"])
				}
			}
		case "user.interrupt":
		default:
			return fmt.Errorf("%w: event type %q", ErrUnsupported, event.Type)
		}
	}
	return nil
}

func (a *App) ListEvents(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	if _, err := a.repository.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	return a.events.List(ctx, sessionID)
}

func (a *App) Subscribe(sessionID string) (<-chan ManagedEvent, func()) {
	return a.events.Subscribe(sessionID)
}

// Recover reconciles durable inputs left by a replaced process or stopped worker.
//
// +spec=`A persisted user input survives process replacement or a transient worker failure and is completed exactly once before the session accepts later input`
// +case:id=recover_committed_input,desc=`replace agentd after the harness commits an input but before it emits output`,input=`send one user message, stop the first process, recover, then send a second message`,expect=`one output per input; session returns to idle; harness revision advances once per input`,forbid=`duplicate user input, duplicate harness state, or duplicate agent output`
// +case:id=reconcile_transient_worker_failure,desc=`a durable event append fails while a worker projects harness output`,expect=`the worker failure is logged; the pending input is retried without restarting agentd; only one durable output is projected`,forbid=`stuck running state or duplicate output`
// +link=server/docs/state-ledger.md
func (a *App) Recover(ctx context.Context) error {
	sessions, err := a.repository.ListSessions(ctx)
	if err != nil {
		return fmt.Errorf("list sessions: %w", err)
	}
	for _, session := range sessions {
		if session.Control.Status == "terminated" {
			continue
		}
		if a.workerActive(session.ID) {
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
		if err := a.events.Append(ctx, session.ID, NewTurnEvent(inputID, "session.status_rescheduled", nil)); err != nil {
			return fmt.Errorf("record rescheduled session %q: %w", session.ID, err)
		}
		a.enqueue(session.ID)
	}
	return nil
}

func (a *App) workerActive(sessionID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, active := a.workers[sessionID]
	return active
}

func (a *App) enqueue(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing {
		return
	}
	worker := a.workers[sessionID]
	if worker == nil {
		worker = &workerState{}
		a.workers[sessionID] = worker
		a.workerSet.Add(1)
		go a.runWorker(sessionID, worker)
		return
	}
	worker.wake = true
}

func (a *App) runWorker(sessionID string, worker *workerState) {
	defer func() {
		a.mu.Lock()
		if a.workers[sessionID] == worker {
			delete(a.workers, sessionID)
		}
		a.mu.Unlock()
		a.workerSet.Done()
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
			keepRunning, err := a.process(sessionID, pending[0])
			if err != nil {
				a.handleWorkerFailure(sessionID, err)
				return
			}
			if !keepRunning {
				return
			}
			continue
		}

		a.mu.Lock()
		if a.workers[sessionID] != worker {
			a.mu.Unlock()
			return
		}
		if worker.wake {
			worker.wake = false
			a.mu.Unlock()
			continue
		}
		delete(a.workers, sessionID)
		a.mu.Unlock()
		return
	}
}

func (a *App) handleWorkerFailure(sessionID string, workerErr error) {
	if a.ctx.Err() != nil {
		return
	}
	a.logger.Error("session worker stopped", "session_id", sessionID, "error", workerErr)
	if err := a.markRescheduling(a.ctx, sessionID); err != nil {
		a.logger.Error("mark failed session for reconciliation", "session_id", sessionID, "error", err)
	}
}

func (a *App) markRescheduling(ctx context.Context, sessionID string) error {
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

func (a *App) process(sessionID string, input ManagedEvent) (bool, error) {
	ctx := a.ctx
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return false, fmt.Errorf("load session: %w", err)
	}
	transition(&session, "running")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return false, fmt.Errorf("persist running state: %w", err)
	}
	inputID, _ := input["id"].(string)
	if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_running", nil)); err != nil {
		return false, fmt.Errorf("record running state: %w", err)
	}

	var outputErr error
	var outputMu sync.Mutex
	result, runErr := a.harness.Run(ctx, session, TurnInput{ID: inputID, Text: textContent(input)}, func(event ManagedEvent) error {
		err := a.events.Append(ctx, sessionID, event)
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
	session.Control.ResumeRevision = result.ResumeRevision
	if errors.Is(runErr, ErrUnsafeRecovery) {
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "unsafe_recovery", "message": runErr.Error()},
		})); err != nil {
			return false, fmt.Errorf("record unsafe recovery: %w", err)
		}
		transition(&session, "terminated")
		if err := a.repository.PutSession(ctx, session); err != nil {
			return false, fmt.Errorf("persist terminated state: %w", err)
		}
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_terminated", nil)); err != nil {
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
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "runtime_error", "message": runErr.Error()},
		})); err != nil {
			return false, fmt.Errorf("record harness error: %w", err)
		}
		stopReason = map[string]any{"type": "retries_exhausted"}
	}
	if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_idle", map[string]any{"stop_reason": stopReason})); err != nil {
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
