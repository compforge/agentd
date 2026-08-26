package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/compforge/agentd/agentlet/internal/harness"
	sessionwork "github.com/compforge/agentd/agentlet/internal/work"
	"github.com/qiankunli/go-stdx/uuid"
)

const forcedWorkStopTimeout = 5 * time.Second

type Service struct {
	repository      Repository
	events          *EventLog
	harness         Harness
	logger          *slog.Logger
	works           *sessionwork.Manager
	workCtx         context.Context
	cancelWork      context.CancelFunc
	reconcileCtx    context.Context
	cancelReconcile context.CancelFunc

	mu                sync.Mutex
	workSet           sync.WaitGroup
	reconcileSet      sync.WaitGroup
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
	workCtx, cancelWork := context.WithCancel(context.Background())
	reconcileCtx, cancelReconcile := context.WithCancel(context.Background())
	executionService := &Service{
		repository: repository, events: events, harness: harness,
		workCtx: workCtx, cancelWork: cancelWork,
		reconcileCtx: reconcileCtx, cancelReconcile: cancelReconcile,
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
	a.reconcileSet.Add(1)
	a.mu.Unlock()
	if err := a.Recover(ctx); err != nil {
		a.mu.Lock()
		a.started = false
		a.mu.Unlock()
		a.reconcileSet.Done()
		return fmt.Errorf("initial session reconciliation: %w", err)
	}

	go a.reconcileLoop()
	return nil
}

func (a *Service) reconcileLoop() {
	defer a.reconcileSet.Done()
	ticker := time.NewTicker(a.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.reconcileCtx.Done():
			return
		case <-ticker.C:
			if err := a.Recover(a.reconcileCtx); err != nil && a.reconcileCtx.Err() == nil {
				a.logger.Error("reconcile durable session inputs", "error", err)
			}
		}
	}
}

// Shutdown stops admission and background reconciliation before waiting for
// accepted Work to reach a stable boundary. Only an expired shutdown deadline
// aborts Harness execution; that forced path must leave durable input pending.
//
// +spec=`Agentlet shutdown drains accepted Work to a stable checkpoint without treating process termination as a user interrupt; forced cancellation leaves durable input recoverable`
// +case:id=agentlet_graceful_drain,desc=`terminate a Worker Pod during a running Turn`,expect=`the Turn either settles before exit or resumes on a replacement Worker without lost or duplicate input`,forbid=`marking shutdown cancellation as retries_exhausted or consuming the input without a safe result`,group=system
// +why=`User interrupt is a product action, while process shutdown is a placement lifecycle event; sharing their terminal path can consume recoverable input as a failed completed Turn`
// +link=agentd/docs/agentlet.md
// +link=tests/e2e/cases/managed-agent.yaml
func (a *Service) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	a.cancelReconcile()
	active := activeWorkCount(a.works.Snapshots())
	a.mu.Unlock()
	a.logger.InfoContext(ctx, "draining Agentlet service", "active_work", active)

	done := make(chan struct{})
	go func() {
		a.reconcileSet.Wait()
		a.workSet.Wait()
		close(done)
	}()
	select {
	case <-done:
		a.cancelWork()
		a.logger.InfoContext(ctx, "drained Agentlet service")
		return nil
	case <-ctx.Done():
		a.cancelWork()
		remaining := a.works.Snapshots()
		for _, work := range remaining {
			if work.Active {
				a.harness.Interrupt(work.Spec.Session.ID)
			}
		}
		a.logger.Warn("Agentlet drain deadline exceeded; canceling active Work",
			"active_work", activeWorkCount(remaining), "error", ctx.Err())
		forceTimer := time.NewTimer(forcedWorkStopTimeout)
		defer forceTimer.Stop()
		select {
		case <-done:
			a.logger.Info("stopped Agentlet Work after forced drain cancellation")
			return nil
		case <-forceTimer.C:
			return fmt.Errorf("force-stop Agentlet Work after drain deadline: %w", ctx.Err())
		}
	}
}

func activeWorkCount(works []WorkSnapshot) int {
	active := 0
	for _, work := range works {
		if work.Active {
			active++
		}
	}
	return active
}

func (a *Service) CreateAgent(ctx context.Context, value Agent) (Agent, error) {
	if strings.TrimSpace(value.Name) == "" || strings.TrimSpace(value.Model.ID) == "" ||
		strings.TrimSpace(value.Model.Provider) == "" || strings.TrimSpace(value.Model.UpstreamID) == "" ||
		strings.TrimSpace(value.Model.APIKey) == "" {
		return Agent{}, invalid("agent name and model are required")
	}
	now := time.Now().UTC()
	value.ID = uuid.NewWithPrefix("agent")
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
	value.ID = uuid.NewWithPrefix("env")
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
		ID:            uuid.NewWithPrefix("session"),
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
	pending, err := a.events.PendingInputs(ctx, sessionID)
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

// Recover reconciles durable inputs for Work assignments still owned by this
// Agentlet process. A replacement process must first receive the current
// WorkSpec from agentd; scanning every Session that ever ran here would let a
// stale Agentlet consume inputs assigned to another Worker.
//
// +spec=`Agentlet retries durable input only for a process-local Work Assignment; a replacement Agentlet resumes only after agentd installs its current WorkSpec`
// +case:id=recover_committed_input,desc=`replace agentd after the harness commits an input but before it emits output`,input=`send one user message, stop the first process, recover, then send a second message`,expect=`one output per input; session returns to idle; harness revision advances once per input`,forbid=`duplicate user input, duplicate harness state, or duplicate agent output`
// +case:id=assignment_handoff_fence,desc=`leave an old Agentlet alive after its Work settles, assign the Session to a new Agentlet, then append input`,expect=`only the new Assignment executes the input`,forbid=`the stale Agentlet scanning shared Ledger input or advancing the Checkpoint`
// +case:id=reconcile_transient_worker_failure,desc=`a durable event append fails while a worker projects harness output`,expect=`the worker failure is logged; the pending input is retried without restarting agentd; only one durable output is projected`,forbid=`stuck running state or duplicate output`
// +link=agentd/docs/agentlet.md
func (a *Service) Recover(ctx context.Context) error {
	for _, localWork := range a.works.Snapshots() {
		sessionID := localWork.Spec.Session.ID
		session, err := a.repository.GetSession(ctx, sessionID)
		if err != nil {
			return fmt.Errorf("load Session %q for recovery: %w", sessionID, err)
		}
		if session.Control.AssignmentID != localWork.Spec.AssignmentID {
			continue
		}
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
		pending, err := a.events.PendingInputs(ctx, session.ID)
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
		a.logger.Debug("coalesced Session wake", "session_id", session.ID,
			"placement_fence", session.Control.AssignmentID)
		return nil
	}
	a.workSet.Add(1)
	a.logger.Info("started Session Work", "session_id", session.ID,
		"placement_fence", session.Control.AssignmentID)
	go a.runWorker(session.ID, session.Control.AssignmentID, resident)
	return nil
}

func (a *Service) runWorker(sessionID, assignmentID string, resident *sessionwork.Work) {
	finished := false
	defer a.workSet.Done()
	defer func() {
		if finished {
			return
		}
		if err := resident.Stop(assignmentID); err != nil {
			a.logger.Warn("stop Session Work", "session_id", sessionID, "error", err)
		}
	}()
	for {
		if a.workCtx.Err() != nil {
			return
		}
		pending, err := a.events.PendingInputs(a.workCtx, sessionID)
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
				a.logger.Info("settled terminal Session Work", "session_id", sessionID,
					"placement_fence", assignmentID)
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
		a.logger.Info("settled idle Session Work", "session_id", sessionID,
			"placement_fence", assignmentID)
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
	if a.workCtx.Err() != nil {
		return
	}
	a.logger.Error("session worker stopped", "session_id", sessionID, "error", workerErr)
	if err := a.markRescheduling(a.workCtx, sessionID); err != nil {
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
	ctx := a.workCtx
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
	a.logger.InfoContext(ctx, "started Session turn", "session_id", sessionID,
		"input_event_id", inputID, "placement_fence", assignmentID)
	if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.status_running", nil)); err != nil {
		return false, fmt.Errorf("record running state: %w", err)
	}

	var outputErr error
	var outputMu sync.Mutex
	turn := turnInput(input)
	if turn.ToolResolution != nil {
		recoveryInput, recoveryErr := a.toolRecoveryInput(ctx, sessionID, turn.ToolResolution.ToolUseID)
		if recoveryErr != nil {
			return false, recoveryErr
		}
		turn.RecoveryInput = &recoveryInput
	}
	result, runErr := a.harness.Run(ctx, executionSession(session), turn, func(event ManagedEvent) error {
		err := a.events.AppendExecution(ctx, sessionID, event)
		outputMu.Lock()
		if outputErr == nil {
			outputErr = err
		}
		outputMu.Unlock()
		return err
	})
	// A shutdown deadline is not a completed Turn outcome. Leave the input and
	// any unresolved Ledger Attempt pending so another Assignment can recover it.
	if err := a.workCtx.Err(); err != nil {
		return false, fmt.Errorf("Agentlet Work canceled during drain: %w", err)
	}
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
	var requiresAction *harness.RequiresActionError
	if errors.As(runErr, &requiresAction) {
		eventIDs := make([]string, 0, len(requiresAction.ToolUses))
		for _, toolUse := range requiresAction.ToolUses {
			toolInputID := toolUse.InputID
			if toolInputID == "" {
				toolInputID = inputID
			}
			value := NewManagedEvent("agent.tool_use", map[string]any{
				"name": toolUse.Name, "input": toolUse.Input, "evaluated_permission": "ask",
				"input_event_id": toolInputID,
			})
			value["id"] = toolUse.ID
			if err := a.events.AppendExecution(ctx, sessionID, value); err != nil {
				return false, fmt.Errorf("record required tool action: %w", err)
			}
			eventIDs = append(eventIDs, toolUse.ID)
		}
		if len(eventIDs) == 0 {
			return false, fmt.Errorf("unsafe recovery did not identify a tool use: %w", runErr)
		}
		if err := a.events.AppendExecution(ctx, sessionID, NewTurnEvent(inputID, "session.status_idle", map[string]any{
			"stop_reason": map[string]any{"type": "requires_action", "event_ids": eventIDs},
		})); err != nil {
			return false, fmt.Errorf("record required action state: %w", err)
		}
		transition(&session, "idle")
		if err := a.repository.PutSession(ctx, session); err != nil {
			return false, fmt.Errorf("persist requires-action idle state: %w", err)
		}
		if err := a.events.MarkProcessed(ctx, sessionID, inputID); err != nil {
			return false, fmt.Errorf("mark unsafe input processed: %w", err)
		}
		a.logger.WarnContext(ctx, "paused Session for required tool action",
			"session_id", sessionID, "input_event_id", inputID,
			"resume_revision", session.Control.ResumeRevision, "tool_use_count", len(eventIDs))
		return false, nil
	}
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
		a.logger.ErrorContext(ctx, "terminated Session at unrecoverable Harness boundary",
			"session_id", sessionID, "input_event_id", inputID, "error", runErr)
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
	a.logger.InfoContext(ctx, "completed Session turn", "session_id", sessionID,
		"input_event_id", inputID, "resume_revision", session.Control.ResumeRevision,
		"run_error", runErr != nil)
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

func turnInput(event ManagedEvent) TurnInput {
	id, _ := event["id"].(string)
	input := TurnInput{ID: id, Text: textContent(event)}
	eventType, _ := event["type"].(string)
	if eventType != "user.tool_confirmation" && eventType != "user.tool_result" {
		return input
	}
	resolution := &harness.ToolResolution{}
	resolution.ToolUseID, _ = event["tool_use_id"].(string)
	if eventType == "user.tool_confirmation" {
		resolution.Decision, _ = event["result"].(string)
		resolution.DenyMessage, _ = event["deny_message"].(string)
	} else {
		resolution.Decision = "result"
		resolution.Content = event["content"]
		resolution.IsError, _ = event["is_error"].(bool)
	}
	input.ToolResolution = resolution
	return input
}

// toolRecoveryInput resolves the accepted user message that originally drove
// a blocked tool execution. AgentGo restores the admission snapshot, so the
// host must redeliver this durable inbox item when a later confirmation or
// supplied result resumes the same native loop.
func (a *Service) toolRecoveryInput(
	ctx context.Context,
	sessionID string,
	toolUseID string,
) (harness.RecoveryInput, error) {
	events, err := a.events.List(ctx, sessionID)
	if err != nil {
		return harness.RecoveryInput{}, fmt.Errorf("load tool recovery input: %w", err)
	}
	var inputID string
	for _, event := range events {
		id, _ := event["id"].(string)
		if id == toolUseID && event["type"] == "agent.tool_use" {
			inputID, _ = event["input_event_id"].(string)
			break
		}
	}
	if inputID == "" {
		return harness.RecoveryInput{}, fmt.Errorf(
			"%w: tool use %q has no source input", ErrUnsafeRecovery, toolUseID,
		)
	}
	for _, event := range events {
		id, _ := event["id"].(string)
		if id == inputID {
			return harness.RecoveryInput{ID: inputID, Text: textContent(event)}, nil
		}
	}
	return harness.RecoveryInput{}, fmt.Errorf(
		"%w: source input %q for tool use %q is missing", ErrUnsafeRecovery, inputID, toolUseID,
	)
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrConflict, message)
}
