package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type App struct {
	repository Repository
	events     *EventLog
	harness    Harness
	ctx        context.Context
	cancel     context.CancelFunc

	mu        sync.Mutex
	workers   map[string]*workerState
	workerSet sync.WaitGroup
	closing   bool
}

type workerState struct {
	wake bool
}

func New(repository Repository, events *EventLog, harness Harness) *App {
	ctx, cancel := context.WithCancel(context.Background())
	return &App{
		repository: repository, events: events, harness: harness, ctx: ctx, cancel: cancel,
		workers: make(map[string]*workerState),
	}
}

func (a *App) Shutdown(ctx context.Context) error {
	a.cancel()
	a.mu.Lock()
	a.closing = true
	for sessionID := range a.workers {
		a.harness.Interrupt(sessionID)
	}
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
		return fmt.Errorf("shut down session workers: %w", ctx.Err())
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
			a.enqueue(sessionID, event)
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

func (a *App) Recover(ctx context.Context) error {
	sessions, err := a.repository.ListSessions(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Control.Status == "terminated" {
			continue
		}
		pending, err := a.events.UnprocessedUserMessages(ctx, session.ID)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			if session.Control.Status == "running" || session.Control.Status == "rescheduling" {
				transition(&session, "idle")
				if err := a.repository.PutSession(ctx, session); err != nil {
					return err
				}
			}
			continue
		}
		transition(&session, "rescheduling")
		if err := a.repository.PutSession(ctx, session); err != nil {
			return err
		}
		if err := a.events.Append(ctx, session.ID, NewManagedEvent("session.status_rescheduled", nil)); err != nil {
			return err
		}
		for _, event := range pending {
			a.enqueue(session.ID, event)
		}
	}
	return nil
}

func (a *App) enqueue(sessionID string, _ ManagedEvent) {
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
			return
		}
		if len(pending) > 0 {
			if !a.process(sessionID, pending[0]) {
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

func (a *App) process(sessionID string, input ManagedEvent) bool {
	ctx := a.ctx
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return false
	}
	transition(&session, "running")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return false
	}
	inputID, _ := input["id"].(string)
	if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_running", nil)); err != nil {
		return false
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
		transition(&session, "rescheduling")
		_ = a.repository.PutSession(ctx, session)
		return false
	}
	session.Control.ResumeRevision = result.ResumeRevision
	if errors.Is(runErr, ErrUnsafeRecovery) {
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "unsafe_recovery", "message": runErr.Error()},
		})); err != nil {
			return false
		}
		transition(&session, "terminated")
		if err := a.repository.PutSession(ctx, session); err != nil {
			return false
		}
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_terminated", nil)); err != nil {
			return false
		}
		if err := a.events.MarkProcessed(ctx, sessionID, inputID); err != nil {
			return false
		}
		// Termination applies to the whole session. Leave later inputs pending for
		// manual reconciliation instead of letting this worker revive the session.
		return false
	}

	stopReason := map[string]any{"type": "end_turn"}
	if runErr != nil {
		if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.error", map[string]any{
			"error": map[string]any{"type": "runtime_error", "message": runErr.Error()},
		})); err != nil {
			return false
		}
		stopReason = map[string]any{"type": "retries_exhausted"}
	}
	if err := a.events.Append(ctx, sessionID, NewTurnEvent(inputID, "session.status_idle", map[string]any{"stop_reason": stopReason})); err != nil {
		return false
	}
	if err := a.events.MarkProcessed(ctx, sessionID, inputID); err != nil {
		return false
	}
	transition(&session, "idle")
	if err := a.repository.PutSession(ctx, session); err != nil {
		return false
	}
	return true
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
