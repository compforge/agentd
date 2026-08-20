package work

import (
	"errors"
	"sync"

	"github.com/compforge/agentd/agentlet/internal/harness"
)

var (
	ErrAssignmentConflict = errors.New("work assignment conflict")
	ErrActive             = errors.New("work is active")
	ErrCapacity           = errors.New("work capacity exhausted")
	ErrNotFound           = errors.New("work not found")
)

// Spec is the control-plane slice needed to execute one durable Work.
type Spec struct {
	AssignmentID string
	Session      harness.Session
}

// Snapshot is a point-in-time view of process-local Work state.
type Snapshot struct {
	Spec    Spec
	Active  bool
	Pending bool
}

// Work is the process-local representative of one durable harness execution.
// Its logical identity survives active passes and Worker migration; this value
// and its Harness runtime resources do not.
type Work struct {
	mu      sync.Mutex
	spec    Spec
	active  bool
	pending bool
}

// Wake records demand for the Work. It returns true only when the caller must
// start an execution goroutine; concurrent wakes are coalesced into another pass.
func (w *Work) Wake(assignmentID string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if assignmentID != w.spec.AssignmentID {
		return false, ErrAssignmentConflict
	}
	if w.active {
		w.pending = true
		return false, nil
	}
	w.active = true
	return true, nil
}

// FinishPass reports whether a wake arrived during the completed pass. When it
// returns false, the Work becomes inactive and its execution goroutine may exit.
func (w *Work) FinishPass() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending {
		w.pending = false
		return true
	}
	w.active = false
	return false
}

// Stop clears execution state after a runner exits because of failure or
// shutdown. The Assignment fence prevents an old runner from stopping a
// replacement Work.
func (w *Work) Stop(assignmentID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if assignmentID != w.spec.AssignmentID {
		return ErrAssignmentConflict
	}
	w.active = false
	w.pending = false
	return nil
}

func (w *Work) UpdateResume(assignmentID, resumeRef string, resumeRevision int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if assignmentID != w.spec.AssignmentID {
		return ErrAssignmentConflict
	}
	if resumeRevision <= w.spec.Session.ResumeRevision {
		return nil
	}
	w.spec.Session.ResumeRef = resumeRef
	w.spec.Session.ResumeRevision = resumeRevision
	return nil
}

func (w *Work) Snapshot() Snapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Snapshot{Spec: w.spec, Active: w.active, Pending: w.pending}
}

// Manager owns the process-local Work set for one Agentlet.
type Manager struct {
	mu       sync.Mutex
	capacity int
	works    map[string]*Work
}

func NewManager(capacity int) *Manager {
	return &Manager{capacity: capacity, works: make(map[string]*Work)}
}

// Ensure reserves one capacity slot for an accepted Assignment. Repeating the
// same Assignment is idempotent; a replacement is allowed only while inactive.
//
// +spec=`Agentlet reserves one Work slot per accepted Assignment, rejects capacity overflow, and never replaces an active Assignment`
// +link=agentd/docs/agentlet.md
func (m *Manager) Ensure(spec Spec) (*Work, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.works[spec.Session.ID]; current != nil {
		current.mu.Lock()
		defer current.mu.Unlock()
		if current.spec.AssignmentID != spec.AssignmentID {
			if current.active {
				return nil, false, ErrAssignmentConflict
			}
			replacement := &Work{spec: spec}
			m.works[spec.Session.ID] = replacement
			return replacement, true, nil
		}
		if spec.Session.ResumeRevision <= current.spec.Session.ResumeRevision {
			spec.Session.ResumeRef = current.spec.Session.ResumeRef
			spec.Session.ResumeRevision = current.spec.Session.ResumeRevision
		}
		current.spec = spec
		return current, false, nil
	}
	if len(m.works) >= m.capacity {
		return nil, false, ErrCapacity
	}
	created := &Work{spec: spec}
	m.works[spec.Session.ID] = created
	return created, true, nil
}

// Wake atomically resolves the current Work and records execution demand, so a
// stale pointer cannot start after the Manager has accepted a replacement.
func (m *Manager) Wake(sessionID, assignmentID string) (*Work, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.works[sessionID]
	if current == nil {
		return nil, false, ErrNotFound
	}
	start, err := current.Wake(assignmentID)
	return current, start, err
}

func (m *Manager) Snapshot(sessionID string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.works[sessionID]
	if current == nil {
		return Snapshot{}, ErrNotFound
	}
	return current.Snapshot(), nil
}

func (m *Manager) Snapshots() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]Snapshot, 0, len(m.works))
	for _, current := range m.works {
		values = append(values, current.Snapshot())
	}
	return values
}

func (m *Manager) Delete(sessionID, assignmentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.works[sessionID]
	if current == nil {
		return ErrNotFound
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.spec.AssignmentID != assignmentID {
		return ErrAssignmentConflict
	}
	if current.active {
		return ErrActive
	}
	delete(m.works, sessionID)
	return nil
}
