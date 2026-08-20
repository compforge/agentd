package work

import (
	"errors"
	"sync"

	"github.com/compforge/agentd/agentlet/internal/execution"
)

var (
	ErrAssignmentConflict = errors.New("work assignment conflict")
	ErrCapacity           = errors.New("work capacity exhausted")
	ErrNotFound           = errors.New("work not found")
)

// Spec is the control-plane slice needed to execute one durable Work.
type Spec struct {
	AssignmentID string
	Session      execution.Session
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

func (m *Manager) Ensure(spec Spec) (*Work, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.works[spec.Session.ID]; current != nil {
		current.mu.Lock()
		defer current.mu.Unlock()
		if current.spec.AssignmentID != spec.AssignmentID {
			return nil, false, ErrAssignmentConflict
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
	delete(m.works, sessionID)
	return nil
}
