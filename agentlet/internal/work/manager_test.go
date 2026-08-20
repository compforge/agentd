package work

import (
	"errors"
	"testing"

	"github.com/compforge/agentd/agentlet/internal/harness"
)

func TestManagerFencesAssignmentsAndCoalescesWake(t *testing.T) {
	manager := NewManager(1)
	spec := Spec{AssignmentID: "assignment-1", Session: harness.Session{ID: "session-1"}}
	resident, created, err := manager.Ensure(spec)
	if err != nil || !created {
		t.Fatalf("Ensure() = created %t, err %v", created, err)
	}
	if start, err := resident.Wake(spec.AssignmentID); err != nil || !start {
		t.Fatalf("first Wake() = start %t, err %v", start, err)
	}
	if start, err := resident.Wake(spec.AssignmentID); err != nil || start {
		t.Fatalf("second Wake() = start %t, err %v", start, err)
	}
	if !resident.FinishPass() || resident.FinishPass() {
		t.Fatal("coalesced wake did not produce exactly one additional pass")
	}

	stale := spec
	stale.AssignmentID = "assignment-2"
	if _, _, err := manager.Ensure(stale); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("Ensure(stale) error = %v", err)
	}
	if err := manager.Delete(spec.Session.ID, stale.AssignmentID); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("Delete(stale) error = %v", err)
	}
}

func TestManagerCapacityAndResumeRevision(t *testing.T) {
	manager := NewManager(1)
	spec := Spec{AssignmentID: "assignment-1", Session: harness.Session{ID: "session-1", ResumeRevision: 2}}
	resident, _, err := manager.Ensure(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.Ensure(Spec{AssignmentID: "assignment-2", Session: harness.Session{ID: "session-2"}}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Ensure(over capacity) error = %v", err)
	}
	if err := resident.UpdateResume(spec.AssignmentID, "checkpoint-3", 3); err != nil {
		t.Fatal(err)
	}
	older := spec
	older.Session.ResumeRef = "checkpoint-2"
	if _, _, err := manager.Ensure(older); err != nil {
		t.Fatal(err)
	}
	if got := resident.Snapshot().Spec.Session; got.ResumeRef != "checkpoint-3" || got.ResumeRevision != 3 {
		t.Fatalf("resume point = %q/%d", got.ResumeRef, got.ResumeRevision)
	}
}
