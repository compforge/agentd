package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormio "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMRepository struct {
	db *gormio.DB
}

var _ repo.Repository = (*GORMRepository)(nil)

type workerRow struct {
	ID             string     `gorm:"primaryKey;size:64"`
	Name           string     `gorm:"not null;size:255"`
	MaxRuns        int        `gorm:"not null"`
	Phase          string     `gorm:"not null;size:32;index"`
	ObserverStatus []byte     `gorm:"type:json"`
	IdleSince      *time.Time `gorm:"index"`
	AbsentAt       *time.Time `gorm:"index"`
	CreatedAt      time.Time  `gorm:"not null"`
	UpdatedAt      time.Time  `gorm:"not null"`
}

func (workerRow) TableName() string { return "workers" }

type sessionRow struct {
	ID           string  `gorm:"primaryKey;size:191"`
	Status       string  `gorm:"not null;size:32;index"`
	AssignmentID *string `gorm:"size:64"`
	WorkerID     *string `gorm:"size:64;index"`
	AssignedAt   *time.Time
	CreatedAt    time.Time `gorm:"not null"`
	UpdatedAt    time.Time `gorm:"not null"`
}

func (sessionRow) TableName() string { return "sessions" }

func NewGORM(db *gormio.DB) (*GORMRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("create control-plane repository: database is required")
	}
	if err := db.AutoMigrate(&workerRow{}, &sessionRow{}, &resourceLockRow{}); err != nil {
		return nil, fmt.Errorf("migrate control-plane store: %w", err)
	}
	return &GORMRepository{db: db}, nil
}

func (r *GORMRepository) Transaction(ctx context.Context, operation func(repo.Repository) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		return operation(&GORMRepository{db: tx})
	})
}

func (r *GORMRepository) PutWorker(ctx context.Context, worker model.Worker) error {
	row := workerToRow(worker)
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put worker %q: %w", worker.ID, err)
	}
	return nil
}

func (r *GORMRepository) GetWorker(ctx context.Context, workerID string) (model.Worker, error) {
	return r.getWorker(r.db.WithContext(ctx), workerID)
}

func (r *GORMRepository) GetWorkerForUpdate(ctx context.Context, workerID string) (model.Worker, error) {
	return r.getWorker(r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}), workerID)
}

func (r *GORMRepository) getWorker(query *gormio.DB, workerID string) (model.Worker, error) {
	var row workerRow
	if err := query.Where("id = ?", workerID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Worker{}, repo.ErrNotFound
		}
		return model.Worker{}, fmt.Errorf("get worker %q: %w", workerID, err)
	}
	return row.worker(), nil
}

func (r *GORMRepository) ListWorkers(ctx context.Context) ([]model.Worker, error) {
	return r.listWorkers(r.db.WithContext(ctx).Order("id ASC"))
}

func (r *GORMRepository) ListWorkersForUpdate(ctx context.Context) ([]model.Worker, error) {
	return r.listWorkers(r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Order("id ASC"))
}

func (r *GORMRepository) listWorkers(query *gormio.DB) ([]model.Worker, error) {
	var rows []workerRow
	err := query.Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	workers := make([]model.Worker, 0, len(rows))
	for _, row := range rows {
		workers = append(workers, row.worker())
	}
	return workers, nil
}

func (r *GORMRepository) PutSession(ctx context.Context, session model.Session) error {
	row := sessionToRow(session)
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put session %q: %w", session.ID, err)
	}
	return nil
}

func (r *GORMRepository) GetSession(ctx context.Context, sessionID string) (model.Session, error) {
	return r.getSession(r.db.WithContext(ctx), sessionID)
}

func (r *GORMRepository) GetSessionForUpdate(ctx context.Context, sessionID string) (model.Session, error) {
	return r.getSession(r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}), sessionID)
}

func (r *GORMRepository) getSession(query *gormio.DB, sessionID string) (model.Session, error) {
	var row sessionRow
	if err := query.Where("id = ?", sessionID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Session{}, repo.ErrNotFound
		}
		return model.Session{}, fmt.Errorf("get session %q: %w", sessionID, err)
	}
	return row.session(), nil
}

func (r *GORMRepository) CountPendingSessions(ctx context.Context) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&sessionRow{}).
		Where("status = ? AND worker_id IS NULL", model.SessionStatusRescheduling).
		Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count pending sessions: %w", err)
	}
	return count, nil
}

func (r *GORMRepository) CountWorkerSessions(ctx context.Context, workerID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&sessionRow{}).Where("worker_id = ?", workerID).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count sessions for worker %q: %w", workerID, err)
	}
	return count, nil
}

func (r *GORMRepository) DeleteRetiredWorkersBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	var rows []workerRow
	if err := r.db.WithContext(ctx).
		Where("phase = ? AND absent_at IS NOT NULL AND updated_at <= ?", model.WorkerPhaseRetired, cutoff).
		Order("updated_at ASC, id ASC").Limit(limit).Find(&rows).Error; err != nil {
		return 0, fmt.Errorf("list retired Workers for record GC: %w", err)
	}
	var deleted int64
	for _, row := range rows {
		err := r.db.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
			var sessions int64
			if err := tx.Model(&sessionRow{}).Where("worker_id = ?", row.ID).Count(&sessions).Error; err != nil {
				return err
			}
			if sessions != 0 {
				return nil
			}
			result := tx.Where(
				"id = ? AND phase = ? AND absent_at IS NOT NULL AND updated_at <= ?",
				row.ID, model.WorkerPhaseRetired, cutoff,
			).Delete(&workerRow{})
			if result.Error != nil {
				return result.Error
			}
			deleted += result.RowsAffected
			return nil
		})
		if err != nil {
			return deleted, fmt.Errorf("delete retired Worker %q: %w", row.ID, err)
		}
	}
	return deleted, nil
}

func workerToRow(worker model.Worker) workerRow {
	return workerRow{
		ID: worker.ID, Name: worker.Name,
		MaxRuns: worker.MaxRuns, Phase: string(worker.Phase), ObserverStatus: worker.ObserverStatus,
		IdleSince: worker.IdleSince, AbsentAt: worker.AbsentAt,
		CreatedAt: worker.CreatedAt, UpdatedAt: worker.UpdatedAt,
	}
}

func (r workerRow) worker() model.Worker {
	return model.Worker{
		ID: r.ID, Name: r.Name,
		MaxRuns: r.MaxRuns, Phase: model.WorkerPhase(r.Phase), ObserverStatus: r.ObserverStatus,
		IdleSince: r.IdleSince, AbsentAt: r.AbsentAt,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func sessionToRow(session model.Session) sessionRow {
	return sessionRow{
		ID: session.ID, Status: string(session.Status),
		AssignmentID: optionalString(session.AssignmentID), WorkerID: optionalString(session.WorkerID),
		AssignedAt: session.AssignedAt, CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
	}
}

func (r sessionRow) session() model.Session {
	return model.Session{
		ID: r.ID, Status: model.SessionStatus(r.Status),
		AssignmentID: stringValue(r.AssignmentID), WorkerID: stringValue(r.WorkerID),
		AssignedAt: r.AssignedAt, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
