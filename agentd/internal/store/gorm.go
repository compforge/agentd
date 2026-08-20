package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/compforge/agentd/agentd/internal/app"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMRepository struct {
	db *gorm.DB
}

var _ app.Repository = (*GORMRepository)(nil)

type workerRow struct {
	ID             string    `gorm:"primaryKey;size:64"`
	Name           string    `gorm:"not null;size:255"`
	MaxRuns        int       `gorm:"not null"`
	ObserverStatus []byte    `gorm:"type:json"`
	CreatedAt      time.Time `gorm:"not null"`
	UpdatedAt      time.Time `gorm:"not null"`
}

func (workerRow) TableName() string { return "workers" }

type assignmentRow struct {
	ID        string    `gorm:"primaryKey;size:64"`
	SessionID string    `gorm:"not null;size:191;uniqueIndex"`
	WorkerID  string    `gorm:"not null;size:64;index"`
	CreatedAt time.Time `gorm:"not null"`
	UpdatedAt time.Time `gorm:"not null"`
}

func (assignmentRow) TableName() string { return "assignments" }

func NewGORM(db *gorm.DB) (*GORMRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("create control-plane repository: database is required")
	}
	if err := db.AutoMigrate(&workerRow{}, &assignmentRow{}); err != nil {
		return nil, fmt.Errorf("migrate control-plane store: %w", err)
	}
	return &GORMRepository{db: db}, nil
}

func (r *GORMRepository) Transaction(ctx context.Context, operation func(app.Repository) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return operation(&GORMRepository{db: tx})
	})
}

func (r *GORMRepository) PutWorker(ctx context.Context, worker app.Worker) error {
	row := workerToRow(worker)
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put worker %q: %w", worker.ID, err)
	}
	return nil
}

func (r *GORMRepository) GetWorker(ctx context.Context, workerID string) (app.Worker, error) {
	var row workerRow
	if err := r.db.WithContext(ctx).Where("id = ?", workerID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return app.Worker{}, app.ErrNotFound
		}
		return app.Worker{}, fmt.Errorf("get worker %q: %w", workerID, err)
	}
	return row.worker(), nil
}

func (r *GORMRepository) ListWorkers(ctx context.Context) ([]app.Worker, error) {
	return r.listWorkers(r.db.WithContext(ctx).Order("id ASC"))
}

func (r *GORMRepository) ListWorkersForUpdate(ctx context.Context) ([]app.Worker, error) {
	return r.listWorkers(r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Order("id ASC"))
}

func (r *GORMRepository) listWorkers(query *gorm.DB) ([]app.Worker, error) {
	var rows []workerRow
	err := query.Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	workers := make([]app.Worker, 0, len(rows))
	for _, row := range rows {
		workers = append(workers, row.worker())
	}
	return workers, nil
}

func (r *GORMRepository) GetAssignment(ctx context.Context, sessionID string) (app.Assignment, error) {
	var row assignmentRow
	if err := r.db.WithContext(ctx).Where("session_id = ?", sessionID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return app.Assignment{}, app.ErrNotFound
		}
		return app.Assignment{}, fmt.Errorf("get assignment for session %q: %w", sessionID, err)
	}
	return row.assignment(), nil
}

func (r *GORMRepository) CountAssignments(ctx context.Context, workerID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&assignmentRow{}).Where("worker_id = ?", workerID).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count assignments for worker %q: %w", workerID, err)
	}
	return count, nil
}

func (r *GORMRepository) PutAssignment(ctx context.Context, assignment app.Assignment) error {
	row := assignmentToRow(assignment)
	if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("put assignment %q: %w", assignment.ID, err)
	}
	return nil
}

func (r *GORMRepository) DeleteAssignment(ctx context.Context, sessionID string) error {
	if err := r.db.WithContext(ctx).Where("session_id = ?", sessionID).Delete(&assignmentRow{}).Error; err != nil {
		return fmt.Errorf("delete assignment for session %q: %w", sessionID, err)
	}
	return nil
}

func workerToRow(worker app.Worker) workerRow {
	return workerRow{
		ID: worker.ID, Name: worker.Name,
		MaxRuns: worker.MaxRuns, ObserverStatus: worker.ObserverStatus,
		CreatedAt: worker.CreatedAt, UpdatedAt: worker.UpdatedAt,
	}
}

func (r workerRow) worker() app.Worker {
	return app.Worker{
		ID: r.ID, Name: r.Name,
		MaxRuns: r.MaxRuns, ObserverStatus: r.ObserverStatus,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func assignmentToRow(assignment app.Assignment) assignmentRow {
	return assignmentRow{
		ID: assignment.ID, SessionID: assignment.SessionID, WorkerID: assignment.WorkerID,
		CreatedAt: assignment.CreatedAt, UpdatedAt: assignment.UpdatedAt,
	}
}

func (r assignmentRow) assignment() app.Assignment {
	return app.Assignment{
		ID: r.ID, SessionID: r.SessionID, WorkerID: r.WorkerID,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}
