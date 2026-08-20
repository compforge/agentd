package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/agentd/agentlet/internal/app"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMRepository struct {
	db *gorm.DB
}

var _ app.Repository = (*GORMRepository)(nil)

type resourceRow struct {
	Kind    string `gorm:"primaryKey;size:32"`
	ID      string `gorm:"primaryKey;size:191"`
	Payload []byte `gorm:"not null"`
}

func (resourceRow) TableName() string { return "agentd_resources" }

func NewGORM(db *gorm.DB) (*GORMRepository, error) {
	if db == nil {
		return nil, errors.New("create GORM resource repository: database is required")
	}
	if err := db.AutoMigrate(&resourceRow{}); err != nil {
		return nil, fmt.Errorf("migrate resource store: %w", err)
	}
	return &GORMRepository{db: db}, nil
}

func (r *GORMRepository) PutAgent(ctx context.Context, value app.Agent) error {
	return r.put(ctx, "agent", value.ID, value)
}

func (r *GORMRepository) GetAgent(ctx context.Context, id string) (app.Agent, error) {
	return getResource[app.Agent](ctx, r.db, "agent", id)
}

func (r *GORMRepository) ListAgents(ctx context.Context) ([]app.Agent, error) {
	return listResources[app.Agent](ctx, r.db, "agent")
}

func (r *GORMRepository) PutEnvironment(ctx context.Context, value app.Environment) error {
	return r.put(ctx, "environment", value.ID, value)
}

func (r *GORMRepository) GetEnvironment(ctx context.Context, id string) (app.Environment, error) {
	return getResource[app.Environment](ctx, r.db, "environment", id)
}

func (r *GORMRepository) ListEnvironments(ctx context.Context) ([]app.Environment, error) {
	return listResources[app.Environment](ctx, r.db, "environment")
}

func (r *GORMRepository) PutSession(ctx context.Context, value app.Session) error {
	return r.put(ctx, "session", value.ID, value)
}

func (r *GORMRepository) GetSession(ctx context.Context, id string) (app.Session, error) {
	return getResource[app.Session](ctx, r.db, "session", id)
}

func (r *GORMRepository) ListSessions(ctx context.Context) ([]app.Session, error) {
	return listResources[app.Session](ctx, r.db, "session")
}

func (r *GORMRepository) put(ctx context.Context, kind, id string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode %s %q: %w", kind, id, err)
	}
	row := resourceRow{Kind: kind, ID: id, Payload: payload}
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kind"}, {Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"payload"}),
	}).Create(&row).Error; err != nil {
		return fmt.Errorf("persist %s %q: %w", kind, id, err)
	}
	return nil
}

func getResource[T any](ctx context.Context, db *gorm.DB, kind, id string) (T, error) {
	var value T
	var row resourceRow
	if err := db.WithContext(ctx).Where("kind = ? AND id = ?", kind, id).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return value, app.ErrNotFound
		}
		return value, fmt.Errorf("get %s %q: %w", kind, id, err)
	}
	if err := json.Unmarshal(row.Payload, &value); err != nil {
		return value, fmt.Errorf("decode %s %q: %w", kind, id, err)
	}
	return value, nil
}

func listResources[T any](ctx context.Context, db *gorm.DB, kind string) ([]T, error) {
	var rows []resourceRow
	if err := db.WithContext(ctx).Where("kind = ?", kind).Order("id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list %s resources: %w", kind, err)
	}
	values := make([]T, 0, len(rows))
	for _, row := range rows {
		var value T
		if err := json.Unmarshal(row.Payload, &value); err != nil {
			return nil, fmt.Errorf("decode %s %q: %w", kind, row.ID, err)
		}
		values = append(values, value)
	}
	return values, nil
}
