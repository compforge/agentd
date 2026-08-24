package gorm

import (
	"context"
	"encoding/json"
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

type agentRow struct {
	ID               string     `gorm:"primaryKey;size:191"`
	CurrentVersionID string     `gorm:"not null;size:191;index"`
	ArchivedAt       *time.Time `gorm:"index"`
	CreatedAt        time.Time  `gorm:"not null"`
	UpdatedAt        time.Time  `gorm:"not null"`
}

func (agentRow) TableName() string { return "agents" }

type agentVersionRow struct {
	ID          string    `gorm:"primaryKey;size:191"`
	AgentID     string    `gorm:"not null;size:191;uniqueIndex:agent_version_number"`
	Version     int64     `gorm:"not null;uniqueIndex:agent_version_number"`
	Name        string    `gorm:"not null;size:255"`
	Description string    `gorm:"type:text"`
	ModelID     string    `gorm:"not null;size:191"`
	System      string    `gorm:"type:text"`
	Tools       []byte    `gorm:"type:json;not null"`
	Metadata    []byte    `gorm:"type:json;not null"`
	CreatedAt   time.Time `gorm:"not null"`
}

func (agentVersionRow) TableName() string { return "agent_versions" }

type modelRow struct {
	ID         string    `gorm:"primaryKey;size:191"`
	Provider   string    `gorm:"not null;size:64"`
	UpstreamID string    `gorm:"column:model_id;not null;size:191"`
	BaseURL    string    `gorm:"size:2048"`
	APIKey     string    `gorm:"type:text;not null"`
	CreatedAt  time.Time `gorm:"not null"`
	UpdatedAt  time.Time `gorm:"not null"`
}

func (modelRow) TableName() string { return "models" }

type environmentRow struct {
	ID          string    `gorm:"primaryKey;size:191"`
	Name        string    `gorm:"not null;size:255"`
	Description string    `gorm:"type:text"`
	Config      []byte    `gorm:"type:json;not null"`
	Metadata    []byte    `gorm:"type:json;not null"`
	CreatedAt   time.Time `gorm:"not null"`
	UpdatedAt   time.Time `gorm:"not null"`
}

func (environmentRow) TableName() string { return "environments" }

type workerRow struct {
	ID             string     `gorm:"primaryKey;size:64"`
	Name           string     `gorm:"not null;size:255"`
	Capacity       int        `gorm:"not null"`
	Phase          string     `gorm:"not null;size:32;index"`
	ObserverStatus []byte     `gorm:"type:json"`
	IdleSince      *time.Time `gorm:"index"`
	AbsentAt       *time.Time `gorm:"index"`
	CreatedAt      time.Time  `gorm:"not null"`
	UpdatedAt      time.Time  `gorm:"not null"`
}

func (workerRow) TableName() string { return "workers" }

type sessionRow struct {
	ID             string  `gorm:"primaryKey;size:191"`
	AgentID        string  `gorm:"not null;size:191;index"`
	AgentVersionID string  `gorm:"not null;size:191;index"`
	EnvironmentID  string  `gorm:"not null;size:191;index"`
	Title          string  `gorm:"size:255"`
	Metadata       []byte  `gorm:"type:json;not null"`
	Status         string  `gorm:"not null;size:32;index"`
	Revision       int64   `gorm:"not null"`
	Harness        string  `gorm:"size:64"`
	HarnessVersion string  `gorm:"size:64"`
	ResumeRef      string  `gorm:"size:191"`
	ResumeRevision int64   `gorm:"not null"`
	ObserverStatus []byte  `gorm:"type:json"`
	AssignmentID   *string `gorm:"size:64"`
	WorkerID       *string `gorm:"size:64;index"`
	LastWorkerID   *string `gorm:"size:64"`
	AssignedAt     *time.Time
	ArchivedAt     *time.Time `gorm:"index"`
	CreatedAt      time.Time  `gorm:"not null"`
	UpdatedAt      time.Time  `gorm:"not null"`
}

func (sessionRow) TableName() string { return "sessions" }

func NewGORM(db *gormio.DB) (*GORMRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("create control-plane repository: database is required")
	}
	if err := db.AutoMigrate(&modelRow{}, &agentRow{}, &agentVersionRow{}, &environmentRow{}, &sessionRow{}, &workerRow{}, &resourceLockRow{}); err != nil {
		return nil, fmt.Errorf("migrate control-plane store: %w", err)
	}
	return &GORMRepository{db: db}, nil
}

func (r *GORMRepository) PutModel(ctx context.Context, value model.Model) error {
	row := modelRow{
		ID: value.ID, Provider: value.Provider, UpstreamID: value.UpstreamID,
		BaseURL: value.BaseURL, APIKey: value.APIKey,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put model %q: %w", value.ID, err)
	}
	return nil
}

func (r *GORMRepository) GetModel(ctx context.Context, modelID string) (model.Model, error) {
	var row modelRow
	if err := r.db.WithContext(ctx).Where("id = ?", modelID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Model{}, repo.ErrNotFound
		}
		return model.Model{}, fmt.Errorf("get model %q: %w", modelID, err)
	}
	return model.Model{
		ID: row.ID, Provider: row.Provider, UpstreamID: row.UpstreamID,
		BaseURL: row.BaseURL, APIKey: row.APIKey,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *GORMRepository) ListModels(ctx context.Context) ([]model.Model, error) {
	var rows []modelRow
	if err := r.db.WithContext(ctx).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list models: %w", err)
	}
	values := make([]model.Model, 0, len(rows))
	for _, row := range rows {
		values = append(values, model.Model{
			ID: row.ID, Provider: row.Provider, UpstreamID: row.UpstreamID,
			BaseURL: row.BaseURL, APIKey: row.APIKey,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		})
	}
	return values, nil
}

func (r *GORMRepository) ListModelsPage(ctx context.Context, page repo.PageQuery) (repo.Page[model.Model], error) {
	var rows []modelRow
	if err := pageQuery(r.db.WithContext(ctx).Order("created_at, id"), page).Find(&rows).Error; err != nil {
		return repo.Page[model.Model]{}, fmt.Errorf("list models page: %w", err)
	}
	values := make([]model.Model, 0, len(rows))
	for _, row := range rows {
		values = append(values, model.Model{
			ID: row.ID, Provider: row.Provider, UpstreamID: row.UpstreamID,
			BaseURL: row.BaseURL, APIKey: row.APIKey,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		})
	}
	return repo.NewPage(values, page.Limit), nil
}

func (r *GORMRepository) PutAgent(ctx context.Context, agent model.Agent) error {
	row := agentRow{
		ID: agent.ID, CurrentVersionID: agent.VersionID, ArchivedAt: agent.ArchivedAt,
		CreatedAt: agent.CreatedAt, UpdatedAt: agent.UpdatedAt,
	}
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put agent %q: %w", agent.ID, err)
	}
	return nil
}

func (r *GORMRepository) CreateAgentVersion(ctx context.Context, agent model.Agent) error {
	row, err := agentToVersionRow(agent)
	if err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("create agent %q version %d: %w", agent.ID, agent.Version, err)
	}
	return nil
}

func (r *GORMRepository) GetAgent(ctx context.Context, agentID string) (model.Agent, error) {
	return r.getAgent(r.db.WithContext(ctx), agentID)
}

func (r *GORMRepository) GetAgentForUpdate(ctx context.Context, agentID string) (model.Agent, error) {
	return r.getAgent(r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}), agentID)
}

func (r *GORMRepository) getAgent(query *gormio.DB, agentID string) (model.Agent, error) {
	var row agentRow
	if err := query.Where("id = ?", agentID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("get agent %q: %w", agentID, err)
	}
	return r.agentFromVersion(r.db.WithContext(query.Statement.Context), row, row.CurrentVersionID)
}

func (r *GORMRepository) GetAgentVersion(ctx context.Context, versionID string) (model.Agent, error) {
	var version agentVersionRow
	if err := r.db.WithContext(ctx).Where("id = ?", versionID).First(&version).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("get agent version %q: %w", versionID, err)
	}
	var agent agentRow
	if err := r.db.WithContext(ctx).Where("id = ?", version.AgentID).First(&agent).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("get agent %q for version %q: %w", version.AgentID, versionID, err)
	}
	return resolveAgentVersion(agent, version)
}

func (r *GORMRepository) FindAgentVersion(ctx context.Context, agentID string, versionNumber int64) (model.Agent, error) {
	var agent agentRow
	if err := r.db.WithContext(ctx).Where("id = ?", agentID).First(&agent).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("get agent %q: %w", agentID, err)
	}
	var version agentVersionRow
	if err := r.db.WithContext(ctx).
		Where("agent_id = ? AND version = ?", agentID, versionNumber).First(&version).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("find agent %q version %d: %w", agentID, versionNumber, err)
	}
	return resolveAgentVersion(agent, version)
}

func (r *GORMRepository) ListAgents(ctx context.Context) ([]model.Agent, error) {
	var rows []agentRow
	if err := r.db.WithContext(ctx).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	values := make([]model.Agent, 0, len(rows))
	for _, row := range rows {
		value, err := r.agentFromVersion(r.db.WithContext(ctx), row, row.CurrentVersionID)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *GORMRepository) ListAgentsPage(
	ctx context.Context,
	page repo.PageQuery,
	includeArchived bool,
) (repo.Page[model.Agent], error) {
	var rows []agentRow
	query := r.db.WithContext(ctx).Order("created_at, id")
	if !includeArchived {
		query = query.Where("archived_at IS NULL")
	}
	if err := pageQuery(query, page).Find(&rows).Error; err != nil {
		return repo.Page[model.Agent]{}, fmt.Errorf("list agents page: %w", err)
	}
	values := make([]model.Agent, 0, len(rows))
	for _, row := range rows {
		value, err := r.agentFromVersion(r.db.WithContext(ctx), row, row.CurrentVersionID)
		if err != nil {
			return repo.Page[model.Agent]{}, err
		}
		values = append(values, value)
	}
	return repo.NewPage(values, page.Limit), nil
}

func (r *GORMRepository) ListAgentVersions(ctx context.Context, agentID string) ([]model.Agent, error) {
	var agent agentRow
	if err := r.db.WithContext(ctx).Where("id = ?", agentID).First(&agent).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return nil, repo.ErrNotFound
		}
		return nil, fmt.Errorf("get agent %q: %w", agentID, err)
	}
	var rows []agentVersionRow
	if err := r.db.WithContext(ctx).
		Where("agent_id = ?", agentID).Order("version DESC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list agent %q versions: %w", agentID, err)
	}
	values := make([]model.Agent, 0, len(rows))
	for _, row := range rows {
		value, err := resolveAgentVersion(agent, row)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *GORMRepository) ListAgentVersionsPage(
	ctx context.Context,
	agentID string,
	page repo.PageQuery,
) (repo.Page[model.Agent], error) {
	var agent agentRow
	if err := r.db.WithContext(ctx).Where("id = ?", agentID).First(&agent).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return repo.Page[model.Agent]{}, repo.ErrNotFound
		}
		return repo.Page[model.Agent]{}, fmt.Errorf("get agent %q: %w", agentID, err)
	}
	var rows []agentVersionRow
	query := r.db.WithContext(ctx).Where("agent_id = ?", agentID).Order("version DESC")
	if err := pageQuery(query, page).Find(&rows).Error; err != nil {
		return repo.Page[model.Agent]{}, fmt.Errorf("list agent %q versions page: %w", agentID, err)
	}
	values := make([]model.Agent, 0, len(rows))
	for _, row := range rows {
		value, err := resolveAgentVersion(agent, row)
		if err != nil {
			return repo.Page[model.Agent]{}, err
		}
		values = append(values, value)
	}
	return repo.NewPage(values, page.Limit), nil
}

func (r *GORMRepository) agentFromVersion(query *gormio.DB, agent agentRow, versionID string) (model.Agent, error) {
	var version agentVersionRow
	if err := query.Where("id = ?", versionID).First(&version).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Agent{}, repo.ErrNotFound
		}
		return model.Agent{}, fmt.Errorf("get current version %q for agent %q: %w", versionID, agent.ID, err)
	}
	return resolveAgentVersion(agent, version)
}

func resolveAgentVersion(identity agentRow, version agentVersionRow) (model.Agent, error) {
	value, err := version.agent(identity)
	if err == nil && version.ID == identity.CurrentVersionID {
		value.UpdatedAt = identity.UpdatedAt
	}
	return value, err
}

func (r *GORMRepository) PutEnvironment(ctx context.Context, environment model.Environment) error {
	row, err := environmentToRow(environment)
	if err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("put environment %q: %w", environment.ID, err)
	}
	return nil
}

func (r *GORMRepository) GetEnvironment(ctx context.Context, environmentID string) (model.Environment, error) {
	var row environmentRow
	if err := r.db.WithContext(ctx).Where("id = ?", environmentID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Environment{}, repo.ErrNotFound
		}
		return model.Environment{}, fmt.Errorf("get environment %q: %w", environmentID, err)
	}
	return row.environment()
}

func (r *GORMRepository) ListEnvironments(ctx context.Context) ([]model.Environment, error) {
	var rows []environmentRow
	if err := r.db.WithContext(ctx).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list environments: %w", err)
	}
	values := make([]model.Environment, 0, len(rows))
	for _, row := range rows {
		value, err := row.environment()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *GORMRepository) ListEnvironmentsPage(
	ctx context.Context,
	page repo.PageQuery,
) (repo.Page[model.Environment], error) {
	var rows []environmentRow
	if err := pageQuery(r.db.WithContext(ctx).Order("created_at, id"), page).Find(&rows).Error; err != nil {
		return repo.Page[model.Environment]{}, fmt.Errorf("list environments page: %w", err)
	}
	values := make([]model.Environment, 0, len(rows))
	for _, row := range rows {
		value, err := row.environment()
		if err != nil {
			return repo.Page[model.Environment]{}, err
		}
		values = append(values, value)
	}
	return repo.NewPage(values, page.Limit), nil
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
	row, err := sessionToRow(session)
	if err != nil {
		return err
	}
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

func (r *GORMRepository) ListSessions(ctx context.Context) ([]model.Session, error) {
	var rows []sessionRow
	if err := r.db.WithContext(ctx).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	values := make([]model.Session, 0, len(rows))
	for _, row := range rows {
		value, err := row.session()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *GORMRepository) ListSessionsPage(
	ctx context.Context,
	page repo.PageQuery,
	includeArchived bool,
) (repo.Page[model.Session], error) {
	var rows []sessionRow
	query := r.db.WithContext(ctx)
	if !includeArchived {
		query = query.Where("archived_at IS NULL")
	}
	if err := pageQuery(query.Order(pageOrder(page)), page).Find(&rows).Error; err != nil {
		return repo.Page[model.Session]{}, fmt.Errorf("list sessions page: %w", err)
	}
	values := make([]model.Session, 0, len(rows))
	for _, row := range rows {
		value, err := row.session()
		if err != nil {
			return repo.Page[model.Session]{}, err
		}
		values = append(values, value)
	}
	return repo.NewPage(values, page.Limit), nil
}

func pageQuery(query *gormio.DB, page repo.PageQuery) *gormio.DB {
	return query.Offset(page.Offset).Limit(page.Limit + 1)
}

func pageOrder(page repo.PageQuery) string {
	if page.Descending {
		return "created_at DESC, id DESC"
	}
	return "created_at, id"
}

func (r *GORMRepository) getSession(query *gormio.DB, sessionID string) (model.Session, error) {
	var row sessionRow
	if err := query.Where("id = ?", sessionID).First(&row).Error; err != nil {
		if errors.Is(err, gormio.ErrRecordNotFound) {
			return model.Session{}, repo.ErrNotFound
		}
		return model.Session{}, fmt.Errorf("get session %q: %w", sessionID, err)
	}
	return row.session()
}

func (r *GORMRepository) CountWorkerSessions(ctx context.Context, workerID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&sessionRow{}).Where("worker_id = ?", workerID).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count sessions for worker %q: %w", workerID, err)
	}
	return count, nil
}

// DeleteRetiredWorkersBefore removes one bounded metadata batch without
// treating record deletion as Worker Pod deletion.
//
// +spec=`Only retired Worker rows whose Pod absence predates the cutoff and which have no bound Session are deleted`
// +link=agentd/docs/agentd.md
func (r *GORMRepository) DeleteRetiredWorkersBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	var rows []workerRow
	if err := r.db.WithContext(ctx).
		Where("phase = ? AND absent_at IS NOT NULL AND absent_at <= ?", model.WorkerPhaseRetired, cutoff).
		Order("absent_at ASC, id ASC").Limit(limit).Find(&rows).Error; err != nil {
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
				"id = ? AND phase = ? AND absent_at IS NOT NULL AND absent_at <= ?",
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
		Capacity: worker.Capacity, Phase: string(worker.Phase), ObserverStatus: worker.ObserverStatus,
		IdleSince: worker.IdleSince, AbsentAt: worker.AbsentAt,
		CreatedAt: worker.CreatedAt, UpdatedAt: worker.UpdatedAt,
	}
}

func agentToVersionRow(agent model.Agent) (agentVersionRow, error) {
	tools, err := json.Marshal(agent.Tools)
	if err != nil {
		return agentVersionRow{}, fmt.Errorf("encode agent %q tools: %w", agent.ID, err)
	}
	metadata, err := json.Marshal(agent.Metadata)
	if err != nil {
		return agentVersionRow{}, fmt.Errorf("encode agent %q metadata: %w", agent.ID, err)
	}
	return agentVersionRow{
		ID: agent.VersionID, AgentID: agent.ID, Version: agent.Version,
		Name: agent.Name, Description: agent.Description, ModelID: agent.ModelID,
		System: agent.System, Tools: tools, Metadata: metadata,
		CreatedAt: agent.UpdatedAt,
	}, nil
}

func (r agentVersionRow) agent(identity agentRow) (model.Agent, error) {
	value := model.Agent{
		ID: identity.ID, VersionID: r.ID,
		Name: r.Name, Description: r.Description, ModelID: r.ModelID,
		System: r.System, Version: r.Version, ArchivedAt: identity.ArchivedAt,
		CreatedAt: identity.CreatedAt, UpdatedAt: r.CreatedAt,
	}
	if err := json.Unmarshal(r.Tools, &value.Tools); err != nil {
		return model.Agent{}, fmt.Errorf("decode agent %q version %d tools: %w", identity.ID, r.Version, err)
	}
	if err := json.Unmarshal(r.Metadata, &value.Metadata); err != nil {
		return model.Agent{}, fmt.Errorf("decode agent %q version %d metadata: %w", identity.ID, r.Version, err)
	}
	return value, nil
}

func environmentToRow(environment model.Environment) (environmentRow, error) {
	config, err := json.Marshal(environment.Config)
	if err != nil {
		return environmentRow{}, fmt.Errorf("encode environment %q config: %w", environment.ID, err)
	}
	metadata, err := json.Marshal(environment.Metadata)
	if err != nil {
		return environmentRow{}, fmt.Errorf("encode environment %q metadata: %w", environment.ID, err)
	}
	return environmentRow{
		ID: environment.ID, Name: environment.Name, Description: environment.Description,
		Config: config, Metadata: metadata, CreatedAt: environment.CreatedAt, UpdatedAt: environment.UpdatedAt,
	}, nil
}

func (r environmentRow) environment() (model.Environment, error) {
	value := model.Environment{
		ID: r.ID, Name: r.Name, Description: r.Description, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
	if err := json.Unmarshal(r.Config, &value.Config); err != nil {
		return model.Environment{}, fmt.Errorf("decode environment %q config: %w", r.ID, err)
	}
	if err := json.Unmarshal(r.Metadata, &value.Metadata); err != nil {
		return model.Environment{}, fmt.Errorf("decode environment %q metadata: %w", r.ID, err)
	}
	return value, nil
}

func (r workerRow) worker() model.Worker {
	return model.Worker{
		ID: r.ID, Name: r.Name,
		Capacity: r.Capacity, Phase: model.WorkerPhase(r.Phase), ObserverStatus: r.ObserverStatus,
		IdleSince: r.IdleSince, AbsentAt: r.AbsentAt,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func sessionToRow(session model.Session) (sessionRow, error) {
	metadata, err := json.Marshal(session.Metadata)
	if err != nil {
		return sessionRow{}, fmt.Errorf("encode session %q metadata: %w", session.ID, err)
	}
	return sessionRow{
		ID: session.ID, AgentID: session.AgentID, AgentVersionID: session.AgentVersionID,
		EnvironmentID: session.EnvironmentID, Title: session.Title, Metadata: metadata,
		Status: string(session.Status), Revision: session.Revision,
		Harness: session.Harness, HarnessVersion: session.HarnessVersion,
		ResumeRef: session.ResumeRef, ResumeRevision: session.ResumeRevision,
		ObserverStatus: session.ObserverStatus,
		AssignmentID:   optionalString(session.Placement.Fence), WorkerID: optionalString(session.Placement.WorkerID),
		LastWorkerID: optionalString(session.LastWorkerID),
		AssignedAt:   session.Placement.PlacedAt, ArchivedAt: session.ArchivedAt,
		CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
	}, nil
}

func (r sessionRow) session() (model.Session, error) {
	value := model.Session{
		ID: r.ID, AgentID: r.AgentID, AgentVersionID: r.AgentVersionID,
		EnvironmentID: r.EnvironmentID, Title: r.Title,
		Status: model.SessionStatus(r.Status), Revision: r.Revision,
		Harness: r.Harness, HarnessVersion: r.HarnessVersion,
		ResumeRef: r.ResumeRef, ResumeRevision: r.ResumeRevision,
		ObserverStatus: r.ObserverStatus,
		Placement: model.SessionPlacement{
			Fence: stringValue(r.AssignmentID), WorkerID: stringValue(r.WorkerID), PlacedAt: r.AssignedAt,
		},
		LastWorkerID: stringValue(r.LastWorkerID),
		ArchivedAt:   r.ArchivedAt,
		CreatedAt:    r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
	if err := json.Unmarshal(r.Metadata, &value.Metadata); err != nil {
		return model.Session{}, fmt.Errorf("decode session %q metadata: %w", r.ID, err)
	}
	return value, nil
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
