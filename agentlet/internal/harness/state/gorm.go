package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type GORMStore struct {
	db *gorm.DB
}

var _ Store = (*GORMStore)(nil)

type stateRow struct {
	ResumeRef   string    `gorm:"primaryKey;size:191"`
	Revision    int64     `gorm:"primaryKey;autoIncrement:false"`
	Format      string    `gorm:"size:255;not null"`
	Data        []byte    `gorm:"not null"`
	CommittedAt time.Time `gorm:"not null"`
}

func (stateRow) TableName() string { return "agentd_harness_states" }

func NewGORM(db *gorm.DB) (*GORMStore, error) {
	if db == nil {
		return nil, errors.New("create GORM harness state store: database is required")
	}
	if err := db.AutoMigrate(&stateRow{}); err != nil {
		return nil, fmt.Errorf("migrate harness state store: %w", err)
	}
	return &GORMStore{db: db}, nil
}

func (s *GORMStore) Append(
	ctx context.Context,
	resumeRef string,
	expectedRevision int64,
	format string,
	data json.RawMessage,
) (Record, error) {
	if resumeRef == "" || format == "" {
		return Record{}, errors.New("append harness state: resume ref and format are required")
	}
	if expectedRevision < -1 {
		return Record{}, errors.New("append harness state: expected revision must be -1 or greater")
	}
	record := Record{
		Revision: expectedRevision + 1, Format: format, Data: append([]byte(nil), data...), CommittedAt: time.Now().UTC(),
	}
	row := stateRow{
		ResumeRef: resumeRef, Revision: record.Revision, Format: format,
		Data: append([]byte(nil), data...), CommittedAt: record.CommittedAt,
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var latest stateRow
		result := tx.Where("resume_ref = ?", resumeRef).Order("revision DESC").Limit(1).Find(&latest)
		if result.Error != nil {
			return result.Error
		}
		actual := int64(-1)
		if result.RowsAffected > 0 {
			actual = latest.Revision
		}
		if actual != expectedRevision {
			return fmt.Errorf("%w: expected %d, actual %d", ErrConflict, expectedRevision, actual)
		}
		return tx.Create(&row).Error
	})
	if err != nil {
		var mysqlErr *gomysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			err = ErrConflict
		}
		return Record{}, fmt.Errorf("append harness state %q: %w", resumeRef, err)
	}
	return record, nil
}

func (s *GORMStore) Load(ctx context.Context, resumeRef string) ([]Record, error) {
	var rows []stateRow
	if err := s.db.WithContext(ctx).Where("resume_ref = ?", resumeRef).Order("revision").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("load harness state %q: %w", resumeRef, err)
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, Record{
			Revision: row.Revision, Format: row.Format,
			Data: append([]byte(nil), row.Data...), CommittedAt: row.CommittedAt,
		})
	}
	return records, nil
}
