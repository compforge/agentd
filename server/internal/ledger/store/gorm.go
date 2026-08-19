package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	gomysql "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMStore struct {
	db *gorm.DB
}

var _ agentledger.EventStore = (*GORMStore)(nil)

type streamRow struct {
	SessionID string `gorm:"primaryKey;size:191"`
	StreamID  string `gorm:"primaryKey;size:191"`
	Version   int64  `gorm:"not null"`
}

func (streamRow) TableName() string { return "agentd_ledger_streams" }

type sessionRow struct {
	SessionID string `gorm:"primaryKey;size:191"`
	Cursor    int64  `gorm:"not null"`
}

func (sessionRow) TableName() string { return "agentd_ledger_sessions" }

type eventRow struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	SessionID     string `gorm:"size:191;not null;uniqueIndex:idx_agentd_ledger_stream_version,priority:1;uniqueIndex:idx_agentd_ledger_session_cursor,priority:1;uniqueIndex:idx_agentd_ledger_event_id,priority:1"`
	StreamID      string `gorm:"size:191;not null;uniqueIndex:idx_agentd_ledger_stream_version,priority:2"`
	StreamVersion int64  `gorm:"not null;uniqueIndex:idx_agentd_ledger_stream_version,priority:3"`
	SessionCursor int64  `gorm:"not null;uniqueIndex:idx_agentd_ledger_session_cursor,priority:2"`
	EventID       string `gorm:"size:191;not null;uniqueIndex:idx_agentd_ledger_event_id,priority:2"`
	Payload       []byte `gorm:"not null"`
}

func (eventRow) TableName() string { return "agentd_ledger_events" }

type receiptRow struct {
	SessionID string `gorm:"primaryKey;size:191"`
	StreamID  string `gorm:"primaryKey;size:191"`
	AppendID  string `gorm:"primaryKey;size:191"`
	Digest    string `gorm:"size:128;not null"`
	Payload   []byte `gorm:"not null"`
}

func (receiptRow) TableName() string { return "agentd_ledger_receipts" }

func NewGORM(db *gorm.DB) (*GORMStore, error) {
	if db == nil {
		return nil, errors.New("create GORM ledger store: database is required")
	}
	if err := db.AutoMigrate(&streamRow{}, &sessionRow{}, &eventRow{}, &receiptRow{}); err != nil {
		return nil, fmt.Errorf("migrate ledger store: %w", err)
	}
	return &GORMStore{db: db}, nil
}

func (s *GORMStore) Append(
	ctx context.Context,
	stream agentledger.EventStream,
	expectedVersion int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.CommitReceipt, error) {
	if len(events) == 0 {
		return agentledger.CommitReceipt{}, errors.New("append requires at least one event")
	}
	batch, err := clone(events)
	if err != nil {
		return agentledger.CommitReceipt{}, fmt.Errorf("snapshot append batch: %w", err)
	}
	seen := make(map[string]struct{}, len(batch))
	for _, event := range batch {
		if event.SessionID != stream.SessionID {
			return agentledger.CommitReceipt{}, errors.New("all events must belong to the target stream's session")
		}
		if _, duplicate := seen[event.EventID]; duplicate {
			return agentledger.CommitReceipt{}, fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.EventID)
		}
		seen[event.EventID] = struct{}{}
	}
	digest, err := agentledger.CanonicalAppendDigest(batch)
	if err != nil {
		return agentledger.CommitReceipt{}, err
	}

	var receipt agentledger.CommitReceipt
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		currentStream, err := lockStream(tx, stream)
		if err != nil {
			return err
		}
		currentSession, err := lockSession(tx, stream.SessionID)
		if err != nil {
			return err
		}
		var storedReceipt receiptRow
		result := tx.Where("session_id = ? AND stream_id = ? AND append_id = ?", stream.SessionID, stream.StreamID, appendID).
			Limit(1).Find(&storedReceipt)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			if storedReceipt.Digest != digest {
				return agentledger.ErrIdempotencyViolation
			}
			return json.Unmarshal(storedReceipt.Payload, &receipt)
		}
		if currentStream.Version != expectedVersion {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrStreamConflict, expectedVersion, currentStream.Version)
		}
		for eventID := range seen {
			var count int64
			if err := tx.Model(&eventRow{}).Where("session_id = ? AND event_id = ?", stream.SessionID, eventID).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, eventID)
			}
		}

		committedAt := time.Now().UTC().Format(time.RFC3339Nano)
		storedEvents := make([]agentledger.StoredEvent, 0, len(batch))
		for index, event := range batch {
			stored := agentledger.StoredEvent{
				ProposedEvent: event,
				StreamID:      stream.StreamID, StreamVersion: currentStream.Version + int64(index) + 1,
				CommitCursor: strconv.FormatInt(currentSession.Cursor+int64(index)+1, 10), CommittedAt: committedAt,
			}
			payload, err := json.Marshal(stored)
			if err != nil {
				return fmt.Errorf("encode stored event: %w", err)
			}
			cursor, _ := strconv.ParseInt(stored.CommitCursor, 10, 64)
			if err := tx.Create(&eventRow{
				SessionID: stream.SessionID, StreamID: stream.StreamID,
				StreamVersion: stored.StreamVersion, SessionCursor: cursor,
				EventID: event.EventID, Payload: payload,
			}).Error; err != nil {
				return err
			}
			storedEvents = append(storedEvents, stored)
		}
		currentStream.Version = storedEvents[len(storedEvents)-1].StreamVersion
		currentSession.Cursor, _ = strconv.ParseInt(storedEvents[len(storedEvents)-1].CommitCursor, 10, 64)
		if err := tx.Save(&currentStream).Error; err != nil {
			return err
		}
		if err := tx.Save(&currentSession).Error; err != nil {
			return err
		}
		receipt = agentledger.CommitReceipt{
			Stream: stream, AppendID: appendID, Digest: digest,
			FirstVersion: storedEvents[0].StreamVersion, LastVersion: storedEvents[len(storedEvents)-1].StreamVersion,
			FirstCursor: storedEvents[0].CommitCursor, LastCursor: storedEvents[len(storedEvents)-1].CommitCursor,
			CommittedAt: committedAt,
		}
		for _, event := range storedEvents {
			receipt.EventIDs = append(receipt.EventIDs, event.EventID)
		}
		payload, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		return tx.Create(&receiptRow{
			SessionID: stream.SessionID, StreamID: stream.StreamID,
			AppendID: appendID, Digest: digest, Payload: payload,
		}).Error
	})
	if err != nil {
		var mysqlErr *gomysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			err = agentledger.ErrDuplicateEvent
		}
		return agentledger.CommitReceipt{}, fmt.Errorf("append GORM event batch: %w", err)
	}
	return clone(receipt)
}

func (s *GORMStore) Load(
	ctx context.Context,
	stream agentledger.EventStream,
	afterVersion int64,
) iter.Seq2[agentledger.StoredEvent, error] {
	return s.read(ctx, "session_id = ? AND stream_id = ? AND stream_version > ?", []any{
		stream.SessionID, stream.StreamID, afterVersion,
	}, "stream_version")
}

func (s *GORMStore) ScanSession(
	ctx context.Context,
	sessionID, afterCursor string,
) iter.Seq2[agentledger.StoredEvent, error] {
	after := int64(-1)
	if afterCursor != "" {
		value, err := strconv.ParseInt(afterCursor, 10, 64)
		if err != nil || value < 0 {
			return errorSequence(fmt.Errorf("invalid cursor %q", afterCursor))
		}
		after = value
	}
	return s.read(ctx, "session_id = ? AND session_cursor > ?", []any{sessionID, after}, "session_cursor")
}

func (s *GORMStore) read(ctx context.Context, query string, args []any, order string) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		var rows []eventRow
		if err := s.db.WithContext(ctx).Where(query, args...).Order(order).Find(&rows).Error; err != nil {
			yield(agentledger.StoredEvent{}, fmt.Errorf("read GORM event stream: %w", err))
			return
		}
		for _, row := range rows {
			var event agentledger.StoredEvent
			if err := json.Unmarshal(row.Payload, &event); err != nil {
				yield(agentledger.StoredEvent{}, fmt.Errorf("decode stored event: %w", err))
				return
			}
			if !yield(event, nil) {
				return
			}
		}
	}
}

func lockStream(tx *gorm.DB, stream agentledger.EventStream) (streamRow, error) {
	initial := streamRow{SessionID: stream.SessionID, StreamID: stream.StreamID, Version: -1}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&initial).Error; err != nil {
		return streamRow{}, err
	}
	var current streamRow
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("session_id = ? AND stream_id = ?", stream.SessionID, stream.StreamID).First(&current).Error
	return current, err
}

func lockSession(tx *gorm.DB, sessionID string) (sessionRow, error) {
	initial := sessionRow{SessionID: sessionID, Cursor: -1}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&initial).Error; err != nil {
		return sessionRow{}, err
	}
	var current sessionRow
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("session_id = ?", sessionID).First(&current).Error
	return current, err
}

func clone[T any](value T) (T, error) {
	var result T
	payload, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(payload, &result); err != nil {
		return result, err
	}
	return result, nil
}

func errorSequence(err error) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		yield(agentledger.StoredEvent{}, err)
	}
}
