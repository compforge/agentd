package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/compforge/agentd/server/internal/app"
	bolt "go.etcd.io/bbolt"
)

var (
	agentsBucket       = []byte("agents")
	environmentsBucket = []byte("environments")
	sessionsBucket     = []byte("sessions")
)

type BoltRepository struct {
	db *bolt.DB
}

func Open(path string, timeout time.Duration) (*BoltRepository, error) {
	if timeout <= 0 {
		return nil, errors.New("open resource store: timeout must be positive")
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("open resource store %q: %w", path, err)
	}
	return &BoltRepository{db: db}, nil
}

func (r *BoltRepository) Close() error {
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("close resource store: %w", err)
	}
	return nil
}

func (r *BoltRepository) PutAgent(ctx context.Context, value app.Agent) error {
	return put(ctx, r.db, agentsBucket, value.ID, value)
}

func (r *BoltRepository) GetAgent(ctx context.Context, id string) (app.Agent, error) {
	return get[app.Agent](ctx, r.db, agentsBucket, id)
}

func (r *BoltRepository) ListAgents(ctx context.Context) ([]app.Agent, error) {
	return list[app.Agent](ctx, r.db, agentsBucket)
}

func (r *BoltRepository) PutEnvironment(ctx context.Context, value app.Environment) error {
	return put(ctx, r.db, environmentsBucket, value.ID, value)
}

func (r *BoltRepository) GetEnvironment(ctx context.Context, id string) (app.Environment, error) {
	return get[app.Environment](ctx, r.db, environmentsBucket, id)
}

func (r *BoltRepository) ListEnvironments(ctx context.Context) ([]app.Environment, error) {
	return list[app.Environment](ctx, r.db, environmentsBucket)
}

func (r *BoltRepository) PutSession(ctx context.Context, value app.Session) error {
	return put(ctx, r.db, sessionsBucket, value.ID, value)
}

func (r *BoltRepository) GetSession(ctx context.Context, id string) (app.Session, error) {
	return get[app.Session](ctx, r.db, sessionsBucket, id)
}

func (r *BoltRepository) ListSessions(ctx context.Context) ([]app.Session, error) {
	return list[app.Session](ctx, r.db, sessionsBucket)
}

func put(ctx context.Context, db *bolt.DB, bucketName []byte, id string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode resource %q: %w", id, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(id), encoded)
	}); err != nil {
		return fmt.Errorf("persist resource %q: %w", id, err)
	}
	return nil
}

func get[T any](ctx context.Context, db *bolt.DB, bucketName []byte, id string) (T, error) {
	var value T
	if err := ctx.Err(); err != nil {
		return value, err
	}
	var encoded []byte
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil || bucket.Get([]byte(id)) == nil {
			return app.ErrNotFound
		}
		encoded = append([]byte(nil), bucket.Get([]byte(id))...)
		return nil
	})
	if err != nil {
		return value, fmt.Errorf("get resource %q: %w", id, err)
	}
	if err := json.Unmarshal(encoded, &value); err != nil {
		return value, fmt.Errorf("decode resource %q: %w", id, err)
	}
	return value, nil
}

func list[T any](ctx context.Context, db *bolt.DB, bucketName []byte) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var encodedValues [][]byte
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			encodedValues = append(encodedValues, append([]byte(nil), value...))
			return nil
		})
	}); err != nil {
		return nil, fmt.Errorf("list resources: %w", err)
	}
	values := make([]T, 0, len(encodedValues))
	for _, encoded := range encodedValues {
		var value T
		if err := json.Unmarshal(encoded, &value); err != nil {
			return nil, fmt.Errorf("decode listed resource: %w", err)
		}
		values = append(values, value)
	}
	return values, nil
}
