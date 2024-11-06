package natsmutex

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const defaultBucket = "distributed_locks"
const defaultTtl = 30 * time.Second
const defaultBackoff = 100 * time.Millisecond

// Mutex implements a distributed lock using NATS JetStream.
type Mutex struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	kv      nats.KeyValue
	bucket  string
	key     string
	owner   string
	ttl     time.Duration
	backoff time.Duration
}

// Option is a functional option type for configuring Mutex.
type Option func(*Mutex)

// WithNatsConn injects an existing NATS connection.
func WithNatsConn(nc *nats.Conn) Option {
	return func(m *Mutex) {
		m.nc = nc
	}
}

// WithJetStream sets a custom JetStream context.
func WithJetStream(js nats.JetStreamContext) Option {
	return func(m *Mutex) {
		m.js = js
	}
}

// WithKeyValue sets a custom KeyValue store.
func WithKeyValue(kv nats.KeyValue) Option {
	return func(m *Mutex) {
		m.kv = kv
	}
}

// WithBucket sets a custom bucket name.
func WithBucket(bucket string) Option {
	return func(m *Mutex) {
		m.bucket = bucket
	}
}

// WithKey sets a custom key for the lock.
func WithKey(key string) Option {
	return func(m *Mutex) {
		m.key = key
	}
}

// WithOwner sets a custom owner ID.
func WithOwner(owner string) Option {
	return func(m *Mutex) {
		m.owner = owner
	}
}

// WithTTL sets a custom TTL for the lock.
func WithTTL(ttl time.Duration) Option {
	return func(m *Mutex) {
		m.ttl = ttl
	}
}

// WithBackoff sets a custom backoff duration.
func WithBackoff(backoff time.Duration) Option {
	return func(m *Mutex) {
		m.backoff = backoff
	}
}

// NewMutex creates a new Mutex with the specified options.
func NewMutex(opts ...Option) (*Mutex, error) {
	m := &Mutex{
		bucket:  defaultBucket,
		ttl:     defaultTtl,
		backoff: defaultBackoff,
	}

	// Apply each option to configure Mutex.
	for _, opt := range opts {
		opt(m)
	}

	// Generate a unique key if not provided.
	if m.key == "" {
		if uid, err := uuid.NewV7(); err == nil {
			m.key = uid.String()
		} else {
			return nil, fmt.Errorf("failed to generate unique key: %w", err)
		}
	}

	// Generate a unique owner ID if not provided.
	if m.owner == "" {
		if uid, err := uuid.NewV7(); err == nil {
			m.owner = uid.String()
		} else {
			return nil, fmt.Errorf("failed to generate unique owner ID: %w", err)
		}
	}

	// Ensure that a NATS connection, JetStream context or key-value store is provided.
	if m.nc == nil && m.js == nil && m.kv == nil {
		return nil, errors.New("nats connection, jetstream context or key-value store must be provided")
	}

	// Create a new JetStreamContext if needed.
	if m.kv == nil && m.js == nil && m.nc != nil {
		js, err := m.nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to create JetStream context: %w", err)
		}
		m.js = js
	}

	// Create a new KeyValue store if not provided.
	if m.kv == nil && m.js != nil {
		kv, err := m.js.KeyValue(m.bucket)
		if err != nil {
			// Try to create if it doesn't exist
			kv, err = m.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: m.bucket,
				TTL:    m.ttl,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create KeyValue store: %w", err)
			}
		}
		m.kv = kv
	}

	return m, nil
}

// SyncMutex is a wrapper around Mutex that implements sync.Locker interface.
func (m *Mutex) SyncMutex() SyncMutex {
	return SyncMutex{m: m}
}

// Lock tries to acquire the lock, blocking until it’s available.
func (m *Mutex) Lock() error {
	for {
		res, err := m.TryLock()
		if err != nil {
			return err
		}
		if res {
			return nil
		}
		time.Sleep(m.backoff)
	}
}

// TryLock attempts to acquire the lock without blocking.
// Returns true if lock was acquired, false otherwise.
func (m *Mutex) TryLock() (bool, error) {
	_, err := m.kv.Create(m.key, []byte(m.owner))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, nats.ErrKeyExists) {
		return false, nil
	}
	return false, err
}

// Unlock releases the lock if it's held by this instance.
func (m *Mutex) Unlock() error {
	entry, err := m.kv.Get(m.key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("failed to retrieve lock state: %w", err)
	}
	if string(entry.Value()) != m.owner {
		return errors.New("lock is not held by this instance, cannot unlock")
	}

	if err := m.kv.Delete(m.key); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}
