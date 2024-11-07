package natsmutex

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// Mutex implements a distributed lock using NATS JetStream.
type Mutex struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	kv      nats.KeyValue
	bucket  string
	owner   uuid.UUID
	ttl     time.Duration
	backoff time.Duration
	so      *singleton
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
		if err := opt(m); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Generate a unique resource id if not provided.
	if m.so == nil {
		uid, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("failed to generate unique resource ID: %w", err)
		}
		m.so = getSingleton(uid.String())
	}

	// Generate a unique owner ID if not provided.
	if m.owner == uuid.Nil {
		uid, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("failed to generate unique owner ID: %w", err)
		}
		m.owner = uid
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

// GetResourceID returns the unique key for the lock.
func (m *Mutex) GetResourceID() string {
	return m.so.id
}

// GetOwner returns the unique owner ID for the lock.
func (m *Mutex) GetOwner() uuid.UUID {
	return m.owner
}

// Lock tries to acquire the lock, blocking until it’s available.
func (m *Mutex) Lock() error {
	m.so.Lock()
	for {
		res, err := m.tryLock()
		if err != nil {
			m.so.Unlock()
			return err
		}
		if res {
			return nil
		}
		time.Sleep(m.backoff)
	}
}

// Unlock releases the lock if it's held by this instance.
func (m *Mutex) Unlock() error {
	entry, err := m.kv.Get(m.so.id)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			m.so.Unlock()
			return nil
		}
		return fmt.Errorf("failed to retrieve lock state: %w", err)
	}
	if !bytes.Equal(entry.Value(), m.owner[:]) {
		return errors.New("lock is not held by this instance, cannot unlock")
	}

	if err := m.kv.Delete(m.so.id); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	m.so.Unlock()
	return nil
}

// TryLock attempts to acquire the lock without blocking.
// Returns true if lock was acquired, false otherwise.
func (m *Mutex) TryLock() (bool, error) {
	if !m.so.TryLock() {
		return false, nil
	}

	acquired, err := m.tryLock()
	if err != nil {
		m.so.Unlock()
		return false, fmt.Errorf("failed to attempt lock acquisition: %w", err)
	}

	if !acquired {
		m.so.Unlock()
	}

	return acquired, nil
}

func (m *Mutex) tryLock() (bool, error) {
	_, err := m.kv.Create(m.so.id, m.owner[:])
	if err == nil {
		return true, nil
	}
	if errors.Is(err, nats.ErrKeyExists) {
		return false, nil
	}
	return false, err
}
