package natsmutex

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// Option is a functional option type for configuring Mutex.
type Option func(*Mutex) error

// WithUrl creates NATS connection using provided url.
func WithUrl(url string) Option {
	return func(m *Mutex) error {
		nc, err := nats.Connect(url)
		if err != nil {
			return fmt.Errorf("failed to connect to NATS server: %w", err)
		}
		m.nc = nc
		return nil
	}
}

// WithConn injects an existing NATS connection.
func WithConn(nc *nats.Conn) Option {
	return func(m *Mutex) error {
		m.nc = nc
		return nil
	}
}

// WithJetStream sets a custom JetStream context.
func WithJetStream(js nats.JetStreamContext) Option {
	return func(m *Mutex) error {
		m.js = js
		return nil
	}
}

// WithKeyValue sets a custom KeyValue store.
func WithKeyValue(kv nats.KeyValue) Option {
	return func(m *Mutex) error {
		m.kv = kv
		return nil
	}
}

// WithBucket sets a custom bucket name.
func WithBucket(bucket string) Option {
	return func(m *Mutex) error {
		m.bucket = bucket
		return nil
	}
}

// WithResourceID sets a custom key for the lock.
func WithResourceID(id string) Option {
	return func(m *Mutex) error {
		m.so = getSingleton(id)
		return nil
	}
}

// WithOwner sets a custom owner ID.
func WithOwner(owner uuid.UUID) Option {
	return func(m *Mutex) error {
		m.owner = owner
		return nil
	}
}

// WithTTL sets a custom TTL for the lock.
func WithTTL(ttl time.Duration) Option {
	return func(m *Mutex) error {
		m.ttl = ttl
		return nil
	}
}

// WithBackoff sets a custom backoff duration.
func WithBackoff(backoff time.Duration) Option {
	return func(m *Mutex) error {
		m.backoff = backoff
		return nil
	}
}
