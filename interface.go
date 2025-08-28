package dsession

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"go.uber.org/zap"
)

// ErrUnavailable is returned when the session pool service cannot be reached
var ErrUnavailable = errors.New("Unavailable")

// ErrPermissionDenied is returned when the userID or keyID has been disabled for that service
var ErrPermissionDenied = errors.New("Permission denied")

// ErrQuotaExceeded is returned when the quota for the service has been exceeded (ex: too many bytes read on a strict quota)
var ErrQuotaExceeded = errors.New("Quota exceeded")

// ErrConcurrentStreamLimitExceeded is returned when no session is available for the given service
var ErrConcurrentStreamLimitExceeded = errors.New("Concurrent stream limit exceeded")

// SessionPool is the main interface for managing session pool sessions
type SessionPool interface {
	// Get borrows a session from the pool, returning a key that must be used to release the session back to the pool.
	// If an error happens during the session (quota exceeded or key gets disabled), the onError function will be called with the error.
	// The returned error that can be unwrapped as one of ErrUnavailable, ErrPermissionDenied, ErrQuotaExceeded, or ErrConcurrentStreamLimitExceeded
	Get(ctx context.Context, serviceName string, userID string, apiKeyID string, traceID string, onError func(error)) (key string, err error)

	// Release returns a session to the pool
	Release(sessionKey, apiKeyID string)
}

var registry = make(map[string]FactoryFunc)

// FactoryFunc is the function signature for session pool factory functions
type FactoryFunc func(config string, logger *zap.Logger) (SessionPool, error)

// New creates a new SessionPool instance based on the configuration URL scheme
func New(config string, logger *zap.Logger) (SessionPool, error) {
	u, err := url.Parse(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config URL: %w", err)
	}

	factory := registry[u.Scheme]
	if factory == nil {
		return nil, fmt.Errorf("no SessionPool plugin named %q is currently registered", u.Scheme)
	}

	return factory(config, logger)
}

// Register registers a new SessionPool factory with the given name
func Register(name string, factory FactoryFunc) {
	registry[name] = factory
}
