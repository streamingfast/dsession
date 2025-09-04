package local

import (
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/streamingfast/dsession"
	"go.uber.org/zap"
)

func init() {
	dsession.Register("local", NewLocalSessionPool)
}

// LocalSessionPool is a local in-memory implementation of the session pool
type LocalSessionPool struct {
	logger *zap.Logger

	// Configuration
	maxSessions          int64
	maxSessionsPerUser   int64
	maxWorkers           int64
	maxWorkersPerUser    int64
	maxWorkersPerSession int64

	// Runtime state
	mu               sync.RWMutex
	borrowedSessions map[string]*sessionInfo
	sessionCounter   atomic.Int64
	workerCounter    atomic.Int64
	traceIDSessions  map[string]int64
	userSessions     map[string]int64
	userWorkers      map[string]int64
	workerToSession  map[string]string
}

// NewLocalSessionPool creates a new local session pool
func NewLocalSessionPool(config string, logger *zap.Logger) (dsession.SessionPool, error) {
	u, err := url.Parse(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config URL: %w", err)
	}

	pool := &LocalSessionPool{
		logger:               logger.With(zap.String("component", "local_session_pool")),
		maxSessions:          100, // defaults
		maxSessionsPerUser:   10,
		maxWorkers:           100,
		maxWorkersPerUser:    100,
		maxWorkersPerSession: 100,
		borrowedSessions:     make(map[string]*sessionInfo),
		traceIDSessions:      make(map[string]int64),
		userSessions:         make(map[string]int64),
		userWorkers:          make(map[string]int64),
		workerToSession:      make(map[string]string),
	}

	// Parse URL parameters for configuration
	params := u.Query()

	if maxSessionsStr := params.Get("max_sessions"); maxSessionsStr != "" {
		if maxSessions, err := strconv.ParseInt(maxSessionsStr, 10, 64); err == nil {
			pool.maxSessions = maxSessions
		}
	}

	if maxSessionsPerUserStr := params.Get("max_sessions_per_user"); maxSessionsPerUserStr != "" {
		if maxSessionsPerUser, err := strconv.ParseInt(maxSessionsPerUserStr, 10, 64); err == nil {
			pool.maxSessionsPerUser = maxSessionsPerUser
		}
	}

	if maxWorkersStr := params.Get("max_workers"); maxWorkersStr != "" {
		if maxWorkers, err := strconv.ParseInt(maxWorkersStr, 10, 64); err == nil {
			pool.maxWorkers = maxWorkers
		}
	}

	if maxWorkersPerUserStr := params.Get("max_workers_per_user"); maxWorkersPerUserStr != "" {
		if maxWorkersPerUser, err := strconv.ParseInt(maxWorkersPerUserStr, 10, 64); err == nil {
			pool.maxWorkersPerUser = maxWorkersPerUser
		}
	}

	if maxWorkersPerSessionStr := params.Get("max_workers_per_session"); maxWorkersPerSessionStr != "" {
		if maxWorkersPerSession, err := strconv.ParseInt(maxWorkersPerSessionStr, 10, 64); err == nil {
			pool.maxWorkersPerSession = maxWorkersPerSession
		}
	}

	logger.Info("local session pool initialized",
		zap.Int64("max_sessions", pool.maxSessions),
		zap.Int64("max_sessions_per_user", pool.maxSessionsPerUser),
		zap.Int64("max_workers", pool.maxWorkers),
		zap.Int64("max_workers_per_session", pool.maxWorkersPerSession),
		zap.Int64("max_workers_per_user", pool.maxWorkersPerUser))

	return pool, nil
}
