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

// workerMapping stores the session key and organization ID for a worker
// This allows proper cleanup of all counters even if the session is released first
type workerMapping struct {
	sessionKey     string
	organizationID string
}

// LocalSessionPool is a local in-memory implementation of the session pool
type LocalSessionPool struct {
	logger *zap.Logger

	// Configuration
	maxSessions          int64
	maxSessionsPerOrg    int64
	maxWorkers           int64
	maxWorkersPerOrg     int64
	maxWorkersPerSession int64

	// Runtime state
	mu                   sync.RWMutex
	borrowedSessions     map[string]*sessionInfo
	sessionCounter       atomic.Int64
	workerCounter        atomic.Int64
	traceIDSessions      map[string]int64
	organizationSessions map[string]int64
	orgWorkers           map[string]int64
	workerToSession      map[string]workerMapping
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
		maxSessionsPerOrg:    10,
		maxWorkers:           100,
		maxWorkersPerOrg:     100,
		maxWorkersPerSession: 100,
		borrowedSessions:     make(map[string]*sessionInfo),
		traceIDSessions:      make(map[string]int64),
		organizationSessions: make(map[string]int64),
		orgWorkers:           make(map[string]int64),
		workerToSession:      make(map[string]workerMapping),
	}

	// Parse URL parameters for configuration
	params := u.Query()

	if maxSessionsStr := params.Get("max_sessions"); maxSessionsStr != "" {
		if maxSessions, err := strconv.ParseInt(maxSessionsStr, 10, 64); err == nil {
			pool.maxSessions = maxSessions
		}
	}

	maxSessionsPerOrgStr := params.Get("max_sessions_per_organization")
	if maxSessionsPerOrgStr == "" {
		// Keep backward compatibility for old name for now
		maxSessionsPerOrgStr = params.Get("max_sessions_per_user")
	}

	if maxSessionsPerOrgStr != "" {
		if maxSessionsPerOrg, err := strconv.ParseInt(maxSessionsPerOrgStr, 10, 64); err == nil {
			pool.maxSessionsPerOrg = maxSessionsPerOrg
		}
	}

	if maxWorkersStr := params.Get("max_workers"); maxWorkersStr != "" {
		if maxWorkers, err := strconv.ParseInt(maxWorkersStr, 10, 64); err == nil {
			pool.maxWorkers = maxWorkers
		}
	}

	maxWorkersPerOrgStr := params.Get("max_workers_per_organization")
	if maxWorkersPerOrgStr == "" {
		// Keep backward compatibility for old name for now
		maxWorkersPerOrgStr = params.Get("max_workers_per_user")
	}

	if maxWorkersPerOrgStr != "" {
		if maxWorkersPerOrg, err := strconv.ParseInt(maxWorkersPerOrgStr, 10, 64); err == nil {
			pool.maxWorkersPerOrg = maxWorkersPerOrg
		}
	}

	if maxWorkersPerSessionStr := params.Get("max_workers_per_session"); maxWorkersPerSessionStr != "" {
		if maxWorkersPerSession, err := strconv.ParseInt(maxWorkersPerSessionStr, 10, 64); err == nil {
			pool.maxWorkersPerSession = maxWorkersPerSession
		}
	}

	logger.Info("local session pool initialized",
		zap.Int64("max_sessions", pool.maxSessions),
		zap.Int64("max_sessions_per_organization", pool.maxSessionsPerOrg),
		zap.Int64("max_workers", pool.maxWorkers),
		zap.Int64("max_workers_per_session", pool.maxWorkersPerSession),
		zap.Int64("max_workers_per_organization", pool.maxWorkersPerOrg))

	return pool, nil
}
