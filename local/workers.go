package local

import (
	"context"
	"fmt"

	"github.com/streamingfast/dsession"
	"go.uber.org/zap"
)

// GetWorker borrows a worker from the local pool
func (p *LocalSessionPool) GetWorker(
	ctx context.Context,
	serviceName string,
	requestKey string,
	maxWorkersPerSession int,
) (string, error) {
	p.logger.Debug("get worker request",
		zap.String("service", serviceName),
		zap.String("request_key", requestKey),
		zap.Int("max_workers_per_session", maxWorkersPerSession))

	p.mu.Lock()
	defer p.mu.Unlock()

	sess, ok := p.borrowedSessions[requestKey]
	if !ok {
		p.logger.Warn("worker request for unknown session",
			zap.String("request_key", requestKey))
		return "", dsession.ErrSessionNotFound
	}

	// Atomically check and increment global worker counter
	workerNum := p.workerCounter.Add(1)
	if workerNum > p.maxWorkers {
		// Rollback and return error
		p.workerCounter.Add(-1)
		p.logger.Debug("global max workers reached",
			zap.Int64("would_be", workerNum),
			zap.Int64("max", p.maxWorkers))
		return "", dsession.ErrWorkersLimitExceeded
	}

	// Check per-user worker limit
	userWorkers := p.orgWorkers[sess.organizationID]
	if userWorkers >= p.maxWorkersPerOrg {
		// Rollback global counter
		p.workerCounter.Add(-1)
		p.logger.Debug("max workers per user reached",
			zap.String("user_id", sess.organizationID),
			zap.Int64("user_workers", userWorkers),
			zap.Int64("max_per_user", p.maxWorkersPerOrg))
		return "", dsession.ErrWorkersLimitExceeded
	}

	// Check per-session worker limit (use the smaller of the two limits)
	effectiveLimit := int64(maxWorkersPerSession)
	if p.maxWorkersPerSession < effectiveLimit {
		effectiveLimit = p.maxWorkersPerSession
	}

	sessionWorkerNum := sess.workers.Add(1)
	if sessionWorkerNum > effectiveLimit {
		// Rollback counters
		sess.workers.Add(-1)
		p.workerCounter.Add(-1)
		p.logger.Debug("max workers per session reached",
			zap.String("session_key", requestKey),
			zap.Int64("would_be_session_workers", sessionWorkerNum),
			zap.Int64("max_per_session", effectiveLimit))
		return "", dsession.ErrWorkersLimitExceeded
	}

	workerKey := fmt.Sprintf("%s-w%d", requestKey, sess.workerSeqID.Add(1))

	// Track the worker-to-session mapping and update user worker count
	p.workerToSession[workerKey] = workerMapping{
		sessionKey:     requestKey,
		organizationID: sess.organizationID,
	}
	p.orgWorkers[sess.organizationID]++

	p.logger.Debug("worker borrowed",
		zap.String("worker_key", workerKey),
		zap.String("session_key", requestKey),
		zap.Int64("session_workers", sessionWorkerNum),
		zap.Int64("global_workers", workerNum))

	return workerKey, nil
}

// ReleaseWorker releases a worker back to the local pool
func (p *LocalSessionPool) ReleaseWorker(workerKey string) {
	if workerKey == "" {
		p.logger.Error("worker key cannot be empty")
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the session this worker belongs to
	mapping, exists := p.workerToSession[workerKey]
	if !exists {
		p.logger.Warn("releasing unknown worker",
			zap.String("worker_key", workerKey))
		return
	}

	// Remove the worker from the mapping
	delete(p.workerToSession, workerKey)

	// Always decrement the global worker counter
	globalWorkers := p.workerCounter.Add(-1)

	// Always decrement org workers using the stored organizationID
	if count, ok := p.orgWorkers[mapping.organizationID]; ok && count > 0 {
		p.orgWorkers[mapping.organizationID]--
		if p.orgWorkers[mapping.organizationID] == 0 {
			delete(p.orgWorkers, mapping.organizationID)
		}
	}

	// Find the session and decrement its worker count (if session still exists)
	sess, sessionExists := p.borrowedSessions[mapping.sessionKey]
	if !sessionExists {
		// Session was already released, but we've already decremented global and org counters above
		p.logger.Warn("worker released but session already gone",
			zap.String("worker_key", workerKey),
			zap.String("session_key", mapping.sessionKey),
			zap.Int64("remaining_global_workers", globalWorkers))
		return
	}

	// Decrement session-specific counter
	sessionWorkers := sess.workers.Add(-1)

	p.logger.Debug("worker released",
		zap.String("worker_key", workerKey),
		zap.String("session_key", mapping.sessionKey),
		zap.Int64("remaining_session_workers", sessionWorkers),
		zap.Int64("remaining_global_workers", globalWorkers))
}
