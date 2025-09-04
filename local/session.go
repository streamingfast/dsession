package local

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streamingfast/dsession"
	"go.uber.org/zap"
)

type sessionInfo struct {
	sessionKey  string
	userID      string
	apiKeyID    string
	traceID     string
	borrowedAt  time.Time
	serviceName string
	workers     atomic.Int64
}

// Get borrows a session from the local pool
func (p *LocalSessionPool) Get(
	ctx context.Context,
	serviceName string,
	userID string,
	apiKeyID string,
	traceID string,
	_ func(error),
) (string, error) {
	p.logger.Debug("get session request",
		zap.String("service", serviceName),
		zap.String("user_id", userID),
		zap.String("api_key_id", apiKeyID),
		zap.String("trace_id", traceID))

	p.mu.Lock()
	defer p.mu.Unlock()

	// Count current borrowed sessions
	borrowedCount := int64(len(p.borrowedSessions))

	// Check if we've hit the max sessions limit
	if borrowedCount >= p.maxSessions {
		p.logger.Info("concurrent stream limit exceeded (max sessions reached)",
			zap.Int64("borrowed", borrowedCount),
			zap.Int64("max", p.maxSessions))
		return "", dsession.ErrConcurrentStreamLimitExceeded
	}

	// Check user-specific session limit
	userSessionCount := p.userSessions[userID]
	if userSessionCount >= p.maxSessionsPerUser {
		p.logger.Info("concurrent stream limit exceeded (max sessions per user reached)",
			zap.String("user_id", userID),
			zap.Int64("user_sessions", userSessionCount),
			zap.Int64("max_per_user", p.maxSessionsPerUser))
		return "", dsession.ErrConcurrentStreamLimitExceeded
	}

	// Generate a new session key
	sessionNum := p.sessionCounter.Add(1)
	sessionKey := fmt.Sprintf("local-session-%d", sessionNum)

	// Store the session info
	p.borrowedSessions[sessionKey] = &sessionInfo{
		sessionKey:  sessionKey,
		userID:      userID,
		apiKeyID:    apiKeyID,
		traceID:     traceID,
		serviceName: serviceName,
		borrowedAt:  time.Now(),
	}

	// Update counters
	if traceID != "" {
		p.traceIDSessions[traceID]++
	}
	p.userSessions[userID]++

	p.logger.Debug("session borrowed",
		zap.String("session_key", sessionKey),
		zap.Int64("available", p.maxSessions-borrowedCount-1),
		zap.Int64("borrowed", borrowedCount+1),
		zap.Int64("max", p.maxSessions),
		zap.Int64("user_sessions", p.userSessions[userID]))

	return sessionKey, nil
}

// Release returns a session to the local pool
func (p *LocalSessionPool) Release(sessionKey string) {
	if sessionKey == "" {
		p.logger.Error("session key cannot be empty")
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	info, exists := p.borrowedSessions[sessionKey]
	if !exists {
		p.logger.Warn("releasing unknown session",
			zap.String("session_key", sessionKey),
			zap.Error(fmt.Errorf("session key %s not found", sessionKey)))
		return
	}

	// Update counters
	if info.traceID != "" {
		if count, ok := p.traceIDSessions[info.traceID]; ok && count > 0 {
			p.traceIDSessions[info.traceID]--
			if p.traceIDSessions[info.traceID] == 0 {
				delete(p.traceIDSessions, info.traceID)
			}
		}
	}

	if count, ok := p.userSessions[info.userID]; ok && count > 0 {
		p.userSessions[info.userID]--
		if p.userSessions[info.userID] == 0 {
			delete(p.userSessions, info.userID)
		}
	}

	// Remove the session
	delete(p.borrowedSessions, sessionKey)

	duration := time.Since(info.borrowedAt)
	p.logger.Debug("session released",
		zap.String("session_key", sessionKey),
		zap.String("user_id", info.userID),
		zap.String("service", info.serviceName),
		zap.Duration("held_duration", duration))
}

// GetStats returns statistics about the pool (kept for debugging, not part of interface)
func (p *LocalSessionPool) GetStats() (borrowed int, available int64, traceIDs int, users int) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	borrowed = len(p.borrowedSessions)
	available = p.maxSessions - int64(borrowed)
	traceIDs = len(p.traceIDSessions)
	users = len(p.userSessions)
	return
}
