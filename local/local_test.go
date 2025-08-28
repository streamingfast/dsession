package local

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/dsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewLocalSessionPool(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name                   string
		config                 string
		wantMaxSessions        int64
		wantMaxSessionsPerUser int64
		wantErr                bool
	}{
		{
			name:                   "default configuration",
			config:                 "local://",
			wantMaxSessions:        100,
			wantMaxSessionsPerUser: 10,
			wantErr:                false,
		},
		{
			name:                   "custom max sessions",
			config:                 "local://?max_sessions=50",
			wantMaxSessions:        50,
			wantMaxSessionsPerUser: 10,
			wantErr:                false,
		},
		{
			name:                   "custom max sessions per user",
			config:                 "local://localhost?max_sessions_per_user=5",
			wantMaxSessions:        100,
			wantMaxSessionsPerUser: 5,
			wantErr:                false,
		},
		{
			name:                   "both custom parameters",
			config:                 "local://localhost?max_sessions=25&max_sessions_per_user=3",
			wantMaxSessions:        25,
			wantMaxSessionsPerUser: 3,
			wantErr:                false,
		},
		{
			name:    "invalid URL",
			config:  "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewLocalSessionPool(tt.config, logger)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pool)

			localPool := pool.(*LocalSessionPool)
			assert.Equal(t, tt.wantMaxSessions, localPool.maxSessions)
			assert.Equal(t, tt.wantMaxSessionsPerUser, localPool.maxSessionsPerUser)
		})
	}
}

func TestLocalSessionPool_Get(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("successful borrow", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		key, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key)
		assert.Contains(t, key, "local-session")

		// Check stats
		localPool := pool.(*LocalSessionPool)
		borrowed, available, traceIDs, _ := localPool.GetStats()
		assert.Equal(t, 1, borrowed)
		assert.Equal(t, int64(9), available)
		assert.Equal(t, 1, traceIDs)
	})

	t.Run("max sessions exhausted", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=2", logger)
		require.NoError(t, err)

		// Borrow all available sessions
		key1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key1)

		key2, err := pool.Get(ctx, "service1", "user1", "api1", "trace2", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key2)

		// Try to borrow one more - should fail
		key3, err := pool.Get(ctx, "service1", "user1", "api1", "trace3", nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, dsession.ErrConcurrentStreamLimitExceeded)
		assert.Empty(t, key3)
	})

	t.Run("max sessions per user exhausted", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10&max_sessions_per_user=2", logger)
		require.NoError(t, err)

		// Borrow maximum sessions for user1
		key1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key1)

		key2, err := pool.Get(ctx, "service1", "user1", "api1", "trace2", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key2)

		// Try to borrow one more for same user - should fail
		key3, err := pool.Get(ctx, "service1", "user1", "api1", "trace3", nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, dsession.ErrConcurrentStreamLimitExceeded)
		assert.Empty(t, key3)

		// Different user should still be able to borrow
		key4, err := pool.Get(ctx, "service1", "user2", "api2", "trace4", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key4)
	})

	t.Run("unique session keys", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		keys := make(map[string]bool)
		for i := 0; i < 5; i++ {
			key, err := pool.Get(ctx, "service1", "user1", "api1", fmt.Sprintf("trace%d", i), nil)
			assert.NoError(t, err)
			assert.NotEmpty(t, key)
			assert.False(t, keys[key], "duplicate key generated")
			keys[key] = true
		}
		assert.Len(t, keys, 5)
	})

	t.Run("trace ID tracking", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		// Borrow sessions for same trace ID
		key1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key1)

		key2, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key2)

		localPool := pool.(*LocalSessionPool)
		localPool.mu.RLock()
		traceCount := localPool.traceIDSessions["trace1"]
		localPool.mu.RUnlock()
		assert.Equal(t, int64(2), traceCount)
	})

	t.Run("empty trace ID allowed", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		key, err := pool.Get(ctx, "service1", "user1", "api1", "", nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, key)

		localPool := pool.(*LocalSessionPool)
		localPool.mu.RLock()
		_, hasEmpty := localPool.traceIDSessions[""]
		localPool.mu.RUnlock()
		assert.False(t, hasEmpty, "empty trace ID should not be tracked")
	})
}

func TestLocalSessionPool_Release(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("successful release", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		key, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		pool.Release(key, "api1")

		// Check stats after release
		localPool := pool.(*LocalSessionPool)
		borrowed, available, _, _ := localPool.GetStats()
		assert.Equal(t, 0, borrowed)
		assert.Equal(t, int64(10), available)
	})

	t.Run("release unknown session", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		// This will now just log an error instead of returning one
		pool.Release("unknown-session-key", "api1")
	})

	t.Run("release with empty key", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		// This will now just log an error instead of returning one
		pool.Release("", "api1")
	})

	t.Run("API key validation", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		key, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		// Release with wrong API key - will now just log an error
		pool.Release(key, "wrong-api")

		// Need to get a new session since the previous one was released (even with wrong API)
		key, err = pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		// Release with correct API key
		pool.Release(key, "api1")
	})

	t.Run("release with empty API key succeeds", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		key, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		// Release with empty API key should succeed
		pool.Release(key, "")
	})

	t.Run("trace ID cleanup", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
		require.NoError(t, err)

		// Borrow two sessions for same trace
		key1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)
		key2, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		localPool := pool.(*LocalSessionPool)

		// Release first session
		pool.Release(key1, "api1")

		localPool.mu.RLock()
		count := localPool.traceIDSessions["trace1"]
		localPool.mu.RUnlock()
		assert.Equal(t, int64(1), count)

		// Release second session
		pool.Release(key2, "api1")

		localPool.mu.RLock()
		_, exists := localPool.traceIDSessions["trace1"]
		localPool.mu.RUnlock()
		assert.False(t, exists, "trace ID should be removed when count reaches 0")
	})
}

func TestLocalSessionPool_Concurrency(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("concurrent borrows and releases", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=50", logger)
		require.NoError(t, err)

		const numGoroutines = 20
		const operationsPerGoroutine = 5

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*operationsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					key, err := pool.Get(ctx, "service", fmt.Sprintf("user%d", workerID),
						fmt.Sprintf("api%d", workerID), fmt.Sprintf("trace%d", workerID), nil)
					if err != nil {
						errors <- err
						continue
					}

					// Simulate some work
					time.Sleep(time.Millisecond * 10)

					pool.Release(key, fmt.Sprintf("api%d", workerID))
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		var errorCount int
		for err := range errors {
			t.Logf("Concurrent operation error: %v", err)
			errorCount++
		}
		assert.Equal(t, 0, errorCount, "Should have no errors in concurrent operations")

		// Final stats should show all sessions released
		localPool := pool.(*LocalSessionPool)
		borrowed, _, _, _ := localPool.GetStats()
		assert.Equal(t, 0, borrowed, "All sessions should be released")
	})

	t.Run("concurrent max sessions enforcement", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=5", logger)
		require.NoError(t, err)

		const numGoroutines = 10
		keys := make(chan string, numGoroutines)
		errors := make(chan error, numGoroutines)

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key, err := pool.Get(ctx, "service", fmt.Sprintf("user%d", id),
					fmt.Sprintf("api%d", id), fmt.Sprintf("trace%d", id), nil)
				if err != nil {
					errors <- err
				} else {
					keys <- key
				}
			}(i)
		}

		wg.Wait()
		close(keys)
		close(errors)

		// Should have exactly 5 successful borrows and 5 errors
		successCount := len(keys)
		errorCount := len(errors)
		assert.Equal(t, 5, successCount, "Should have exactly max_sessions successful borrows")
		assert.Equal(t, 5, errorCount, "Should have exactly (numGoroutines - max_sessions) errors")

		// Release all borrowed sessions
		for key := range keys {
			pool.Release(key, "")
		}
	})
}

func TestLocalSessionPool_GetStats(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	pool, err := NewLocalSessionPool("local://localhost?max_sessions=10", logger)
	require.NoError(t, err)

	// Initial stats
	borrowed, available, traceIDs, users := pool.(*LocalSessionPool).GetStats()
	assert.Equal(t, 0, borrowed)
	assert.Equal(t, int64(10), available)
	assert.Equal(t, 0, traceIDs)
	assert.Equal(t, 0, users)

	// Borrow some sessions
	key1, err := pool.Get(ctx, "service", "user1", "api1", "trace1", nil)
	require.NoError(t, err)
	key2, err := pool.Get(ctx, "service", "user2", "api2", "trace1", nil)
	require.NoError(t, err)
	key3, err := pool.Get(ctx, "service", "user3", "api3", "trace2", nil)
	require.NoError(t, err)

	// Check stats
	borrowed, available, traceIDs, users = pool.(*LocalSessionPool).GetStats()
	assert.Equal(t, 3, borrowed)
	assert.Equal(t, int64(7), available)
	assert.Equal(t, 2, traceIDs) // trace1 and trace2
	assert.Equal(t, 3, users)    // user1, user2, user3

	// Release one session
	pool.Release(key1, "api1")

	borrowed, available, traceIDs, users = pool.(*LocalSessionPool).GetStats()
	assert.Equal(t, 2, borrowed)
	assert.Equal(t, int64(8), available)
	assert.Equal(t, 2, traceIDs) // still 2 trace IDs
	assert.Equal(t, 2, users)    // user2, user3

	// Release remaining sessions
	pool.Release(key2, "api2")
	pool.Release(key3, "api3")

	borrowed, available, traceIDs, users = pool.(*LocalSessionPool).GetStats()
	assert.Equal(t, 0, borrowed)
	assert.Equal(t, int64(10), available)
	assert.Equal(t, 0, traceIDs) // all trace IDs cleaned up
	assert.Equal(t, 0, users)    // all user sessions cleaned up
}

func TestLocalSessionPool_UserSessionTracking(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("user session counting", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10&max_sessions_per_user=3", logger)
		require.NoError(t, err)

		// Borrow sessions for different users
		key1, err := pool.Get(ctx, "service", "user1", "api1", "trace1", nil)
		require.NoError(t, err)
		key2, err := pool.Get(ctx, "service", "user1", "api1", "trace2", nil)
		require.NoError(t, err)
		key3, err := pool.Get(ctx, "service", "user2", "api2", "trace3", nil)
		require.NoError(t, err)

		localPool := pool.(*LocalSessionPool)
		localPool.mu.RLock()
		user1Count := localPool.userSessions["user1"]
		user2Count := localPool.userSessions["user2"]
		localPool.mu.RUnlock()

		assert.Equal(t, int64(2), user1Count)
		assert.Equal(t, int64(1), user2Count)

		// Release one session for user1
		pool.Release(key1, "api1")

		localPool.mu.RLock()
		user1Count = localPool.userSessions["user1"]
		localPool.mu.RUnlock()
		assert.Equal(t, int64(1), user1Count)

		// Release remaining sessions
		pool.Release(key2, "api1")
		pool.Release(key3, "api2")

		localPool.mu.RLock()
		_, user1Exists := localPool.userSessions["user1"]
		_, user2Exists := localPool.userSessions["user2"]
		localPool.mu.RUnlock()
		assert.False(t, user1Exists)
		assert.False(t, user2Exists)
	})

	t.Run("user session limit enforcement", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_sessions=10&max_sessions_per_user=2", logger)
		require.NoError(t, err)

		// User1 borrows maximum sessions
		key1, err := pool.Get(ctx, "service", "user1", "api1", "trace1", nil)
		require.NoError(t, err)
		key2, err := pool.Get(ctx, "service", "user1", "api1", "trace2", nil)
		require.NoError(t, err)

		// User1 tries to borrow one more - should fail
		_, err = pool.Get(ctx, "service", "user1", "api1", "trace3", nil)
		assert.ErrorIs(t, err, dsession.ErrConcurrentStreamLimitExceeded)

		// User2 should still be able to borrow
		key3, err := pool.Get(ctx, "service", "user2", "api2", "trace4", nil)
		require.NoError(t, err)
		key4, err := pool.Get(ctx, "service", "user2", "api2", "trace5", nil)
		require.NoError(t, err)

		// User2 tries to borrow one more - should also fail
		_, err = pool.Get(ctx, "service", "user2", "api2", "trace6", nil)
		assert.ErrorIs(t, err, dsession.ErrConcurrentStreamLimitExceeded)

		// Release one session for user1, should be able to borrow again
		pool.Release(key1, "api1")
		key5, err := pool.Get(ctx, "service", "user1", "api1", "trace7", nil)
		require.NoError(t, err)

		// Clean up
		pool.Release(key2, "api1")
		pool.Release(key3, "api2")
		pool.Release(key4, "api2")
		pool.Release(key5, "api1")
	})
}
