package local

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/streamingfast/dsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLocalSessionPool_GetWorker(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("successful worker borrow", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=5&max_workers_per_user=8", logger)
		require.NoError(t, err)

		// First create a session
		sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		// Get a worker for the session
		workerKey, err := pool.GetWorker(ctx, "service1", sessionKey, 3)
		assert.NoError(t, err)
		assert.NotEmpty(t, workerKey)
		assert.Contains(t, workerKey, sessionKey)
		assert.Contains(t, workerKey, "-w")

		// Check that worker was tracked
		localPool := pool.(*LocalSessionPool)
		localPool.mu.RLock()
		mappedSessionKey, exists := localPool.workerToSession[workerKey]
		userWorkers := localPool.userWorkers["user1"]
		localPool.mu.RUnlock()

		assert.True(t, exists)
		assert.Equal(t, sessionKey, mappedSessionKey)
		assert.Equal(t, int64(1), userWorkers)
	})

	t.Run("session not found", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10", logger)
		require.NoError(t, err)

		_, err = pool.GetWorker(ctx, "service1", "non-existent-session", 3)
		assert.ErrorIs(t, err, dsession.ErrSessionNotFound)
	})

	t.Run("limit enforcement", func(t *testing.T) {
		testCases := []struct {
			name        string
			config      string
			setupFunc   func(*testing.T, dsession.SessionPool) ([]string, []string) // returns sessionKeys, users
			expectError bool
		}{
			{
				name:   "max global workers exceeded",
				config: "local://localhost?max_workers=2&max_workers_per_session=5&max_workers_per_user=5",
				setupFunc: func(t *testing.T, pool dsession.SessionPool) ([]string, []string) {
					sessionKey1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
					require.NoError(t, err)
					sessionKey2, err := pool.Get(ctx, "service1", "user2", "api2", "trace2", nil)
					require.NoError(t, err)

					// Get maximum workers
					workerKey1, err := pool.GetWorker(ctx, "service1", sessionKey1, 3)
					require.NoError(t, err)
					workerKey2, err := pool.GetWorker(ctx, "service1", sessionKey2, 3)
					require.NoError(t, err)

					return []string{sessionKey1, sessionKey2}, []string{workerKey1, workerKey2}
				},
				expectError: true,
			},
			{
				name:   "max workers per user exceeded",
				config: "local://localhost?max_workers=10&max_workers_per_user=2&max_workers_per_session=5",
				setupFunc: func(t *testing.T, pool dsession.SessionPool) ([]string, []string) {
					sessionKey1, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
					require.NoError(t, err)
					sessionKey2, err := pool.Get(ctx, "service1", "user1", "api1", "trace2", nil)
					require.NoError(t, err)

					// Get maximum workers for the user
					workerKey1, err := pool.GetWorker(ctx, "service1", sessionKey1, 3)
					require.NoError(t, err)
					workerKey2, err := pool.GetWorker(ctx, "service1", sessionKey2, 3)
					require.NoError(t, err)

					return []string{sessionKey1, sessionKey2}, []string{workerKey1, workerKey2}
				},
				expectError: true,
			},
			{
				name:   "max workers per session exceeded - pool limit",
				config: "local://localhost?max_workers=10&max_workers_per_session=2&max_workers_per_user=5",
				setupFunc: func(t *testing.T, pool dsession.SessionPool) ([]string, []string) {
					sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
					require.NoError(t, err)

					// Get maximum workers for the session (pool limit is 2)
					workerKey1, err := pool.GetWorker(ctx, "service1", sessionKey, 5) // request 5, but pool limit is 2
					require.NoError(t, err)
					workerKey2, err := pool.GetWorker(ctx, "service1", sessionKey, 5)
					require.NoError(t, err)

					return []string{sessionKey}, []string{workerKey1, workerKey2}
				},
				expectError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pool, err := NewLocalSessionPool(tc.config, logger)
				require.NoError(t, err)

				sessionKeys, workerKeys := tc.setupFunc(t, pool)

				// Try to get one more worker - should fail if expectError is true
				_, err = pool.GetWorker(ctx, "service1", sessionKeys[0], 3)
				if tc.expectError {
					assert.ErrorIs(t, err, dsession.ErrWorkersLimitExceeded)
				} else {
					assert.NoError(t, err)
				}

				// Clean up
				for _, workerKey := range workerKeys {
					pool.ReleaseWorker(workerKey)
				}
			})
		}
	})

	t.Run("worker properties", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=5", logger)
		require.NoError(t, err)

		sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		// Test unique keys and numbering
		keys := make(map[string]bool)
		expectedSuffixes := []string{"-w0001", "-w0002", "-w0003"}

		for i := 0; i < 3; i++ {
			workerKey, err := pool.GetWorker(ctx, "service1", sessionKey, 5)
			require.NoError(t, err)
			assert.NotEmpty(t, workerKey)
			assert.False(t, keys[workerKey], "duplicate worker key generated")
			assert.Contains(t, workerKey, expectedSuffixes[i])
			keys[workerKey] = true
		}
		assert.Len(t, keys, 3)

		// Clean up
		for workerKey := range keys {
			pool.ReleaseWorker(workerKey)
		}
	})
}

func TestLocalSessionPool_ReleaseWorker(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("successful worker release", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=5&max_workers_per_user=5", logger)
		require.NoError(t, err)

		// Create session and get worker
		sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)
		workerKey, err := pool.GetWorker(ctx, "service1", sessionKey, 3)
		require.NoError(t, err)

		localPool := pool.(*LocalSessionPool)

		// Verify worker is tracked before release
		localPool.mu.RLock()
		_, exists := localPool.workerToSession[workerKey]
		userWorkers := localPool.userWorkers["user1"]
		localPool.mu.RUnlock()
		assert.True(t, exists)
		assert.Equal(t, int64(1), userWorkers)

		// Release the worker
		pool.ReleaseWorker(workerKey)

		// Verify worker is no longer tracked
		localPool.mu.RLock()
		_, exists = localPool.workerToSession[workerKey]
		userWorkers = localPool.userWorkers["user1"]
		localPool.mu.RUnlock()
		assert.False(t, exists)
		assert.Equal(t, int64(0), userWorkers)
	})

	t.Run("edge cases", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=5", logger)
		require.NoError(t, err)

		// Test various edge cases - these should not panic
		testCases := []struct {
			name      string
			workerKey string
			setupFunc func() string
		}{
			{
				name:      "unknown worker",
				workerKey: "unknown-worker-key",
			},
			{
				name:      "empty key",
				workerKey: "",
			},
			{
				name: "worker for released session",
				setupFunc: func() string {
					// Create session and get worker
					sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
					require.NoError(t, err)
					workerKey, err := pool.GetWorker(ctx, "service1", sessionKey, 3)
					require.NoError(t, err)

					// Release the session first
					pool.Release(sessionKey)

					return workerKey
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				workerKey := tc.workerKey
				if tc.setupFunc != nil {
					workerKey = tc.setupFunc()
				}

				// These should not panic, just handle gracefully
				assert.NotPanics(t, func() {
					pool.ReleaseWorker(workerKey)
				})
			})
		}
	})

	t.Run("counter cleanup", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=5&max_workers_per_user=5", logger)
		require.NoError(t, err)

		// Create session and get multiple workers
		sessionKey, err := pool.Get(ctx, "service1", "user1", "api1", "trace1", nil)
		require.NoError(t, err)

		workerKeys := make([]string, 3)
		for i := 0; i < 3; i++ {
			workerKeys[i], err = pool.GetWorker(ctx, "service1", sessionKey, 5)
			require.NoError(t, err)
		}

		localPool := pool.(*LocalSessionPool)

		// Release workers one by one and verify counters
		for i, workerKey := range workerKeys {
			pool.ReleaseWorker(workerKey)

			expectedCount := int64(len(workerKeys) - i - 1)
			globalWorkers := localPool.workerCounter.Load()

			localPool.mu.RLock()
			sessionInfo := localPool.borrowedSessions[sessionKey]
			sessionWorkers := sessionInfo.workers.Load()
			userWorkers, userExists := localPool.userWorkers["user1"]
			localPool.mu.RUnlock()

			assert.Equal(t, expectedCount, globalWorkers)
			assert.Equal(t, expectedCount, sessionWorkers)

			if expectedCount == 0 {
				assert.False(t, userExists, "user worker count should be cleaned up when it reaches 0")
			} else {
				assert.Equal(t, expectedCount, userWorkers)
			}
		}
	})
}

func TestLocalSessionPool_WorkerConcurrency(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("concurrent operations and limit enforcement", func(t *testing.T) {
		// Test concurrent operations with clear global limit only
		pool, err := NewLocalSessionPool("local://localhost?max_workers=10&max_workers_per_session=100&max_workers_per_user=100", logger)
		require.NoError(t, err)

		// Create sessions for multiple users
		sessions := make([]string, 3)
		users := []string{"user1", "user2", "user3"}
		for i, user := range users {
			sessions[i], err = pool.Get(ctx, "service", user, fmt.Sprintf("api%d", i), fmt.Sprintf("trace%d", i), nil)
			require.NoError(t, err)
		}

		// Concurrent worker acquisition
		const numGoroutines = 15 // More than max_workers to test limits
		var wg sync.WaitGroup
		successKeys := make(chan string, numGoroutines)
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				sessionKey := sessions[index%len(sessions)]
				workerKey, err := pool.GetWorker(ctx, "service", sessionKey, 100)
				if err != nil {
					errors <- err
				} else {
					successKeys <- workerKey
				}
			}(i)
		}

		wg.Wait()
		close(successKeys)
		close(errors)

		// Collect results
		var workers []string
		for key := range successKeys {
			workers = append(workers, key)
		}
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}

		// Should have exactly max_workers successful and rest as errors
		assert.Equal(t, 10, len(workers), "Should have exactly max_workers successful acquisitions")
		assert.Equal(t, 5, len(errorList), "Should have (numGoroutines - max_workers) errors")

		// All errors should be limit exceeded
		for _, err := range errorList {
			assert.ErrorIs(t, err, dsession.ErrWorkersLimitExceeded)
		}

		// Concurrent worker release
		var releaseWg sync.WaitGroup
		for _, workerKey := range workers {
			releaseWg.Add(1)
			go func(key string) {
				defer releaseWg.Done()
				pool.ReleaseWorker(key)
			}(workerKey)
		}
		releaseWg.Wait()

		// Verify cleanup
		localPool := pool.(*LocalSessionPool)
		globalWorkers := localPool.workerCounter.Load()
		localPool.mu.RLock()
		workerMappings := len(localPool.workerToSession)
		userWorkers := len(localPool.userWorkers)
		localPool.mu.RUnlock()

		assert.Equal(t, int64(0), globalWorkers)
		assert.Equal(t, 0, workerMappings)
		assert.Equal(t, 0, userWorkers)

		// Clean up sessions
		for _, sessionKey := range sessions {
			pool.Release(sessionKey)
		}
	})
}

func TestLocalSessionPool_WorkerIntegration(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	t.Run("complete lifecycle", func(t *testing.T) {
		pool, err := NewLocalSessionPool("local://localhost?max_workers=20&max_workers_per_session=3&max_workers_per_user=5", logger)
		require.NoError(t, err)

		// Create sessions for different users
		sessionKeys := make(map[string][]string)
		users := []string{"user1", "user2", "user3"}

		for _, user := range users {
			sessionKeys[user] = make([]string, 2)
			for i := 0; i < 2; i++ {
				sessionKeys[user][i], err = pool.Get(ctx, "service", user, fmt.Sprintf("api-%s-%d", user, i), fmt.Sprintf("trace-%s-%d", user, i), nil)
				require.NoError(t, err)
			}
		}

		// Get workers for each session (2 workers per session)
		workerKeys := make([]string, 0)
		for _, user := range users {
			for _, sessionKey := range sessionKeys[user] {
				for i := 0; i < 2; i++ {
					workerKey, err := pool.GetWorker(ctx, "service", sessionKey, 3)
					require.NoError(t, err)
					workerKeys = append(workerKeys, workerKey)
				}
			}
		}

		// Verify totals
		assert.Len(t, workerKeys, 12) // 3 users × 2 sessions × 2 workers

		localPool := pool.(*LocalSessionPool)
		globalWorkers := localPool.workerCounter.Load()
		assert.Equal(t, int64(12), globalWorkers)

		// Verify per-user worker counts
		localPool.mu.RLock()
		for _, user := range users {
			userWorkerCount := localPool.userWorkers[user]
			assert.Equal(t, int64(4), userWorkerCount) // 2 sessions × 2 workers each
		}
		localPool.mu.RUnlock()

		// Release all workers
		for _, workerKey := range workerKeys {
			pool.ReleaseWorker(workerKey)
		}

		// Release all sessions
		for _, user := range users {
			for _, sessionKey := range sessionKeys[user] {
				pool.Release(sessionKey)
			}
		}

		// Verify complete cleanup
		globalWorkers = localPool.workerCounter.Load()
		localPool.mu.RLock()
		workerMappings := len(localPool.workerToSession)
		userWorkers := len(localPool.userWorkers)
		sessions := len(localPool.borrowedSessions)
		userSessions := len(localPool.userSessions)
		localPool.mu.RUnlock()

		assert.Equal(t, int64(0), globalWorkers)
		assert.Equal(t, 0, workerMappings)
		assert.Equal(t, 0, userWorkers)
		assert.Equal(t, 0, sessions)
		assert.Equal(t, 0, userSessions)
	})
}
