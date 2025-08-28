package dsession

import (
	"errors"

	"connectrpc.com/connect"
)

// ErrUnavailable is returned when the session pool service cannot be reached
var ErrUnavailable = errors.New("Unavailable")

// ErrPermissionDenied is returned when the userID or keyID has been disabled for that service
var ErrPermissionDenied = errors.New("Permission denied")

// ErrQuotaExceeded is returned when the quota for the service has been exceeded (ex: too many bytes read on a strict quota)
var ErrQuotaExceeded = errors.New("Quota exceeded")

// ErrConcurrentStreamLimitExceeded is returned when no session is available for the given service
var ErrConcurrentStreamLimitExceeded = errors.New("Concurrent stream limit exceeded")

func ToConnectError(err error) (error, bool) {
	// Handle session pool errors
	if errors.Is(err, ErrUnavailable) {
		return connect.NewError(connect.CodeUnavailable, err), true
	}
	if errors.Is(err, ErrPermissionDenied) {
		return connect.NewError(connect.CodePermissionDenied, err), true
	}
	if errors.Is(err, ErrQuotaExceeded) {
		return connect.NewError(connect.CodeResourceExhausted, err), true
	}
	if errors.Is(err, ErrConcurrentStreamLimitExceeded) {
		return connect.NewError(connect.CodeResourceExhausted, err), true
	}
	return err, false
}
