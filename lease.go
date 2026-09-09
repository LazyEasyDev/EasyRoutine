package EasyRoutine

import (
	"context"
	"time"
)

// LeaseProvider is the backend required to coordinate unique supervisors across
// processes. Redis is one possible implementation.
//
// Implementations must make each operation atomic. Renew and Release must only
// succeed when owner still matches the value stored for name. Methods must
// return promptly after ctx is canceled.
type LeaseProvider interface {
	Acquire(ctx context.Context, name, owner string, ttl time.Duration) bool
	Renew(ctx context.Context, name, owner string, ttl time.Duration) bool
	Release(ctx context.Context, name, owner string) bool
}
