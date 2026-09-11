package EasyRoutine

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxRoutineNameBytes = 255

// LeaseAction identifies an ownership operation.
type LeaseAction string

const (
	LeaseAcquire LeaseAction = "acquire"
	LeaseRenew   LeaseAction = "renew"
	LeaseRelease LeaseAction = "release"
)

// RoutineStatus describes the latest observed state of a uniquely supervised task.
type RoutineStatus string

const (
	RoutineNotStarted RoutineStatus = "not_started"
	RoutineRunning    RoutineStatus = "running"
	RoutineDone       RoutineStatus = "done"
	RoutinePanic      RoutineStatus = "panic"
)

// LeaseState is the ownership and task state submitted with a lease action.
type LeaseState struct {
	// Name is the exact external task identifier.
	Name   string
	Owner  string
	TTL    time.Duration
	Status RoutineStatus
	// SuccessCount is the number of task attempts that returned normally.
	SuccessCount int64
	// FailureCount is the number of task attempts that panicked.
	FailureCount int64
	// Log contains panic details when Status is RoutinePanic.
	Log string
}

// SupervisorLog is a durable lifecycle event recorded by a LeaseProvider.
type SupervisorLog struct {
	ID     string
	Name   string
	Owner  string
	Action LeaseAction
	Status RoutineStatus
	Log    string
	// CreatedAt is assigned by the provider's storage clock.
	CreatedAt time.Time
}

// SupervisorStatus is the latest state stored for a uniquely supervised task.
// Owner identifies the last process to hold the lease; ExpiresAt determines
// whether that ownership is still current.
type SupervisorStatus struct {
	Name         string
	Owner        string
	Status       RoutineStatus
	SuccessCount int64
	FailureCount int64
	Log          string
	ExpiresAt    time.Time
	UpdatedAt    time.Time
}

func validateRoutineName(name string) error {
	if name == "" {
		return errors.New("task name is required")
	}
	if !utf8.ValidString(name) {
		return errors.New("task name must be valid UTF-8")
	}
	if len(name) > maxRoutineNameBytes {
		return errors.New("task name must not exceed 255 bytes")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("task name must not have surrounding whitespace")
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return errors.New("task name must not contain control characters")
		}
	}
	return nil
}

// LeaseProvider is the backend required to coordinate unique supervisors across
// processes. Redis is one possible implementation.
//
// Implementations must make the ownership effect of each Action atomic.
// LeaseRenew may succeed only while Owner matches the value stored for Name.
// LeaseRelease must end only a matching lease and is idempotent: it succeeds
// when Owner no longer holds Name, including when no matching lease exists.
// Methods must return promptly after ctx is canceled.
// Action's result describes only the ownership change; log persistence must not
// change a successful result to false. Query methods with no names return all
// records. GetLogs returns records newest first.
type LeaseProvider interface {
	Action(ctx context.Context, action LeaseAction, state LeaseState) bool
	GetStatuses(ctx context.Context, names ...string) ([]SupervisorStatus, error)
	GetLogs(ctx context.Context, names ...string) ([]SupervisorLog, error)
}

// GetStatuses returns the current supervisor states ordered by name for all
// tasks or only the supplied names.
func GetStatuses(ctx context.Context, names ...string) (statuses []SupervisorStatus, err error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
	}

	coordinatorMu.RLock()
	configured := defaultCoordinator
	coordinatorMu.RUnlock()
	if configured == nil {
		return nil, errors.New("lease provider is not initialized")
	}

	defer func() {
		if recover() != nil {
			statuses = nil
			err = errors.New("lease provider panicked while getting statuses")
		}
	}()
	return configured.backend.GetStatuses(ctx, names...)
}

// GetLogs returns retained supervisor logs newest first for all names or only
// the supplied names.
func GetLogs(ctx context.Context, names ...string) (logs []SupervisorLog, err error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
	}

	coordinatorMu.RLock()
	configured := defaultCoordinator
	coordinatorMu.RUnlock()
	if configured == nil {
		return nil, errors.New("lease provider is not initialized")
	}

	defer func() {
		if recover() != nil {
			logs = nil
			err = errors.New("lease provider panicked while getting logs")
		}
	}()
	return configured.backend.GetLogs(ctx, names...)
}
