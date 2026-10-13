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

// leaseState is the ownership and task state submitted with a lease action.
type leaseState struct {
	// Name is the exact external task identifier.
	Name   string
	Owner  string
	TTL    time.Duration
	Status RoutineStatus
	// SuccessCount is the number of task attempts that returned normally.
	SuccessCount int64
	// FailureCount is the number of task attempts that panicked.
	FailureCount int64
	// Log starts with the server tag and contains panic details when applicable.
	Log string
}

// SupervisorLog is a durable lifecycle event recorded by the SQL backend.
type SupervisorLog struct {
	ID        string
	RoutineID string
	Name      string
	Owner     string
	Action    LeaseAction
	Status    RoutineStatus
	// Log starts with the server tag and contains panic details when applicable.
	Log string
	// CreatedAt is Unix time in seconds assigned by the provider's storage clock.
	CreatedAt int64
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

// leaseProvider is the internal backend used to coordinate unique supervisors.
//
// Implementations must make the ownership effect of each Action atomic.
// LeaseRenew may succeed only while Owner matches the value stored for Name.
// Implementations should perform bounded, context-aware retries for transient
// LeaseRenew backend errors before returning false. A false result means
// ownership could not be confirmed and local work must stop.
// LeaseRelease must end only a matching lease and is idempotent: it succeeds
// when Owner no longer holds Name, including when no matching lease exists.
// Methods must return promptly after ctx is canceled.
// Action's result describes only the ownership change; log persistence must not
// change a successful result to false. Listing excludes log text; history is
// ordered newest first. Deletion atomically removes the state and all history.
type leaseProvider interface {
	Action(ctx context.Context, action LeaseAction, state leaseState) bool
	ListSupervisors(ctx context.Context, names ...string) ([]Supervisor, error)
	GetSupervisorHistory(ctx context.Context, routineID string) ([]SupervisorLog, error)
	DeleteSupervisorRecords(ctx context.Context, name string) error
}
