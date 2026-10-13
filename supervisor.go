package EasyRoutine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrSupervisorRegistered identifies a name reserved by local work or cleanup.
var ErrSupervisorRegistered = errors.New("name is already registered in this process")

// SupervisorLogCounts describes retained lifecycle history, not lifetime totals.
type SupervisorLogCounts struct {
	NotStarted int64
	Running    int64
	Done       int64
	Panic      int64
	Other      int64
}

// Supervisor is a lightweight stored state with database-clock lease activity.
// Owner is an acquisition token. Timestamp fields are Unix seconds.
type Supervisor struct {
	RoutineID     string
	Name          string
	Owner         string
	Status        RoutineStatus
	SuccessCount  int64
	FailureCount  int64
	ExpiresAt     int64
	UpdatedAt     int64
	LeaseActive   bool
	RetainedLogs  int64
	LatestPanicAt int64
	LogCounts     SupervisorLogCounts
}

// ListSupervisors returns stored states and retained log counts without log text.
// Optional exact names limit the result. Results are ordered by name and routine ID.
func ListSupervisors(ctx context.Context, names ...string) (items []Supervisor, err error) {
	configured, err := supervisorCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
	}
	defer func() {
		if recover() != nil {
			items = nil
			err = errors.New("lease provider panicked while listing supervisors")
		}
	}()
	return configured.backend.ListSupervisors(ctx, names...)
}

// GetSupervisorHistory returns one routine's retained logs, newest first.
// The routine ID is the lowercase SHA-256 identifier returned by ListSupervisors.
func GetSupervisorHistory(ctx context.Context, routineID string) (items []SupervisorLog, err error) {
	configured, err := supervisorCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	if len(routineID) != 64 || strings.ToLower(routineID) != routineID {
		return nil, errors.New("routine ID must be 64 lowercase hexadecimal characters")
	}
	if _, err := hex.DecodeString(routineID); err != nil {
		return nil, errors.New("routine ID must be 64 lowercase hexadecimal characters")
	}
	defer func() {
		if recover() != nil {
			items = nil
			err = errors.New("lease provider panicked while getting supervisor history")
		}
	}()
	return configured.backend.GetSupervisorHistory(ctx, routineID)
}

// DeleteSupervisorRecords removes an exact name's stored state and all history.
// Names registered in this process are rejected. An absent record is a success.
// This does not stop work or check registrations in other processes.
func DeleteSupervisorRecords(ctx context.Context, name string) (err error) {
	configured, err := supervisorCoordinator(ctx)
	if err != nil {
		return err
	}
	if err := validateRoutineName(name); err != nil {
		return err
	}
	if _, exists := configured.supervisorNames.LoadOrStore(name, struct{}{}); exists {
		return fmt.Errorf("supervisor %q: %w", name, ErrSupervisorRegistered)
	}
	defer func() {
		configured.supervisorNames.Delete(name)
		if recover() != nil {
			err = errors.New("lease provider panicked while deleting supervisor records")
		}
	}()
	return configured.backend.DeleteSupervisorRecords(ctx, name)
}

func supervisorCoordinator(ctx context.Context) (*coordinator, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lifecycleMu.RLock()
	configured := defaultCoordinator
	lifecycleMu.RUnlock()
	if configured == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	return configured, nil
}
