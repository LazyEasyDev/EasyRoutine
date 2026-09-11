package EasyRoutine

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	leaseTTL            = 180 * time.Second
	heartbeatInterval   = 30 * time.Second
	releaseTimeout      = 30 * time.Second
	panicReacquireDelay = 300 * time.Second
)

var (
	coordinatorMu      sync.RWMutex
	defaultCoordinator *coordinator
	serverTag          = detectServerTag()
)

type leaseTiming struct {
	ttl       time.Duration
	heartbeat time.Duration
	release   time.Duration
}

type coordinator struct {
	backend leaseProvider
	timing  leaseTiming
}

type taskAttemptResult struct {
	recovered Panic
	panicked  bool
}

type supervisorState struct {
	mu           sync.RWMutex
	status       RoutineStatus
	successCount int64
	failureCount int64
	log          string
}

func newSupervisorState() *supervisorState {
	return &supervisorState{status: RoutineNotStarted}
}

func (s *supervisorState) snapshot(name, owner string, ttl time.Duration) leaseState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return leaseState{
		Name:         name,
		Owner:        owner,
		TTL:          ttl,
		Status:       s.status,
		SuccessCount: s.successCount,
		FailureCount: s.failureCount,
		Log:          serverLog(s.log),
	}
}

func detectServerTag() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "unknown"
	}
	return hostname
}

func serverLog(message string) string {
	prefix := "[" + serverTag + "]"
	if message == "" {
		return prefix
	}
	return prefix + "\n" + message
}

func (s *supervisorState) notStarted() {
	s.mu.Lock()
	s.status = RoutineNotStarted
	s.log = ""
	s.mu.Unlock()
}

func (s *supervisorState) running() {
	s.mu.Lock()
	s.status = RoutineRunning
	s.log = ""
	s.mu.Unlock()
}

func (s *supervisorState) done(succeeded bool) {
	s.mu.Lock()
	s.status = RoutineDone
	if succeeded {
		s.successCount++
	}
	s.log = ""
	s.mu.Unlock()
}

func (s *supervisorState) panicked(recovered Panic) {
	s.mu.Lock()
	s.status = RoutinePanic
	s.failureCount++
	s.log = fmt.Sprintf("%v\n%s", recovered.Value, recovered.Stack)
	s.mu.Unlock()
}

// uniqueTask configures work that repeats while its supervisor owns the lease.
type uniqueTask struct {
	// Run performs one task attempt. It should observe ctx to support cancellation.
	Run func(ctx context.Context)
	// RepeatAfter is the delay after a normal return. Zero repeats immediately;
	// negative values are invalid.
	RepeatAfter time.Duration
}

// SupervisorPanicHandler observes a panic from a uniquely supervised task.
// Recovery policy is fixed; the handler does not control when work resumes.
type SupervisorPanicHandler func(Panic)

func initLease(backend leaseProvider) error {
	if backend == nil {
		return errors.New("lease provider is required")
	}

	coordinatorMu.Lock()
	defer coordinatorMu.Unlock()
	if defaultCoordinator != nil {
		return errors.New("lease provider is already initialized")
	}
	defaultCoordinator = &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       leaseTTL,
			heartbeat: heartbeatInterval,
			release:   releaseTimeout,
		},
	}
	return nil
}

// StartUniqueSupervisor supervises task while this process owns its distributed lease.
// Normal returns retain the lease for restart; panics release it before reacquisition.
// Supervision continues until ctx is canceled or Stop is called.
// The context, task, and panic handler are required. A negative repeat delay
// is invalid. Invalid arguments are returned before a goroutine is started.
// The task and panic handler must not call runtime.Goexit; if either does,
// supervision stops without treating Goexit as a panic.
// InitSQLLease must be called before StartUniqueSupervisor.
func StartUniqueSupervisor(ctx context.Context, name string, run func(context.Context), onPanic SupervisorPanicHandler, repeatAfter time.Duration) (*Handle, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRoutineName(name); err != nil {
		return nil, err
	}
	if run == nil {
		return nil, errors.New("task is required")
	}
	if onPanic == nil {
		return nil, errors.New("panic handler is required")
	}
	if repeatAfter < 0 {
		return nil, errors.New("task repeat delay must not be negative")
	}

	coordinatorMu.RLock()
	configured := defaultCoordinator
	coordinatorMu.RUnlock()
	if configured == nil {
		return nil, errors.New("lease provider is not initialized")
	}
	return configured.startUniqueSupervisor(ctx, name, uniqueTask{Run: run, RepeatAfter: repeatAfter}, onPanic), nil
}

func (c *coordinator) startUniqueSupervisor(ctx context.Context, name string, task uniqueTask, onPanic SupervisorPanicHandler) *Handle {
	return startHandle(ctx, func(ctx context.Context) {
		c.run(ctx, name, task, onPanic)
	})
}

func (c *coordinator) run(ctx context.Context, name string, task uniqueTask, onPanic SupervisorPanicHandler) {
	state := newSupervisorState()
	for ctx.Err() == nil {
		owner := rand.Text()
		state.notStarted()
		if c.action(ctx, LeaseAcquire, state.snapshot(name, owner, c.timing.ttl)) {
			stop, recovered := c.runAsOwner(ctx, name, owner, task, state)
			if stop {
				return
			}
			if recovered != nil {
				if !c.waitAfterPanic(ctx, name, owner, state, onPanic, *recovered) {
					return
				}
				continue
			}
		}

		if !waitForDelay(ctx, c.timing.heartbeat) {
			return
		}
	}
}

func (c *coordinator) runAsOwner(ctx context.Context, name, owner string, task uniqueTask, state *supervisorState) (stop bool, recovered *Panic) {
	if ctx.Err() != nil {
		state.done(false)
		c.release(name, owner, state)
		return true, nil
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	heartbeatDone := make(chan struct{})
	go c.maintainLease(heartbeatCtx, name, owner, state, heartbeatDone)
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
		if recovered == nil {
			c.release(name, owner, state)
		}
	}()

	for {
		if ctx.Err() != nil {
			state.done(false)
			return true, nil
		}
		select {
		case <-heartbeatDone:
			return false, nil
		default:
		}

		state.running()
		attemptResult := make(chan taskAttemptResult, 1)
		taskHandle := startHandle(ctx, func(ctx context.Context) {
			defer close(attemptResult)
			recovered, panicked := runTaskAttempt(ctx, task.Run)
			attemptResult <- taskAttemptResult{recovered: recovered, panicked: panicked}
		})
		select {
		case <-ctx.Done():
			taskHandle.Wait()
			state.done(false)
			return true, nil
		case <-taskHandle.Done():
			result, completed := <-attemptResult
			if !completed {
				state.done(false)
				return true, nil
			}
			if ctx.Err() != nil {
				state.done(false)
				return true, nil
			}
			if result.panicked {
				state.panicked(result.recovered)
				return false, &result.recovered
			}
			state.done(true)
			select {
			case <-heartbeatDone:
				return false, nil
			default:
			}
		case <-heartbeatDone:
			taskHandle.Stop()
			taskHandle.Wait()
			state.done(false)
			return false, nil
		}

		if !waitForNextCycleAsOwner(ctx, heartbeatDone, task.RepeatAfter) {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, nil
		}
	}
}

func notifySupervisorPanic(handler SupervisorPanicHandler, recovered Panic) {
	if handler == nil {
		return
	}
	defer func() { _ = recover() }()
	handler(recovered)
}

func (c *coordinator) waitAfterPanic(ctx context.Context, name, owner string, state *supervisorState, handler SupervisorPanicHandler, recovered Panic) bool {
	reacquireAt := time.Now().Add(panicReacquireDelay)
	released := c.releaseWithin(name, owner, state, min(c.timing.release, panicReacquireDelay))
	notifySupervisorPanic(handler, recovered)

	for {
		remaining := time.Until(reacquireAt)
		if remaining <= 0 {
			return ctx.Err() == nil
		}

		if released {
			return waitForDelay(ctx, time.Until(reacquireAt))
		}
		if !waitForDelay(ctx, min(c.timing.heartbeat, remaining)) {
			return false
		}

		remaining = time.Until(reacquireAt)
		if remaining > 0 {
			released = c.releaseWithin(name, owner, state, min(c.timing.release, remaining))
		}
	}
}

func (c *coordinator) maintainLease(ctx context.Context, name, owner string, state *supervisorState, done chan<- struct{}) {
	defer close(done)
	defer func() { _ = recover() }()
	ticker := time.NewTicker(c.timing.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, c.timing.heartbeat)
			renewed := c.action(renewCtx, LeaseRenew, state.snapshot(name, owner, c.timing.ttl))
			cancel()
			if !renewed {
				return
			}
		}
	}
}

func waitForNextCycleAsOwner(ctx context.Context, heartbeatDone <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-heartbeatDone:
		return false
	case <-timer.C:
		select {
		case <-heartbeatDone:
			return false
		default:
			return true
		}
	}
}

func (c *coordinator) action(ctx context.Context, action LeaseAction, state leaseState) (applied bool) {
	defer func() {
		if recover() != nil {
			applied = false
		}
	}()
	return c.backend.Action(ctx, action, state)
}

func (c *coordinator) release(name, owner string, state *supervisorState) bool {
	return c.releaseWithin(name, owner, state, c.timing.release)
}

func (c *coordinator) releaseWithin(name, owner string, state *supervisorState, timeout time.Duration) (released bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.action(ctx, LeaseRelease, state.snapshot(name, owner, c.timing.ttl))
}
