package EasyRoutine

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"
)

const (
	leaseTTL          = 60 * time.Second
	heartbeatInterval = 15 * time.Second
	retryInterval     = 30 * time.Second
	restartInterval   = 90 * time.Second
	releaseTimeout    = 8 * time.Second
)

var (
	coordinatorMu      sync.RWMutex
	defaultCoordinator *coordinator
)

type leaseTiming struct {
	ttl       time.Duration
	heartbeat time.Duration
	retry     time.Duration
	restart   time.Duration
	release   time.Duration
}

type coordinator struct {
	backend LeaseProvider
	timing  leaseTiming
}

// UniqueSupervisor controls and observes a persistent distributed task supervisor.
type UniqueSupervisor struct {
	handle *Handle
}

// InitLease sets the backend used by StartUniqueSupervisor. It may be called only once.
func InitLease(backend LeaseProvider) error {
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
			retry:     retryInterval,
			restart:   restartInterval,
			release:   releaseTimeout,
		},
	}
	return nil
}

// StartUniqueSupervisor supervises task while this process owns its distributed lease.
// It keeps acquiring ownership and restarting task until ctx is canceled or Stop is called.
// The context must not be nil.
// InitLease must be called before StartUniqueSupervisor.
func StartUniqueSupervisor(ctx context.Context, name string, task Task, onPanic PanicHandler) (*UniqueSupervisor, error) {
	coordinatorMu.RLock()
	configured := defaultCoordinator
	coordinatorMu.RUnlock()
	if configured == nil {
		return nil, errors.New("lease provider is not initialized")
	}
	return configured.startUniqueSupervisor(ctx, name, task, onPanic)
}

func (c *coordinator) startUniqueSupervisor(ctx context.Context, name string, task Task, onPanic PanicHandler) (*UniqueSupervisor, error) {
	if name == "" {
		return nil, errors.New("task name is required")
	}
	if task == nil {
		return nil, errors.New("task is required")
	}

	owner := rand.Text()
	handle := startHandle(ctx, func(ctx context.Context) {
		c.run(ctx, name, owner, task, onPanic)
	})
	return &UniqueSupervisor{handle: handle}, nil
}

func (c *coordinator) run(ctx context.Context, name, owner string, task Task, onPanic PanicHandler) {
	for ctx.Err() == nil {
		if c.acquire(ctx, name, owner) && c.runAsOwner(ctx, name, owner, task, onPanic) {
			return
		}

		if !waitForRetry(ctx, c.timing.retry) {
			return
		}
	}
}

func (c *coordinator) runAsOwner(ctx context.Context, name, owner string, task Task, onPanic PanicHandler) bool {
	if ctx.Err() != nil {
		c.release(name, owner)
		return true
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	heartbeatDone := make(chan struct{})
	go c.maintainLease(heartbeatCtx, name, owner, heartbeatDone)
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
		c.release(name, owner)
	}()

	for {
		if ctx.Err() != nil {
			return true
		}
		select {
		case <-heartbeatDone:
			return false
		default:
		}

		taskHandle := launch(ctx, task, onPanic)
		select {
		case <-ctx.Done():
			taskHandle.Wait()
			return true
		case <-taskHandle.Done():
		case <-heartbeatDone:
			taskHandle.Stop()
			taskHandle.Wait()
			return false
		}

		if !waitToRestartAsOwner(ctx, heartbeatDone, c.timing.restart) {
			return ctx.Err() != nil
		}
	}
}

func (c *coordinator) maintainLease(ctx context.Context, name, owner string, done chan<- struct{}) {
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
			renewed := c.renew(renewCtx, name, owner)
			cancel()
			if !renewed {
				return
			}
		}
	}
}

func waitToRestartAsOwner(ctx context.Context, heartbeatDone <-chan struct{}, delay time.Duration) bool {
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

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *coordinator) acquire(ctx context.Context, name, owner string) (acquired bool) {
	defer func() {
		if recover() != nil {
			acquired = false
		}
	}()
	return c.backend.Acquire(ctx, name, owner, c.timing.ttl)
}

func (c *coordinator) renew(ctx context.Context, name, owner string) (renewed bool) {
	defer func() {
		if recover() != nil {
			renewed = false
		}
	}()
	return c.backend.Renew(ctx, name, owner, c.timing.ttl)
}

func (c *coordinator) release(name, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timing.release)
	defer cancel()
	defer func() { _ = recover() }()
	c.backend.Release(ctx, name, owner)
}

// Stop requests cooperative cancellation of the supervised Task.
func (s *UniqueSupervisor) Stop() {
	s.handle.Stop()
}

// Done is closed after supervision and Task cleanup finish.
func (s *UniqueSupervisor) Done() <-chan struct{} {
	return s.handle.Done()
}

// Wait blocks until supervision and Task cleanup finish.
func (s *UniqueSupervisor) Wait() {
	s.handle.Wait()
}
