package EasyRoutine

import (
	"context"
	"errors"
	"runtime/debug"
	"time"
)

// Panic describes a panic recovered from a task.
type Panic struct {
	Value any
	Stack []byte
}

// PanicDecision controls whether and when SafeGo retries a panicked task.
type PanicDecision struct {
	// Retry starts the task again after recovery.
	Retry bool
	// After is the delay before retrying. A non-positive value retries immediately.
	After time.Duration
}

// NoRetry returns a decision that stops SafeGo after recovering a panic.
func NoRetry() PanicDecision {
	return PanicDecision{}
}

// PanicPolicy receives a recovered panic and its one-based consecutive failure
// count. A policy panic stops the task after recovery.
type PanicPolicy func(recovered Panic, failures int) PanicDecision

// Handle controls and observes managed background work.
type Handle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// SafeGo launches a panic-safe goroutine derived from ctx. Its policy decides
// whether and when a panicked task is retried. The context, task, and policy
// are required. Invalid arguments are returned before a goroutine is started.
// The task and policy must not call runtime.Goexit; if either does, managed
// work stops without treating Goexit as a panic.
func SafeGo(ctx context.Context, task func(ctx context.Context), policy PanicPolicy) (*Handle, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.New("task is required")
	}
	if policy == nil {
		return nil, errors.New("panic policy is required")
	}

	return startHandle(ctx, func(ctx context.Context) {
		runTask(ctx, task, policy)
	}, activeHandles), nil
}

func startHandle(parent context.Context, run func(context.Context), registry *handleRegistry) *Handle {
	ctx, cancel := context.WithCancel(parent)
	handle := &Handle{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	if registry != nil {
		registry.add(handle)
	}

	go func() {
		if registry != nil {
			defer registry.remove(handle)
		}
		defer close(handle.done)
		defer cancel()
		run(ctx)
	}()

	return handle
}

// Stop requests cooperative cancellation of the managed work.
func (h *Handle) Stop() {
	h.cancel()
}

// Done is closed after the managed work and its cleanup finish.
func (h *Handle) Done() <-chan struct{} {
	return h.done
}

// Wait blocks until the managed work and its cleanup finish.
func (h *Handle) Wait() {
	<-h.done
}

func runTask(ctx context.Context, task func(ctx context.Context), policy PanicPolicy) {
	failures := 0
	for ctx.Err() == nil {
		recovered, panicked := runTaskAttempt(ctx, task)
		if !panicked {
			return
		}

		failures++
		decision := applyPanicPolicy(policy, recovered, failures)
		if !decision.Retry {
			return
		}
		if decision.After > 0 && !waitForDelay(ctx, decision.After) {
			return
		}
	}
}

func runTaskAttempt(parent context.Context, task func(ctx context.Context)) (recovered Panic, panicked bool) {
	ctx, cancel := context.WithCancel(parent)
	defer func() {
		cancel()
		if value := recover(); value != nil {
			recovered = Panic{Value: value, Stack: debug.Stack()}
			panicked = true
		}
	}()

	if task != nil {
		task(ctx)
	}
	return Panic{}, false
}

func applyPanicPolicy(policy PanicPolicy, recovered Panic, failures int) (decision PanicDecision) {
	if policy == nil {
		return PanicDecision{}
	}

	defer func() {
		if recover() != nil {
			decision = PanicDecision{}
		}
	}()
	return policy(recovered, failures)
}

func waitForDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
