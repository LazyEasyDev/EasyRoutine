package EasyRoutine

import (
	"context"
	"runtime/debug"
	"time"
)

// Task is work launched by Go or a UniqueSupervisor. It should observe ctx to support cancellation.
type Task func(ctx context.Context)

// Panic describes a panic recovered from a Task.
type Panic struct {
	Value any
	Stack []byte
}

// PanicRetry controls when a panicked Task is retried.
type PanicRetry uint8

const (
	PanicRetry30s PanicRetry = iota + 1
	PanicRetry90s
	PanicRetry300s
)

// PanicHandler is called after a Task panics. A nil handler, a handler panic,
// or an unsupported retry value defaults to PanicRetry90s.
type PanicHandler func(Panic) PanicRetry

// Handle controls and observes a running Task.
type Handle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Go launches a local panic-safe goroutine derived from ctx. A panicked Task is
// retried until it returns normally or ctx is canceled. The context must not be nil.
func Go(ctx context.Context, task Task, onPanic PanicHandler) *Handle {
	return startHandle(ctx, func(ctx context.Context) {
		runTask(ctx, task, onPanic)
	})
}

func startHandle(parent context.Context, run func(context.Context)) *Handle {
	ctx, cancel := context.WithCancel(parent)
	handle := &Handle{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(handle.done)
		defer cancel()
		run(ctx)
	}()

	return handle
}

func launch(parent context.Context, task Task, onPanic PanicHandler) *Handle {
	return startHandle(parent, func(ctx context.Context) {
		runTask(ctx, task, onPanic)
	})
}

// Stop requests cooperative cancellation of the Task.
func (h *Handle) Stop() {
	h.cancel()
}

// Done is closed after the Task and its panic handler return.
func (h *Handle) Done() <-chan struct{} {
	return h.done
}

// Wait blocks until the Task and its panic handler return.
func (h *Handle) Wait() {
	<-h.done
}

func runTask(ctx context.Context, task Task, onPanic PanicHandler) {
	for ctx.Err() == nil {
		retry, panicked := runTaskOnce(ctx, task, onPanic)
		if !panicked {
			return
		}
		if !waitToRestart(ctx, panicRetryDelay(retry)) {
			return
		}
	}
}

func runTaskOnce(parent context.Context, task Task, onPanic PanicHandler) (retry PanicRetry, panicked bool) {
	ctx, cancel := context.WithCancel(parent)
	defer func() {
		cancel()
		if value := recover(); value != nil {
			retry = notifyPanic(onPanic, Panic{Value: value, Stack: debug.Stack()})
			panicked = true
		}
	}()

	if task != nil {
		task(ctx)
	}
	return PanicRetry90s, false
}

func notifyPanic(handler PanicHandler, recovered Panic) (retry PanicRetry) {
	if handler == nil {
		return PanicRetry90s
	}

	retry = PanicRetry90s
	defer func() {
		_ = recover()
	}()
	return handler(recovered)
}

func panicRetryDelay(retry PanicRetry) time.Duration {
	switch retry {
	case PanicRetry30s:
		return 30 * time.Second
	case PanicRetry300s:
		return 300 * time.Second
	default:
		return 90 * time.Second
	}
}

func waitToRestart(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
