package EasyRoutine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoRecoversPanic(t *testing.T) {
	panicReceived := make(chan Panic, 1)
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	handle := Go(ctx, func(context.Context) {
		attempts.Add(1)
		panic("boom")
	}, func(recovered Panic) PanicRetry {
		panicReceived <- recovered
		cancel()
		return PanicRetry30s
	})

	handle.Wait()

	select {
	case recovered := <-panicReceived:
		if recovered.Value != "boom" {
			t.Fatalf("panic value = %v, want boom", recovered.Value)
		}
		if !strings.Contains(string(recovered.Stack), "TestGoRecoversPanic") {
			t.Fatal("panic stack does not contain the task")
		}
	default:
		t.Fatal("panic handler was not called")
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
}

func TestHandleStopCancelsTask(t *testing.T) {
	started := make(chan struct{})
	handle := Go(context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}, nil)

	<-started
	handle.Stop()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not stop after cancellation")
	}
}

func TestParentContextCancelsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handle := Go(ctx, func(ctx context.Context) {
		<-ctx.Done()
	}, nil)

	cancel()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not stop after parent context cancellation")
	}
}

func TestGoRecoversPanicFromHandler(t *testing.T) {
	handlerStarted := make(chan struct{})
	handle := Go(context.Background(), func(context.Context) {
		panic("task panic")
	}, func(Panic) PanicRetry {
		close(handlerStarted)
		panic("handler panic")
	})
	<-handlerStarted
	handle.Stop()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not finish after panic handler panicked")
	}
}

func TestPanicHandlerRunsAfterAttemptCancellation(t *testing.T) {
	var firstAttempt context.Context
	handlerSawCancellation := false
	ctx, cancel := context.WithCancel(context.Background())
	handle := Go(ctx, func(ctx context.Context) {
		firstAttempt = ctx
		panic("stop")
	}, func(Panic) PanicRetry {
		handlerSawCancellation = firstAttempt.Err() == context.Canceled
		cancel()
		return PanicRetry90s
	})

	handle.Wait()
	if !handlerSawCancellation {
		t.Fatal("panic handler ran before the failed attempt was canceled")
	}
}

func TestPanicRetryDelays(t *testing.T) {
	tests := []struct {
		retry PanicRetry
		delay time.Duration
	}{
		{PanicRetry30s, 30 * time.Second},
		{PanicRetry90s, 90 * time.Second},
		{PanicRetry300s, 300 * time.Second},
		{PanicRetry(0), 90 * time.Second},
		{PanicRetry(255), 90 * time.Second},
	}

	for _, test := range tests {
		delay := panicRetryDelay(test.retry)
		if delay != test.delay {
			t.Fatalf("retry %d = %s, want %s", test.retry, delay, test.delay)
		}
	}
}

func TestStopCancelsPanicRetryDelay(t *testing.T) {
	panicHandled := make(chan struct{})
	handle := Go(context.Background(), func(context.Context) {
		panic("retry")
	}, func(Panic) PanicRetry {
		close(panicHandled)
		return PanicRetry300s
	})

	<-panicHandled
	handle.Stop()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not stop during panic retry delay")
	}
}
