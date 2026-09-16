package EasyRoutine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func noRetryPolicy(Panic, int) PanicDecision {
	return NoRetry()
}

func mustSafeGo(t *testing.T, ctx context.Context, task func(context.Context), policy PanicPolicy) *Handle {
	t.Helper()
	handle, err := SafeGo(ctx, task, policy)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func TestWaitIncludesSafeGoStartedWhileWaiting(t *testing.T) {
	Wait()
	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		firstRelease := make(chan struct{})
		first := mustSafeGo(t, context.Background(), func(ctx context.Context) {
			close(firstStarted)
			select {
			case <-firstRelease:
			case <-ctx.Done():
			}
		}, noRetryPolicy)
		t.Cleanup(func() {
			first.Stop()
			first.Wait()
		})
		<-firstStarted

		waitDone := make(chan struct{})
		go func() {
			Wait()
			close(waitDone)
		}()
		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("Wait returned while the first SafeGo was running")
		default:
		}

		secondStarted := make(chan struct{})
		secondRelease := make(chan struct{})
		second := mustSafeGo(t, context.Background(), func(ctx context.Context) {
			close(secondStarted)
			select {
			case <-secondRelease:
			case <-ctx.Done():
			}
		}, noRetryPolicy)
		t.Cleanup(func() {
			second.Stop()
			second.Wait()
		})
		<-secondStarted

		close(firstRelease)
		first.Wait()
		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("Wait returned while a later SafeGo was still running")
		default:
		}

		close(secondRelease)
		select {
		case <-waitDone:
		case <-time.After(time.Second):
			t.Fatal("Wait did not return after all SafeGo handles completed")
		}

		activeHandles.mu.Lock()
		remaining := len(activeHandles.handles)
		activeHandles.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("active handle count = %d, want 0", remaining)
		}
	})
}

func TestSafeGoRecoversPanic(t *testing.T) {
	panicReceived := make(chan Panic, 1)
	var attempts atomic.Int32
	handle := mustSafeGo(t, context.Background(), func(context.Context) {
		attempts.Add(1)
		panic("boom")
	}, func(recovered Panic, failures int) PanicDecision {
		panicReceived <- recovered
		if failures != 1 {
			t.Errorf("failures = %d, want 1", failures)
		}
		return NoRetry()
	})

	handle.Wait()

	select {
	case recovered := <-panicReceived:
		if recovered.Value != "boom" {
			t.Fatalf("panic value = %v, want boom", recovered.Value)
		}
		if !strings.Contains(string(recovered.Stack), "TestSafeGoRecoversPanic") {
			t.Fatal("panic stack does not contain the task")
		}
	default:
		t.Fatal("panic policy was not called")
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
}

func TestNoRetryReturnsIndependentDecision(t *testing.T) {
	decision := NoRetry()
	decision.Retry = true
	if NoRetry().Retry {
		t.Fatal("mutating one no-retry decision changed another")
	}
}

func TestHandleStopCancelsTask(t *testing.T) {
	started := make(chan struct{})
	handle := mustSafeGo(t, context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}, noRetryPolicy)

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
	handle := mustSafeGo(t, ctx, func(ctx context.Context) {
		<-ctx.Done()
	}, noRetryPolicy)

	cancel()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not stop after parent context cancellation")
	}
}

func TestSafeGoRecoversPanicFromPolicy(t *testing.T) {
	policyStarted := make(chan struct{})
	handle := mustSafeGo(t, context.Background(), func(context.Context) {
		panic("task panic")
	}, func(Panic, int) PanicDecision {
		close(policyStarted)
		panic("policy panic")
	})
	<-policyStarted

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not finish after panic policy panicked")
	}
}

func TestSafeGoRejectsInvalidArguments(t *testing.T) {
	var nilContext context.Context
	if _, err := SafeGo(nilContext, func(context.Context) {}, noRetryPolicy); err == nil {
		t.Fatal("expected nil context error")
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SafeGo(canceledCtx, func(context.Context) {}, noRetryPolicy); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
	if _, err := SafeGo(context.Background(), nil, noRetryPolicy); err == nil {
		t.Fatal("expected missing task error")
	}
	if _, err := SafeGo(context.Background(), func(context.Context) {}, nil); err == nil {
		t.Fatal("expected missing panic policy error")
	}
}

func TestPanicPolicyRunsAfterAttemptCancellation(t *testing.T) {
	var firstAttempt context.Context
	policySawCancellation := false
	handle := mustSafeGo(t, context.Background(), func(ctx context.Context) {
		firstAttempt = ctx
		panic("stop")
	}, func(Panic, int) PanicDecision {
		policySawCancellation = firstAttempt.Err() == context.Canceled
		return NoRetry()
	})

	handle.Wait()
	if !policySawCancellation {
		t.Fatal("panic policy ran before the failed attempt was canceled")
	}
}

func TestSafeGoPolicyControlsRetries(t *testing.T) {
	var attempts atomic.Int32
	var failures []int
	handle := mustSafeGo(t, context.Background(), func(context.Context) {
		if attempts.Add(1) < 3 {
			panic("retry")
		}
	}, func(_ Panic, failureCount int) PanicDecision {
		failures = append(failures, failureCount)
		return PanicDecision{Retry: true, After: time.Millisecond}
	})

	handle.Wait()
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
	if len(failures) != 2 || failures[0] != 1 || failures[1] != 2 {
		t.Fatalf("failure counts = %v, want [1 2]", failures)
	}
}

func TestStopCancelsSafeGoRetryDelay(t *testing.T) {
	panicHandled := make(chan struct{})
	handle := mustSafeGo(t, context.Background(), func(context.Context) {
		panic("retry")
	}, func(Panic, int) PanicDecision {
		close(panicHandled)
		return PanicDecision{Retry: true, After: time.Hour}
	})

	<-panicHandled
	handle.Stop()

	select {
	case <-handle.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not stop during panic retry delay")
	}
}
