package EasyRoutine

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func waitForRegistrySignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(failure)
	}
}

func TestHandleRegistryWaitReturnsWhenEmpty(t *testing.T) {
	registry := newHandleRegistry()
	done := make(chan struct{})
	go func() {
		registry.wait()
		close(done)
	}()

	waitForRegistrySignal(t, done, "empty registry did not return from wait")
}

func TestHandleRegistryWaitsForEveryHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newHandleRegistry()
		first := &Handle{}
		second := &Handle{}
		registry.add(first)
		registry.add(second)
		defer registry.remove(first)
		defer registry.remove(second)

		done := make(chan struct{})
		go func() {
			registry.wait()
			close(done)
		}()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("wait returned with both handles still registered")
		default:
		}

		registry.remove(first)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("wait returned with one handle still registered")
		default:
		}

		registry.remove(second)
		waitForRegistrySignal(t, done, "wait did not return after every handle was removed")
	})
}

func TestHandleRegistryWakesEveryWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newHandleRegistry()
		handle := &Handle{}
		registry.add(handle)
		defer registry.remove(handle)

		const waiterCount = 64
		done := make(chan struct{}, waiterCount)
		for range waiterCount {
			go func() {
				registry.wait()
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("waiters returned before the handle was removed")
		}

		registry.remove(handle)
		for range waiterCount {
			waitForRegistrySignal(t, done, "not every concurrent waiter returned")
		}
	})
}

func TestHandleRegistryCreatesFreshEmptyChannelForEachGeneration(t *testing.T) {
	registry := newHandleRegistry()
	first := &Handle{}
	registry.add(first)
	firstGeneration := registry.empty

	select {
	case <-firstGeneration:
		t.Fatal("active generation channel was already closed")
	default:
	}

	registry.remove(first)
	select {
	case <-firstGeneration:
	default:
		t.Fatal("completed generation channel was not closed")
	}

	second := &Handle{}
	registry.add(second)
	secondGeneration := registry.empty
	if secondGeneration == firstGeneration {
		t.Fatal("completed generation channel was reused")
	}
	select {
	case <-secondGeneration:
		t.Fatal("new active generation channel was already closed")
	default:
	}

	registry.remove(second)
	select {
	case <-secondGeneration:
	default:
		t.Fatal("second generation channel was not closed")
	}
}

func TestHandleRegistryIgnoresUnknownAndRepeatedRemoval(t *testing.T) {
	registry := newHandleRegistry()
	tracked := &Handle{}
	unknown := &Handle{}
	registry.add(tracked)
	generation := registry.empty

	registry.remove(unknown)
	select {
	case <-generation:
		t.Fatal("unknown removal closed the active generation")
	default:
	}

	registry.remove(tracked)
	registry.remove(tracked)
	select {
	case <-generation:
	default:
		t.Fatal("tracked handle removal did not close the generation")
	}
}

func TestHandleRegistryConcurrentAddAndLastRemovalDoNotDeadlock(t *testing.T) {
	registry := newHandleRegistry()

	const rounds = 500
	for range rounds {
		first := &Handle{}
		second := &Handle{}
		registry.add(first)

		waitDone := make(chan struct{})
		go func() {
			registry.wait()
			close(waitDone)
		}()

		start := make(chan struct{})
		var transitions sync.WaitGroup
		transitions.Add(2)
		go func() {
			defer transitions.Done()
			<-start
			registry.add(second)
		}()
		go func() {
			defer transitions.Done()
			<-start
			registry.remove(first)
		}()
		close(start)
		transitions.Wait()

		registry.remove(second)
		waitForRegistrySignal(t, waitDone, "wait deadlocked during an add and final removal race")
	}
}

func TestWaitHandlesConcurrentImmediateCompletion(t *testing.T) {
	const taskCount = 128
	type launchResult struct {
		handle *Handle
		err    error
	}

	start := make(chan struct{})
	results := make(chan launchResult, taskCount)
	for range taskCount {
		go func() {
			<-start
			handle, err := SafeGo(context.Background(), func(context.Context) {}, noRetryPolicy)
			results <- launchResult{handle: handle, err: err}
		}()
	}
	close(start)

	handles := make([]*Handle, 0, taskCount)
	for range taskCount {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		handles = append(handles, result.handle)
	}

	waitDone := make(chan struct{})
	go func() {
		Wait()
		close(waitDone)
	}()
	waitForRegistrySignal(t, waitDone, "Wait deadlocked after concurrent immediate completions")

	for _, handle := range handles {
		select {
		case <-handle.Done():
		default:
			t.Fatal("Wait returned before a tracked handle's Done channel closed")
		}
	}
}

func TestWaitHandlesConcurrentCancellation(t *testing.T) {
	Wait()
	synctest.Test(t, func(t *testing.T) {
		const taskCount = 64
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var started sync.WaitGroup
		started.Add(taskCount)
		handles := make([]*Handle, 0, taskCount)
		for range taskCount {
			handle, err := SafeGo(ctx, func(ctx context.Context) {
				started.Done()
				<-ctx.Done()
			}, noRetryPolicy)
			if err != nil {
				t.Fatal(err)
			}
			handles = append(handles, handle)
		}
		started.Wait()

		waitDone := make(chan struct{})
		go func() {
			Wait()
			close(waitDone)
		}()
		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("Wait returned before active tasks were canceled")
		default:
		}

		cancel()
		waitForRegistrySignal(t, waitDone, "Wait deadlocked during concurrent task cancellation")
		for _, handle := range handles {
			select {
			case <-handle.Done():
			default:
				t.Fatal("Wait returned before canceled handle cleanup completed")
			}
		}
	})
}

func TestWaitHandlesCancellationRacingSafeGoStart(t *testing.T) {
	const attemptCount = 256
	type launchResult struct {
		handle *Handle
		err    error
	}

	results := make(chan launchResult, attemptCount)
	var cancelers sync.WaitGroup
	cancelers.Add(attemptCount)
	for range attemptCount {
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		go func() {
			defer cancelers.Done()
			<-start
			cancel()
		}()
		go func() {
			<-start
			handle, err := SafeGo(ctx, func(ctx context.Context) {
				<-ctx.Done()
			}, noRetryPolicy)
			results <- launchResult{handle: handle, err: err}
		}()
		close(start)
	}

	handles := make([]*Handle, 0, attemptCount)
	for range attemptCount {
		result := <-results
		if result.err != nil {
			if !errors.Is(result.err, context.Canceled) {
				t.Fatalf("SafeGo error = %v, want context.Canceled", result.err)
			}
			if result.handle != nil {
				t.Fatal("rejected SafeGo returned a handle")
			}
			continue
		}
		handles = append(handles, result.handle)
	}
	cancelers.Wait()

	waitDone := make(chan struct{})
	go func() {
		Wait()
		close(waitDone)
	}()
	waitForRegistrySignal(t, waitDone, "Wait deadlocked after cancellation raced SafeGo startup")
	for _, handle := range handles {
		select {
		case <-handle.Done():
		default:
			t.Fatal("Wait returned before a race-started handle completed")
		}
	}
}

func TestWaitHandlesSafeGoGoexit(t *testing.T) {
	handle, err := SafeGo(context.Background(), func(context.Context) {
		runtime.Goexit()
	}, noRetryPolicy)
	if err != nil {
		t.Fatal(err)
	}

	waitDone := make(chan struct{})
	go func() {
		Wait()
		close(waitDone)
	}()
	waitForRegistrySignal(t, waitDone, "Wait deadlocked after a task called runtime.Goexit")
	select {
	case <-handle.Done():
	default:
		t.Fatal("Wait returned before the Goexit handle completed")
	}
}
