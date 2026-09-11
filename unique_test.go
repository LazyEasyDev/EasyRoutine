package EasyRoutine

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryLease struct {
	mu      sync.Mutex
	owner   string
	expires time.Time
}

type panicRenewLease struct {
	*memoryLease
}

type countingLease struct {
	memoryLease
	acquires atomic.Int32
	renews   atomic.Int32
	renewed  chan struct{}
	released chan struct{}
}

type coordinatedLossLease struct {
	memoryLease
	acquires       atomic.Int32
	renews         atomic.Int32
	renewalStarted chan struct{}
	loseLease      chan struct{}
	owners         chan string
}

type blockingRenewLease struct {
	memoryLease
	deadlineSeen chan bool
}

type panicAcquireLease struct {
	attempted chan struct{}
}

type panicReleaseLease struct{}

type retryReleaseLease struct {
	memoryLease
	attempts atomic.Int32
	released chan struct{}
}

type recordedLeaseAction struct {
	action LeaseAction
	state  leaseState
}

type recordingLease struct {
	memoryLease
	callsMu sync.Mutex
	calls   []recordedLeaseAction
	called  chan LeaseAction
}

type logQueryLease struct {
	memoryLease
	logs        []SupervisorLog
	statuses    []SupervisorStatus
	names       []string
	statusNames []string
}

type panicLogLease struct {
	memoryLease
}

func (*panicLogLease) GetLogs(context.Context, ...string) ([]SupervisorLog, error) {
	panic("log query failed")
}

func (*panicLogLease) GetStatuses(context.Context, ...string) ([]SupervisorStatus, error) {
	panic("status query failed")
}

func (l *logQueryLease) GetLogs(_ context.Context, names ...string) ([]SupervisorLog, error) {
	l.names = append([]string(nil), names...)
	return append([]SupervisorLog(nil), l.logs...), nil
}

func (l *logQueryLease) GetStatuses(_ context.Context, names ...string) ([]SupervisorStatus, error) {
	l.statusNames = append([]string(nil), names...)
	return append([]SupervisorStatus(nil), l.statuses...), nil
}

func (r *recordingLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	applied := r.memoryLease.Action(ctx, action, state)
	if !applied {
		return false
	}
	r.callsMu.Lock()
	r.calls = append(r.calls, recordedLeaseAction{action: action, state: state})
	r.callsMu.Unlock()
	if r.called != nil {
		r.called <- action
	}
	return true
}

func (r *recordingLease) recordedActions() []recordedLeaseAction {
	r.callsMu.Lock()
	defer r.callsMu.Unlock()
	return append([]recordedLeaseAction(nil), r.calls...)
}

func (c *countingLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	switch action {
	case LeaseAcquire:
		c.acquires.Add(1)
	case LeaseRenew:
		c.renews.Add(1)
	}
	applied := c.memoryLease.Action(ctx, action, state)
	if applied && action == LeaseRenew && c.renewed != nil {
		select {
		case c.renewed <- struct{}{}:
		default:
		}
	}
	if applied && action == LeaseRelease && c.released != nil {
		c.released <- struct{}{}
	}
	return applied
}

func (c *coordinatedLossLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	if action == LeaseAcquire {
		c.acquires.Add(1)
	}
	if action == LeaseRenew && c.renews.Add(1) == 1 {
		close(c.renewalStarted)
		select {
		case <-ctx.Done():
			return false
		case <-c.loseLease:
			return false
		}
	}
	applied := c.memoryLease.Action(ctx, action, state)
	if applied && action == LeaseAcquire && c.owners != nil {
		c.owners <- state.Owner
	}
	return applied
}

func (b *blockingRenewLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	if action != LeaseRenew {
		return b.memoryLease.Action(ctx, action, state)
	}
	_, hasDeadline := ctx.Deadline()
	b.deadlineSeen <- hasDeadline
	<-ctx.Done()
	return false
}

func (p *panicAcquireLease) Action(context.Context, LeaseAction, leaseState) bool {
	p.attempted <- struct{}{}
	panic("action failed")
}

func (panicReleaseLease) Action(_ context.Context, action LeaseAction, _ leaseState) bool {
	if action == LeaseRelease {
		panic("release failed")
	}
	return true
}

func (r *retryReleaseLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	if action == LeaseRelease {
		if r.attempts.Add(1) < 3 {
			return false
		}
	}
	applied := r.memoryLease.Action(ctx, action, state)
	if applied && action == LeaseRelease {
		r.released <- struct{}{}
	}
	return applied
}

func (p *panicRenewLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	if action == LeaseRenew {
		panic("renew failed")
	}
	return p.memoryLease.Action(ctx, action, state)
}

func (m *memoryLease) Action(_ context.Context, action LeaseAction, state leaseState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch action {
	case LeaseAcquire:
		if m.owner != "" && time.Now().Before(m.expires) {
			return false
		}
		m.owner = state.Owner
		m.expires = time.Now().Add(state.TTL)
		return true
	case LeaseRenew:
		if m.owner != state.Owner || time.Now().After(m.expires) {
			return false
		}
		m.expires = time.Now().Add(state.TTL)
		return true
	case LeaseRelease:
		if m.owner != state.Owner {
			return false
		}
		m.owner = ""
		m.expires = time.Time{}
		return true
	default:
		return false
	}
}

func (*memoryLease) GetLogs(context.Context, ...string) ([]SupervisorLog, error) {
	return nil, nil
}

func (*memoryLease) GetStatuses(context.Context, ...string) ([]SupervisorStatus, error) {
	return nil, nil
}

func (*panicAcquireLease) GetLogs(context.Context, ...string) ([]SupervisorLog, error) {
	return nil, nil
}

func (*panicAcquireLease) GetStatuses(context.Context, ...string) ([]SupervisorStatus, error) {
	return nil, nil
}

func (panicReleaseLease) GetLogs(context.Context, ...string) ([]SupervisorLog, error) {
	return nil, nil
}

func (panicReleaseLease) GetStatuses(context.Context, ...string) ([]SupervisorStatus, error) {
	return nil, nil
}

func TestUniqueSupervisorUsesInitializedBackend(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := initLease(&memoryLease{}); err != nil {
		t.Fatal(err)
	}
	if err := initLease(&memoryLease{}); err == nil {
		t.Fatal("expected repeated initialization error")
	}
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	supervisor, err := StartUniqueSupervisor(ctx, "reports", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}, func(Panic) {}, 0)
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, started)
	cancel()
	supervisor.Wait()
}

func TestUniqueSupervisorStopsAfterTaskGoexit(t *testing.T) {
	backend := &countingLease{released: make(chan struct{}, 1)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{})
	var handled atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			close(started)
			runtime.Goexit()
		},
	}, func(Panic) {
		handled.Add(1)
	})
	waitForSignal(t, started)
	waitForSignal(t, backend.released)
	select {
	case <-supervisor.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop after task called runtime.Goexit")
	}
	if handled.Load() != 0 {
		t.Fatalf("panic handler calls = %d, want 0", handled.Load())
	}
}

func TestUniqueSupervisorReleasesLeaseDuringPanicCooldown(t *testing.T) {
	backend := &countingLease{released: make(chan struct{}, 1)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	panicHandlerSawRelease := make(chan bool, 1)
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			panic("retry")
		},
	}, func(Panic) {
		backend.mu.Lock()
		panicHandlerSawRelease <- backend.owner == ""
		backend.mu.Unlock()
	})
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	if releasedBeforeHandler := <-panicHandlerSawRelease; !releasedBeforeHandler {
		t.Fatal("panic handler ran before the lease was released")
	}
	waitForSignal(t, backend.released)
	backend.mu.Lock()
	owner := backend.owner
	backend.mu.Unlock()
	if owner != "" {
		t.Fatal("lease was retained after task panic")
	}
	if backend.acquires.Load() != 1 {
		t.Fatalf("lease acquisitions = %d, want 1", backend.acquires.Load())
	}
	time.Sleep(25 * time.Millisecond)
	if backend.acquires.Load() != 1 {
		t.Fatalf("lease acquisitions during panic cooldown = %d, want 1", backend.acquires.Load())
	}
}

func TestUniqueSupervisorReportsLifecycleStateInLeaseActions(t *testing.T) {
	backend := &recordingLease{called: make(chan LeaseAction, 4)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: time.Hour,
			release:   time.Second,
		},
	}

	var attempts atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			if attempts.Add(1) == 3 {
				panic("boom")
			}
		},
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForLeaseAction(t, backend.called, LeaseAcquire)
	waitForLeaseAction(t, backend.called, LeaseRelease)
	actions := backend.recordedActions()
	if len(actions) != 2 {
		t.Fatalf("lease actions = %#v, want acquire and release", actions)
	}
	acquired := actions[0]
	if acquired.action != LeaseAcquire || acquired.state.Status != RoutineNotStarted {
		t.Fatalf("acquire = %#v, want not-started acquire", acquired)
	}
	if acquired.state.SuccessCount != 0 || acquired.state.FailureCount != 0 || acquired.state.Log != serverLog("") {
		t.Fatalf("initial state = %#v, want zero counters and server tag log", acquired.state)
	}
	released := actions[1]
	if released.action != LeaseRelease || released.state.Status != RoutinePanic {
		t.Fatalf("release = %#v, want panic release", released)
	}
	if released.state.SuccessCount != 2 || released.state.FailureCount != 1 {
		t.Fatalf("panic counters = (%d, %d), want (2, 1)", released.state.SuccessCount, released.state.FailureCount)
	}
	if !strings.HasPrefix(released.state.Log, serverLog("")+"\n") || !strings.Contains(released.state.Log, "boom") || !strings.Contains(released.state.Log, "TestUniqueSupervisorReportsLifecycleStateInLeaseActions") {
		t.Fatalf("panic log = %q, want panic value and stack", released.state.Log)
	}
}

func TestUniqueSupervisorReportsCompletedWorkOnRenew(t *testing.T) {
	backend := &recordingLease{called: make(chan LeaseAction, 6)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{})
	finish := make(chan struct{})
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			close(started)
			<-finish
		},
		RepeatAfter: time.Hour,
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForLeaseAction(t, backend.called, LeaseAcquire)
	waitForSignal(t, started)
	waitForLeaseAction(t, backend.called, LeaseRenew)
	actions := backend.recordedActions()
	if len(actions) < 2 {
		t.Fatalf("lease actions = %#v, want acquire and renew", actions)
	}
	running := actions[1]
	if running.state.Status != RoutineRunning || running.state.SuccessCount != 0 || running.state.FailureCount != 0 || running.state.Log != serverLog("") {
		t.Fatalf("running renew state = %#v, want active task", running.state)
	}

	close(finish)
	waitForLeaseAction(t, backend.called, LeaseRenew)
	actions = backend.recordedActions()
	renewed := actions[2]
	if renewed.state.Status != RoutineDone || renewed.state.SuccessCount != 1 || renewed.state.FailureCount != 0 || renewed.state.Log != serverLog("") {
		t.Fatalf("renew state = %#v, want one successful completion", renewed.state)
	}
}

func TestUniqueSupervisorRetriesFailedReleaseAfterPanic(t *testing.T) {
	backend := &retryReleaseLease{released: make(chan struct{}, 1)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 5 * time.Millisecond,
			release:   time.Second,
		},
	}

	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			panic("retry release")
		},
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, backend.released)
	if backend.attempts.Load() != 3 {
		t.Fatalf("release attempts = %d, want 3", backend.attempts.Load())
	}
}

func TestUniqueSupervisorRestartsAfterNormalReturn(t *testing.T) {
	backend := &countingLease{renewed: make(chan struct{}, 1)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 5 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{}, 3)
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(context.Context) {
			started <- struct{}{}
		},
		RepeatAfter: 10 * time.Millisecond,
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, started)
	waitForSignal(t, backend.renewed)
	waitForSignal(t, started)
	if backend.acquires.Load() != 1 {
		t.Fatalf("lease acquisitions = %d, want 1", backend.acquires.Load())
	}
	if backend.renews.Load() == 0 {
		t.Fatal("lease was not renewed after normal task return")
	}
	select {
	case <-supervisor.Done():
		t.Fatal("supervisor stopped after the task returned")
	default:
	}
}

func TestUniqueSupervisorUsesTaskRepeatDelay(t *testing.T) {
	configured := &coordinator{
		backend: &memoryLease{},
		timing: leaseTiming{
			ttl:       2 * time.Second,
			heartbeat: time.Second,
			release:   time.Second,
		},
	}

	firstStarted := make(chan struct{})
	finishFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var attempts atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			if attempts.Add(1) == 1 {
				close(firstStarted)
				<-finishFirst
				return
			}
			close(secondStarted)
			<-ctx.Done()
		},
		RepeatAfter: 50 * time.Millisecond,
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, firstStarted)
	close(finishFirst)
	select {
	case <-secondStarted:
		t.Fatal("task repeated before RepeatAfter elapsed")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-secondStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task waited for heartbeat instead of RepeatAfter")
	}
}

func TestUniqueSupervisorRepeatsImmediatelyByDefault(t *testing.T) {
	configured := &coordinator{
		backend: &memoryLease{},
		timing: leaseTiming{
			ttl:       2 * time.Hour,
			heartbeat: time.Hour,
			release:   time.Second,
		},
	}

	firstStarted := make(chan struct{})
	finishFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var attempts atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			if attempts.Add(1) == 1 {
				close(firstStarted)
				<-finishFirst
				return
			}
			close(secondStarted)
			<-ctx.Done()
		},
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, firstStarted)
	close(finishFirst)
	waitForSignal(t, secondStarted)
}

func TestStartUniqueSupervisorRejectsInvalidArguments(t *testing.T) {
	resetDefaultCoordinator(t)
	run := func(context.Context) {}
	onPanic := func(Panic) {}

	var nilContext context.Context
	if _, err := StartUniqueSupervisor(nilContext, "reports", run, onPanic, 0); err == nil {
		t.Fatal("expected nil context error")
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := StartUniqueSupervisor(canceledCtx, "reports", run, onPanic, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
	if _, err := StartUniqueSupervisor(context.Background(), strings.Repeat("a", 256), run, onPanic, 0); err == nil {
		t.Fatal("expected oversized task name error")
	}
	if _, err := StartUniqueSupervisor(context.Background(), "reports", nil, onPanic, 0); err == nil {
		t.Fatal("expected missing task function error")
	}
	if _, err := StartUniqueSupervisor(context.Background(), "reports", run, nil, 0); err == nil {
		t.Fatal("expected missing panic handler error")
	}
	if _, err := StartUniqueSupervisor(context.Background(), "reports", run, onPanic, -time.Nanosecond); err == nil {
		t.Fatal("expected negative repeat delay error")
	}
}

func TestUniqueSupervisorReacquiresAfterLeaseLossRacesTaskReturn(t *testing.T) {
	backend := &coordinatedLossLease{
		renewalStarted: make(chan struct{}),
		loseLease:      make(chan struct{}),
		owners:         make(chan string, 2),
	}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{}, 2)
	var attempts atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			started <- struct{}{}
			if attempts.Add(1) == 1 {
				<-backend.renewalStarted
				close(backend.loseLease)
				return
			}
			<-ctx.Done()
		},
		RepeatAfter: 20 * time.Millisecond,
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, started)
	waitForSignal(t, started)
	if backend.acquires.Load() < 2 {
		t.Fatalf("lease acquisitions = %d, want at least 2", backend.acquires.Load())
	}
	firstOwner := <-backend.owners
	secondOwner := <-backend.owners
	if firstOwner == secondOwner {
		t.Fatal("lease owner token was reused after reacquisition")
	}
}

func TestUniqueSupervisorBoundsRenewalCalls(t *testing.T) {
	backend := &blockingRenewLease{
		deadlineSeen: make(chan bool, 1),
	}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	stopped := make(chan struct{})
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			<-ctx.Done()
			close(stopped)
		},
	}, nil)
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	select {
	case hasDeadline := <-backend.deadlineSeen:
		if !hasDeadline {
			t.Fatal("renewal context has no deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for renewal")
	}
	waitForSignal(t, stopped)
}

func TestBackendPanicsDoNotReachSupervisorPanicHandler(t *testing.T) {
	backend := &panicAcquireLease{attempted: make(chan struct{}, 2)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	var handled atomic.Int32
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{Run: func(context.Context) {}}, func(Panic) {
		handled.Add(1)
	})
	waitForSignal(t, backend.attempted)
	waitForSignal(t, backend.attempted)
	supervisor.Stop()
	supervisor.Wait()
	if handled.Load() != 0 {
		t.Fatalf("supervisor panic handler calls = %d, want 0", handled.Load())
	}

	releaseTaskStarted := make(chan struct{})
	releaseSupervisor := (&coordinator{
		backend: panicReleaseLease{},
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}).startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			close(releaseTaskStarted)
			<-ctx.Done()
		},
	}, func(Panic) {
		handled.Add(1)
	})
	waitForSignal(t, releaseTaskStarted)
	releaseSupervisor.Stop()
	releaseSupervisor.Wait()
	if handled.Load() != 0 {
		t.Fatalf("supervisor panic handler calls = %d, want 0", handled.Load())
	}
}

func TestCoordinatorsFailOverWithoutOverlap(t *testing.T) {
	backend := &memoryLease{}
	timing := leaseTiming{
		ttl:       200 * time.Millisecond,
		heartbeat: 40 * time.Millisecond,
		release:   time.Second,
	}
	first := &coordinator{backend: backend, timing: timing}
	second := &coordinator{backend: backend, timing: timing}

	var active atomic.Int32
	var overlap atomic.Bool
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	task := func(started chan struct{}) uniqueTask {
		return uniqueTask{
			Run: func(ctx context.Context) {
				if active.Add(1) != 1 {
					overlap.Store(true)
				}
				defer active.Add(-1)
				close(started)
				<-ctx.Done()
			},
		}
	}

	firstSupervisor := first.startUniqueSupervisor(context.Background(), "reports", task(firstStarted), nil)
	waitForSignal(t, firstStarted)

	secondSupervisor := second.startUniqueSupervisor(context.Background(), "reports", task(secondStarted), nil)

	select {
	case <-secondStarted:
		t.Fatal("second unique routine started before failover")
	case <-time.After(100 * time.Millisecond):
	}

	firstSupervisor.Stop()
	firstSupervisor.Wait()
	waitForSignal(t, secondStarted)
	secondSupervisor.Stop()
	secondSupervisor.Wait()

	if overlap.Load() {
		t.Fatal("unique routines overlapped")
	}
}

func TestStopRenewsLeaseUntilCleanupFinishes(t *testing.T) {
	backend := &memoryLease{}
	timing := leaseTiming{
		ttl:       80 * time.Millisecond,
		heartbeat: 10 * time.Millisecond,
		release:   time.Second,
	}
	first := &coordinator{backend: backend, timing: timing}
	second := &coordinator{backend: backend, timing: timing}

	firstStarted := make(chan struct{})
	cleanupStarted := make(chan struct{})
	finishCleanup := make(chan struct{})
	firstSupervisor := first.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			close(firstStarted)
			<-ctx.Done()
			close(cleanupStarted)
			<-finishCleanup
		},
	}, nil)
	waitForSignal(t, firstStarted)

	secondStarted := make(chan struct{})
	secondSupervisor := second.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			close(secondStarted)
			<-ctx.Done()
		},
	}, nil)
	firstSupervisor.Stop()
	waitForSignal(t, cleanupStarted)
	select {
	case <-secondStarted:
		t.Fatal("second task started while the first task was cleaning up")
	case <-time.After(2 * timing.ttl):
	}

	close(finishCleanup)
	firstSupervisor.Wait()
	waitForSignal(t, secondStarted)
	secondSupervisor.Stop()
	secondSupervisor.Wait()
}

func TestRenewPanicIsContained(t *testing.T) {
	configured := &coordinator{
		backend: &panicRenewLease{memoryLease: &memoryLease{}},
		timing: leaseTiming{
			ttl:       100 * time.Millisecond,
			heartbeat: 10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{})
	stopped := make(chan struct{})
	supervisor := configured.startUniqueSupervisor(context.Background(), "reports", uniqueTask{
		Run: func(ctx context.Context) {
			close(started)
			<-ctx.Done()
			close(stopped)
		},
	}, nil)
	waitForSignal(t, started)
	waitForSignal(t, stopped)
	supervisor.Stop()
	supervisor.Wait()
}

func TestInitLeaseRequiresBackend(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := initLease(nil); err == nil {
		t.Fatal("expected missing backend error")
	}
}

func TestValidateRoutineName(t *testing.T) {
	valid := []string{
		"reports",
		"service=billing/region=eu-west-1@daily*",
		"résumé + sync",
		strings.Repeat("a", 255),
	}
	for _, name := range valid {
		if err := validateRoutineName(name); err != nil {
			t.Errorf("validateRoutineName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{
		"",
		" leading",
		"trailing ",
		"line\nbreak",
		string([]byte{0xff}),
		strings.Repeat("a", 256),
	}
	for _, name := range invalid {
		if err := validateRoutineName(name); err == nil {
			t.Errorf("validateRoutineName(%q) = nil, want error", name)
		}
	}
}

func TestGetLogsUsesInitializedBackend(t *testing.T) {
	resetDefaultCoordinator(t)
	want := []SupervisorLog{{ID: "log-1", Name: "reports"}}
	backend := &logQueryLease{logs: want}
	if err := initLease(backend); err != nil {
		t.Fatal(err)
	}

	logs, err := GetLogs(context.Background(), "reports", "billing")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(logs, want) {
		t.Fatalf("logs = %#v, want %#v", logs, want)
	}
	if !reflect.DeepEqual(backend.names, []string{"reports", "billing"}) {
		t.Fatalf("names = %#v, want selected names", backend.names)
	}

	if _, err := GetLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(backend.names) != 0 {
		t.Fatalf("names = %#v, want all names", backend.names)
	}
}

func TestGetStatusesUsesInitializedBackend(t *testing.T) {
	resetDefaultCoordinator(t)
	want := []SupervisorStatus{{Name: "reports", Status: RoutineRunning}}
	backend := &logQueryLease{statuses: want}
	if err := initLease(backend); err != nil {
		t.Fatal(err)
	}

	statuses, err := GetStatuses(context.Background(), "reports", "billing")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(statuses, want) {
		t.Fatalf("statuses = %#v, want %#v", statuses, want)
	}
	if !reflect.DeepEqual(backend.statusNames, []string{"reports", "billing"}) {
		t.Fatalf("names = %#v, want selected names", backend.statusNames)
	}

	if _, err := GetStatuses(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(backend.statusNames) != 0 {
		t.Fatalf("names = %#v, want all names", backend.statusNames)
	}
}

func TestGetLogsValidatesRequest(t *testing.T) {
	resetDefaultCoordinator(t)
	var nilContext context.Context
	if _, err := GetLogs(nilContext); err == nil {
		t.Fatal("expected nil context error")
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := GetLogs(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
	if _, err := GetLogs(context.Background(), ""); err == nil {
		t.Fatal("expected empty task name error")
	}
	if _, err := GetLogs(context.Background(), "line\nbreak"); err == nil {
		t.Fatal("expected control character error")
	}
	if _, err := GetLogs(context.Background()); err == nil {
		t.Fatal("expected uninitialized provider error")
	}
}

func TestGetStatusesValidatesRequest(t *testing.T) {
	resetDefaultCoordinator(t)
	var nilContext context.Context
	if _, err := GetStatuses(nilContext); err == nil {
		t.Fatal("expected nil context error")
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := GetStatuses(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
	if _, err := GetStatuses(context.Background(), ""); err == nil {
		t.Fatal("expected empty task name error")
	}
	if _, err := GetStatuses(context.Background(), " trailing "); err == nil {
		t.Fatal("expected surrounding whitespace error")
	}
	if _, err := GetStatuses(context.Background()); err == nil {
		t.Fatal("expected uninitialized provider error")
	}
}

func TestGetLogsContainsProviderPanic(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := initLease(&panicLogLease{}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetLogs(context.Background()); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("provider panic error = %v, want contained panic", err)
	}
}

func TestGetStatusesContainsProviderPanic(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := initLease(&panicLogLease{}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetStatuses(context.Background()); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("provider panic error = %v, want contained panic", err)
	}
}

func TestCoordinatorDefaults(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := initLease(&memoryLease{}); err != nil {
		t.Fatal(err)
	}
	configured := defaultCoordinator

	if configured.timing.ttl != 180*time.Second {
		t.Fatalf("lease TTL = %s, want 180s", configured.timing.ttl)
	}
	if configured.timing.heartbeat != 30*time.Second {
		t.Fatalf("heartbeat interval = %s, want 30s", configured.timing.heartbeat)
	}
	if configured.timing.release != 30*time.Second {
		t.Fatalf("release timeout = %s, want 30s", configured.timing.release)
	}
	if panicReacquireDelay != 300*time.Second {
		t.Fatalf("panic reacquisition delay = %s, want 300s", panicReacquireDelay)
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task")
	}
}

func waitForLeaseAction(t *testing.T, actions <-chan LeaseAction, want LeaseAction) {
	t.Helper()

	select {
	case action := <-actions:
		if action != want {
			t.Fatalf("lease action = %q, want %q", action, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for lease action %q", want)
	}
}

func resetDefaultCoordinator(t *testing.T) {
	t.Helper()
	coordinatorMu.Lock()
	defaultCoordinator = nil
	coordinatorMu.Unlock()
	t.Cleanup(func() {
		coordinatorMu.Lock()
		defaultCoordinator = nil
		coordinatorMu.Unlock()
	})
}
