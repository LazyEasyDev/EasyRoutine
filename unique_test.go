package EasyRoutine

import (
	"context"
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
}

type coordinatedLossLease struct {
	memoryLease
	acquires       atomic.Int32
	renews         atomic.Int32
	renewalStarted chan struct{}
	loseLease      chan struct{}
}

type blockingRenewLease struct {
	memoryLease
	deadlineSeen chan bool
}

type panicAcquireLease struct {
	attempted chan struct{}
}

type panicReleaseLease struct{}

func (c *countingLease) Acquire(ctx context.Context, name, owner string, ttl time.Duration) bool {
	c.acquires.Add(1)
	return c.memoryLease.Acquire(ctx, name, owner, ttl)
}

func (c *coordinatedLossLease) Acquire(ctx context.Context, name, owner string, ttl time.Duration) bool {
	c.acquires.Add(1)
	return c.memoryLease.Acquire(ctx, name, owner, ttl)
}

func (c *coordinatedLossLease) Renew(ctx context.Context, name, owner string, ttl time.Duration) bool {
	if c.renews.Add(1) == 1 {
		close(c.renewalStarted)
		select {
		case <-ctx.Done():
			return false
		case <-c.loseLease:
			return false
		}
	}
	return c.memoryLease.Renew(ctx, name, owner, ttl)
}

func (b *blockingRenewLease) Renew(ctx context.Context, _ string, _ string, _ time.Duration) bool {
	_, hasDeadline := ctx.Deadline()
	b.deadlineSeen <- hasDeadline
	<-ctx.Done()
	return false
}

func (p *panicAcquireLease) Acquire(context.Context, string, string, time.Duration) bool {
	p.attempted <- struct{}{}
	panic("acquire failed")
}

func (*panicAcquireLease) Renew(context.Context, string, string, time.Duration) bool {
	return true
}

func (*panicAcquireLease) Release(context.Context, string, string) bool {
	return true
}

func (panicReleaseLease) Acquire(context.Context, string, string, time.Duration) bool {
	return true
}

func (panicReleaseLease) Renew(context.Context, string, string, time.Duration) bool {
	return true
}

func (panicReleaseLease) Release(context.Context, string, string) bool {
	panic("release failed")
}

func (p *panicRenewLease) Renew(context.Context, string, string, time.Duration) bool {
	panic("renew failed")
}

func (m *memoryLease) Acquire(_ context.Context, _ string, owner string, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.owner != "" && time.Now().Before(m.expires) {
		return false
	}
	m.owner = owner
	m.expires = time.Now().Add(ttl)
	return true
}

func (m *memoryLease) Renew(_ context.Context, _ string, owner string, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.owner != owner || time.Now().After(m.expires) {
		return false
	}
	m.expires = time.Now().Add(ttl)
	return true
}

func (m *memoryLease) Release(_ context.Context, _ string, owner string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.owner != owner {
		return false
	}
	m.owner = ""
	m.expires = time.Time{}
	return true
}

func TestUniqueSupervisorUsesInitializedBackend(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := InitLease(&memoryLease{}); err != nil {
		t.Fatal(err)
	}
	if err := InitLease(&memoryLease{}); err == nil {
		t.Fatal("expected repeated initialization error")
	}
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	supervisor, err := StartUniqueSupervisor(ctx, "reports", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, started)
	cancel()
	supervisor.Wait()
}

func TestUniqueSupervisorKeepsLeaseDuringPanicRetry(t *testing.T) {
	resetDefaultCoordinator(t)
	backend := &countingLease{}
	if err := InitLease(backend); err != nil {
		t.Fatal(err)
	}

	panicHandled := make(chan struct{})
	supervisor, err := StartUniqueSupervisor(context.Background(), "reports", func(context.Context) {
		panic("retry")
	}, func(Panic) PanicRetry {
		close(panicHandled)
		return PanicRetry30s
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, panicHandled)
	backend.mu.Lock()
	owner := backend.owner
	backend.mu.Unlock()
	if owner == "" {
		t.Fatal("lease was released during panic retry delay")
	}
	if backend.acquires.Load() != 1 {
		t.Fatalf("lease acquisitions = %d, want 1", backend.acquires.Load())
	}

	supervisor.Stop()
	supervisor.Wait()
}

func TestUniqueSupervisorRestartsAfterNormalReturn(t *testing.T) {
	backend := &countingLease{}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 20 * time.Millisecond,
			retry:     5 * time.Millisecond,
			restart:   10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{}, 3)
	supervisor, err := configured.startUniqueSupervisor(context.Background(), "reports", func(context.Context) {
		started <- struct{}{}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, started)
	waitForSignal(t, started)
	if backend.acquires.Load() != 1 {
		t.Fatalf("lease acquisitions = %d, want 1", backend.acquires.Load())
	}
	select {
	case <-supervisor.Done():
		t.Fatal("supervisor stopped after the task returned")
	default:
	}
}

func TestUniqueSupervisorReacquiresAfterLeaseLossRacesTaskReturn(t *testing.T) {
	backend := &coordinatedLossLease{
		renewalStarted: make(chan struct{}),
		loseLease:      make(chan struct{}),
	}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			retry:     5 * time.Millisecond,
			restart:   time.Second,
			release:   time.Second,
		},
	}

	started := make(chan struct{}, 2)
	var attempts atomic.Int32
	supervisor, err := configured.startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		started <- struct{}{}
		if attempts.Add(1) == 1 {
			<-backend.renewalStarted
			close(backend.loseLease)
			return
		}
		<-ctx.Done()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		supervisor.Stop()
		supervisor.Wait()
	})

	waitForSignal(t, started)
	waitForSignal(t, started)
	if backend.acquires.Load() < 2 {
		t.Fatalf("lease acquisitions = %d, want at least 2", backend.acquires.Load())
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
			retry:     time.Second,
			restart:   time.Second,
			release:   time.Second,
		},
	}

	stopped := make(chan struct{})
	supervisor, err := configured.startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		<-ctx.Done()
		close(stopped)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
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

func TestBackendPanicsDoNotReachTaskPanicHandler(t *testing.T) {
	backend := &panicAcquireLease{attempted: make(chan struct{}, 2)}
	configured := &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			retry:     5 * time.Millisecond,
			restart:   time.Second,
			release:   time.Second,
		},
	}

	var handled atomic.Int32
	supervisor, err := configured.startUniqueSupervisor(context.Background(), "reports", func(context.Context) {}, func(Panic) PanicRetry {
		handled.Add(1)
		return PanicRetry30s
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, backend.attempted)
	waitForSignal(t, backend.attempted)
	supervisor.Stop()
	supervisor.Wait()
	if handled.Load() != 0 {
		t.Fatalf("task panic handler calls = %d, want 0", handled.Load())
	}

	releaseTaskStarted := make(chan struct{})
	releaseSupervisor, err := (&coordinator{
		backend: panicReleaseLease{},
		timing: leaseTiming{
			ttl:       time.Second,
			heartbeat: 10 * time.Millisecond,
			retry:     time.Second,
			restart:   time.Second,
			release:   time.Second,
		},
	}).startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		close(releaseTaskStarted)
		<-ctx.Done()
	}, func(Panic) PanicRetry {
		handled.Add(1)
		return PanicRetry30s
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, releaseTaskStarted)
	releaseSupervisor.Stop()
	releaseSupervisor.Wait()
	if handled.Load() != 0 {
		t.Fatalf("task panic handler calls = %d, want 0", handled.Load())
	}
}

func TestCoordinatorsFailOverWithoutOverlap(t *testing.T) {
	backend := &memoryLease{}
	timing := leaseTiming{
		ttl:       200 * time.Millisecond,
		heartbeat: 40 * time.Millisecond,
		retry:     10 * time.Millisecond,
		restart:   10 * time.Millisecond,
		release:   time.Second,
	}
	first := &coordinator{backend: backend, timing: timing}
	second := &coordinator{backend: backend, timing: timing}

	var active atomic.Int32
	var overlap atomic.Bool
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	task := func(started chan struct{}) Task {
		return func(ctx context.Context) {
			if active.Add(1) != 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			close(started)
			<-ctx.Done()
		}
	}

	firstSupervisor, err := first.startUniqueSupervisor(context.Background(), "reports", task(firstStarted), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, firstStarted)

	secondSupervisor, err := second.startUniqueSupervisor(context.Background(), "reports", task(secondStarted), nil)
	if err != nil {
		t.Fatal(err)
	}

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
		retry:     5 * time.Millisecond,
		restart:   5 * time.Millisecond,
		release:   time.Second,
	}
	first := &coordinator{backend: backend, timing: timing}
	second := &coordinator{backend: backend, timing: timing}

	firstStarted := make(chan struct{})
	cleanupStarted := make(chan struct{})
	finishCleanup := make(chan struct{})
	firstSupervisor, err := first.startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		close(firstStarted)
		<-ctx.Done()
		close(cleanupStarted)
		<-finishCleanup
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, firstStarted)

	secondStarted := make(chan struct{})
	secondSupervisor, err := second.startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		close(secondStarted)
		<-ctx.Done()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

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
			retry:     time.Second,
			restart:   10 * time.Millisecond,
			release:   time.Second,
		},
	}

	started := make(chan struct{})
	stopped := make(chan struct{})
	supervisor, err := configured.startUniqueSupervisor(context.Background(), "reports", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(stopped)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, started)
	waitForSignal(t, stopped)
	supervisor.Stop()
	supervisor.Wait()
}

func TestInitLeaseRequiresBackend(t *testing.T) {
	if err := InitLease(nil); err == nil {
		t.Fatal("expected missing backend error")
	}
}

func TestCoordinatorDefaults(t *testing.T) {
	resetDefaultCoordinator(t)
	if err := InitLease(&memoryLease{}); err != nil {
		t.Fatal(err)
	}
	configured := defaultCoordinator

	if configured.timing.ttl != 60*time.Second {
		t.Fatalf("lease TTL = %s, want 60s", configured.timing.ttl)
	}
	if configured.timing.heartbeat != 15*time.Second {
		t.Fatalf("heartbeat interval = %s, want 15s", configured.timing.heartbeat)
	}
	if configured.timing.retry != 30*time.Second {
		t.Fatalf("retry interval = %s, want 30s", configured.timing.retry)
	}
	if configured.timing.restart != 90*time.Second {
		t.Fatalf("restart interval = %s, want 90s", configured.timing.restart)
	}
	if configured.timing.release != 8*time.Second {
		t.Fatalf("release timeout = %s, want 8s", configured.timing.release)
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
