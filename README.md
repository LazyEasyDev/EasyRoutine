# EasyRoutine

EasyRoutine exposes three main functions:

- `InitLease` sets the coordination backend used by unique supervisors.
- `Go` launches a local panic-safe goroutine.
- `StartUniqueSupervisor` continuously supervises a task under a distributed lease.

EasyRoutine does not include a Redis implementation. Applications provide any
backend that implements `LeaseProvider`.

## Backend interface

```go
type LeaseProvider interface {
	Acquire(ctx context.Context, name, owner string, ttl time.Duration) bool
	Renew(ctx context.Context, name, owner string, ttl time.Duration) bool
	Release(ctx context.Context, name, owner string) bool
}
```

`Acquire`, `Renew`, and `Release` must be atomic. `Renew` and `Release` must
change a lease only when `owner` still owns it. Each method must return promptly
when its context is canceled. Return `true` when the operation succeeds and
`false` when it fails. EasyRoutine retries acquisition after a `false` result.

Initialize lease coordination once when the process starts:

```go
if err := EasyRoutine.InitLease(myLeaseProvider); err != nil {
	log.Fatal(err)
}
```

`Go` requires no initialization. `InitLease` is required only for
`StartUniqueSupervisor` and may be called only once. A second call returns an
error so running jobs cannot be split across different coordination backends.

Lease timing is fixed: a 60-second lease, renewal every 15 seconds, acquisition
retry every 30 seconds, normal task restart after 90 seconds, and an 8-second
release timeout. Each renewal call receives a 15-second deadline.

The `appCtx` used below is a non-nil application context. Its deadline and
values propagate to each task. Canceling it requests shutdown; a unique
supervisor keeps renewing its lease until cooperative task cleanup finishes.

## Go

```go
handle := EasyRoutine.Go(appCtx, func(ctx context.Context) {
	process(ctx)
}, func(recovered EasyRoutine.Panic) EasyRoutine.PanicRetry {
	log.Printf("routine panicked: %v\n%s", recovered.Value, recovered.Stack)
	return EasyRoutine.PanicRetry90s
})
```

The panic handler controls when the task starts again from the beginning:

- `PanicRetry30s` retries after 30 seconds.
- `PanicRetry90s` retries after 90 seconds.
- `PanicRetry300s` retries after 300 seconds.

A nil panic handler, an unsupported retry value, or a panic from the handler
defaults to 90 seconds. Canceling the context or calling `Stop()` prevents a
pending restart. `Go` finishes when its task returns normally.

Each attempt receives its own child context. If an attempt panics, EasyRoutine
cancels that context before calling the panic handler; a retry receives a fresh
context. This signals context-aware work started by the failed attempt to stop,
but EasyRoutine cannot wait for goroutines that the task starts and does not
track.

## Unique supervisor

```go
supervisor, err := EasyRoutine.StartUniqueSupervisor(appCtx, "queue-consumer", func(ctx context.Context) {
	consumeQueue(ctx)
}, onPanic)
if err != nil {
	log.Fatal(err)
}
```

Every deployed process may start a supervisor with the same name. During normal
operation, only the process holding the lease runs the task. Other supervisors
keep trying to acquire it until they are stopped.

Once it owns the lease, a supervisor keeps renewing it while the task runs and
during retry delays. A panic uses the delay selected by `PanicHandler`; a normal
return restarts after 90 seconds. Losing the lease cancels the current task and
returns the supervisor to acquisition. Only parent-context cancellation or
`Stop()` ends the supervisor.

## Stopping

```go
supervisor.Stop()
supervisor.Wait()
```

EasyRoutine derives the callback context from the supplied parent context.
Canceling the parent or calling `Stop()` cancels the callback context. Stopping
is cooperative, so tasks must observe `ctx.Done()` and pass the supplied context
to network and database calls. A unique supervisor continues renewing its lease
while the task performs graceful cleanup, then releases the lease after cleanup
finishes.

## Distributed safety

A time-based lease prevents concurrent owners during normal operation, but it
cannot by itself prevent stale work after a long process pause or network
partition. Use idempotent operations or backend-specific fencing tokens for
side effects that require strict ordering.