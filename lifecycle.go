package EasyRoutine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

var (
	lifecycleMu        sync.RWMutex
	defaultContext     context.Context
	defaultCancel      context.CancelFunc
	defaultCoordinator *coordinator
	activeHandles      = newHandleRegistry()
)

// SQLConfig configures SQL coordination for unique supervisors and queries.
// DB and Dialect are required. The caller retains ownership of DB.
type SQLConfig struct {
	DB      *sql.DB
	Dialect SQLDialect
}

// Initialize configures the package lifetime and optionally its SQL backend.
// A nil sqlConfig enables local work only. Otherwise DB must be non-nil and
// Dialect must be supported.
// Failed initialization may be retried. Successful initialization may not be
// repeated, even after Close. The caller retains ownership of ctx and the database.
func Initialize(ctx context.Context, sqlConfig *SQLConfig) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if defaultContext != nil {
		return errors.New("EasyRoutine is already initialized")
	}

	var configured *coordinator
	if sqlConfig != nil {
		if sqlConfig.DB == nil {
			return errors.New("SQL database is required")
		}
		backend, err := newSQLLease(sqlConfig.DB, sqlConfig.Dialect)
		if err != nil {
			return err
		}
		if err := backend.ensureSchema(ctx); err != nil {
			return err
		}
		configured = &coordinator{
			backend: backend,
			timing: leaseTiming{
				ttl:       leaseTTL,
				heartbeat: heartbeatInterval,
				release:   releaseTimeout,
			},
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	defaultContext, defaultCancel = context.WithCancel(ctx)
	defaultCoordinator = configured
	return nil
}

// Close requests cancellation of all managed work without waiting for cleanup.
// It is safe to call repeatedly or before Initialize. It does not cancel the
// caller's context or close the caller's database. Use Wait to await completion.
func Close() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if defaultCancel != nil {
		defaultCancel()
	}
}

// Wait blocks until all successfully started local tasks and unique supervisors
// have completed, including WithCtx variants. Operations started before Wait
// observes no active handles are included, including operations started while
// Wait is blocked.
// Wait does not stop or cancel managed work; call Close to request shutdown.
func Wait() {
	activeHandles.wait()
}

func initializedContext() (context.Context, error) {
	lifecycleMu.RLock()
	defer lifecycleMu.RUnlock()
	if defaultContext == nil {
		return nil, errors.New("EasyRoutine is not initialized")
	}
	return defaultContext, nil
}

func startManagedHandle(ctx context.Context, run func(context.Context)) (*Handle, error) {
	lifecycleMu.RLock()
	defer lifecycleMu.RUnlock()
	if defaultContext == nil {
		return nil, errors.New("EasyRoutine is not initialized")
	}
	if err := defaultContext.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == defaultContext.Done() {
		return startHandle(ctx, run, activeHandles), nil
	}

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(defaultContext, cancel)
	return startHandle(ctx, func(ctx context.Context) {
		defer stop()
		defer cancel()
		run(ctx)
	}, activeHandles), nil
}

type handleRegistry struct {
	mu      sync.Mutex
	empty   chan struct{}
	handles map[*Handle]struct{}
}

func newHandleRegistry() *handleRegistry {
	return &handleRegistry{handles: make(map[*Handle]struct{})}
}

func (r *handleRegistry) add(handle *Handle) {
	r.mu.Lock()
	if len(r.handles) == 0 {
		r.empty = make(chan struct{})
	}
	r.handles[handle] = struct{}{}
	r.mu.Unlock()
}

func (r *handleRegistry) remove(handle *Handle) {
	r.mu.Lock()
	if _, exists := r.handles[handle]; exists {
		delete(r.handles, handle)
		if len(r.handles) == 0 {
			close(r.empty)
		}
	}
	r.mu.Unlock()
}

func (r *handleRegistry) wait() {
	for {
		r.mu.Lock()
		if len(r.handles) == 0 {
			r.mu.Unlock()
			return
		}
		empty := r.empty
		r.mu.Unlock()
		<-empty
	}
}
