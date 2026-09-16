package EasyRoutine

import "sync"

type handleRegistry struct {
	mu      sync.Mutex
	empty   chan struct{}
	handles map[*Handle]struct{}
}

var activeHandles = newHandleRegistry()

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

// Wait blocks until all successfully started SafeGo and StartUniqueSupervisor
// operations have completed. Operations started before Wait observes no active
// handles are included, including operations started while Wait is blocked.
// Wait does not stop or cancel managed work.
func Wait() {
	activeHandles.wait()
}
