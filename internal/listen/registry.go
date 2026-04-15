package listen

import (
	"sync"

	"github.com/petereon/firstyr/internal/store"
)

// Registry fan-outs DocChange events to all active subscribers.
// Each subscriber gets a buffered channel; slow subscribers drop changes
// rather than blocking the dispatch goroutine.
type Registry struct {
	mu   sync.RWMutex
	subs map[uint64]chan store.DocChange
	next uint64
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{subs: make(map[uint64]chan store.DocChange)}
}

// Subscribe returns a channel that receives all future DocChange events
// and a cancel function. Call cancel when the subscriber no longer needs events.
// The channel is buffered (capacity 64) to avoid blocking Dispatch.
func (r *Registry) Subscribe() (<-chan store.DocChange, func()) {
	ch := make(chan store.DocChange, 64)
	r.mu.Lock()
	id := r.next
	r.next++
	r.subs[id] = ch
	r.mu.Unlock()

	cancel := func() {
		r.mu.Lock()
		delete(r.subs, id)
		r.mu.Unlock()
		// Drain and close to unblock any waiting receiver.
		close(ch)
		for range ch {
		}
	}
	return ch, cancel
}

// Dispatch sends c to all current subscribers. Non-blocking: if a subscriber's
// buffer is full the change is silently dropped for that subscriber.
func (r *Registry) Dispatch(c store.DocChange) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ch := range r.subs {
		select {
		case ch <- c:
		default: // subscriber is slow; drop rather than block
		}
	}
}
