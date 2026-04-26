package listen

import (
	"sync"
	"sync/atomic"

	"github.com/petereon/firstyr/internal/store"
)

// subscription is a single registered consumer plus a flag the registry sets
// when a Dispatch had to drop a change because the subscriber's buffer was full.
type subscription struct {
	ch         chan store.DocChange
	overflowed atomic.Bool
}

// Registry fan-outs DocChange events to all active subscribers.
// Each subscriber gets a buffered channel; slow subscribers drop changes
// rather than blocking the dispatch goroutine, and have their overflow flag
// raised so the Listen handler can recover via a RESET to the client.
type Registry struct {
	mu   sync.RWMutex
	subs map[uint64]*subscription
	next uint64
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{subs: make(map[uint64]*subscription)}
}

// Subscribe returns a channel that receives all future DocChange events
// and a cancel function. Call cancel when the subscriber no longer needs events.
// The channel is buffered (capacity 64) to avoid blocking Dispatch. When the
// buffer fills the change is dropped for this subscriber and the overflow flag
// (visible via SubscribeWithSignal) is raised.
func (r *Registry) Subscribe() (<-chan store.DocChange, func()) {
	sub := r.SubscribeWithSignal()
	return sub.Ch(), sub.Cancel
}

// Subscription is a handle returned by SubscribeWithSignal that exposes both
// the channel of changes and the overflow flag.
type Subscription struct {
	sub    *subscription
	cancel func()
}

// Ch returns the channel of changes.
func (s *Subscription) Ch() <-chan store.DocChange { return s.sub.ch }

// Overflowed reports whether at least one Dispatch dropped a change for this
// subscriber since the last call to Overflowed. The flag is cleared atomically
// when read so callers can detect each new burst of drops independently.
func (s *Subscription) Overflowed() bool { return s.sub.overflowed.Swap(false) }

// Cancel removes this subscription from the registry and closes the channel.
func (s *Subscription) Cancel() { s.cancel() }

// SubscribeWithSignal is the typed variant of Subscribe that exposes the
// overflow signal. Use this when the consumer needs to recover from drops.
func (r *Registry) SubscribeWithSignal() *Subscription {
	s := &subscription{ch: make(chan store.DocChange, 64)}
	r.mu.Lock()
	id := r.next
	r.next++
	r.subs[id] = s
	r.mu.Unlock()

	cancel := func() {
		r.mu.Lock()
		delete(r.subs, id)
		r.mu.Unlock()
		close(s.ch)
	}
	return &Subscription{sub: s, cancel: cancel}
}

// Dispatch sends c to all current subscribers. Non-blocking: if a subscriber's
// buffer is full the change is dropped for that subscriber and its overflow
// flag is raised so the consumer can detect the data loss and recover.
func (r *Registry) Dispatch(c store.DocChange) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.subs {
		select {
		case s.ch <- c:
		default:
			s.overflowed.Store(true)
		}
	}
}
