package seqdelay

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gocronx/seqflow"
)

// slotList holds all entries scheduled to fire in a particular wheel slot.
type slotList struct {
	entries []*entry
}

// entry represents a single scheduled task inside the wheel.
type entry struct {
	rounds int    // full laps remaining before the task fires
	taskID string
	topic  string
}

// addCmd is the message sent from Add() to the handler goroutine.
//
// Carries the absolute target tick sequence rather than a precomputed
// (pos, rounds) pair. If the producer were to precompute slot coordinates
// using cursor.Load() and then get descheduled before writing to addCh,
// the handler could drain the cmd long after cursor had advanced past
// that slot, leaving the entry stranded for a full wheel cycle. With
// targetSeq, the handler resolves the slot at drain time against the
// up-to-date cursor and fires immediately if the deadline has already
// passed.
type addCmd struct {
	targetSeq int64
	taskID    string
	topic     string
}

// cancelCmd is the message sent from Cancel() to the handler goroutine.
type cancelCmd struct {
	taskID string
	topic  string
}

// timeWheel is a hashed timing wheel driven by a seqflow Disruptor.
//
// Ownership model:
//   - slots and cancelled are exclusively owned by the handler goroutine —
//     no locking needed on those fields.
//   - addCh / cancelCh are the only cross-goroutine communication paths.
type timeWheel struct {
	slots        []slotList
	capacity     uint32
	mask         uint32
	tickInterval time.Duration
	cursor       atomic.Int64 // current tick sequence, written by handler, read by add()

	addCh    chan addCmd
	cancelCh chan cancelCmd
	onFire   func(taskID, topic string)

	// cancelled is a lazy-cancel set: taskIDs added here are skipped on fire.
	// Exclusively accessed by the handler goroutine.
	cancelled map[string]struct{}

	disruptor *seqflow.Disruptor[int64]

	// stopTickerCh signals the ticker goroutine to stop.
	stopTickerCh chan struct{}
	// tickerDone is closed when the ticker goroutine exits.
	tickerDone chan struct{}
}

// newTimeWheel creates a time wheel with the given slot capacity and tick
// interval. capacity must be a power of two (the seqflow Disruptor enforces
// this). onFire is called by the handler goroutine when a task's delay
// expires.
func newTimeWheel(capacity uint32, tickInterval time.Duration, onFire func(taskID, topic string)) *timeWheel {
	w := &timeWheel{
		slots:        make([]slotList, capacity),
		capacity:     capacity,
		mask:         capacity - 1,
		tickInterval: tickInterval,
		addCh:        make(chan addCmd, 4096),
		cancelCh:     make(chan cancelCmd, 4096),
		onFire:       onFire,
		cancelled:    make(map[string]struct{}),
		stopTickerCh: make(chan struct{}),
		tickerDone:   make(chan struct{}),
	}
	w.cursor.Store(-1) // will be incremented to 0 on first tick

	handler := &wheelHandler{wheel: w}
	d, err := seqflow.New[int64](
		seqflow.WithCapacity(capacity),
		seqflow.WithHandler("tick", handler),
	)
	if err != nil {
		// capacity is always a power-of-two constant; panic here is appropriate.
		panic("seqdelay: failed to create seqflow disruptor: " + err.Error())
	}
	w.disruptor = d
	return w
}

// start launches the ticker goroutine and the seqflow listener goroutine.
// It must be called exactly once.
func (w *timeWheel) start() {
	// Listener goroutine: runs seqflow's consumer loop.
	go w.disruptor.Listen()

	// Ticker goroutine: publishes one tick event per interval.
	go func() {
		defer close(w.tickerDone)
		ticker := time.NewTicker(w.tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				upper, err := w.disruptor.Reserve(1)
				if err != nil {
					return
				}
				w.disruptor.Commit(upper, upper)
			case <-w.stopTickerCh:
				return
			}
		}
	}()
}

// stop signals the ticker to stop and waits for all already-committed ticks
// to be processed, bounded by ctx.
func (w *timeWheel) stop(ctx context.Context) error {
	close(w.stopTickerCh)
	<-w.tickerDone // wait for ticker goroutine to exit before draining
	return w.disruptor.Drain(ctx)
}

// add schedules a task to fire after the given delay.
func (w *timeWheel) add(delay time.Duration, taskID, topic string) {
	ticks := int64(delay / w.tickInterval)
	if ticks <= 0 {
		ticks = 1
	}
	w.addCh <- addCmd{
		targetSeq: w.cursor.Load() + ticks,
		taskID:    taskID,
		topic:     topic,
	}
}

// placeAdd resolves an addCmd against the tick currently being processed and
// either fires immediately (deadline already passed) or appends an entry to
// the appropriate slot. Used by both the pre- and post-slot drain loops.
func (w *timeWheel) placeAdd(cmd addCmd, seq int64) {
	if _, isCancelled := w.cancelled[cmd.taskID]; isCancelled {
		delete(w.cancelled, cmd.taskID)
		return
	}
	if cmd.targetSeq <= seq {
		w.onFire(cmd.taskID, cmd.topic)
		return
	}
	pos := int(cmd.targetSeq & int64(w.mask))
	rounds := int((cmd.targetSeq - seq - 1) / int64(w.capacity))
	w.slots[pos].entries = append(w.slots[pos].entries, &entry{
		rounds: rounds,
		taskID: cmd.taskID,
		topic:  cmd.topic,
	})
}

// cancel lazily marks a task so it is skipped when its slot fires.
func (w *timeWheel) cancel(taskID, topic string) {
	w.cancelCh <- cancelCmd{taskID: taskID, topic: topic}
}

// wheelHandler implements seqflow.Handler and exclusively owns the wheel's
// slot data.
type wheelHandler struct {
	wheel *timeWheel
}

// Handle is called by seqflow for each batch of committed tick sequences.
// For every tick in [lower, upper] it:
//  1. Drains pending addCmds (non-blocking).
//  2. Drains pending cancelCmds into the lazy-cancel set (non-blocking).
//  3. Advances the wheel cursor and fires any matured entries.
func (h *wheelHandler) Handle(lower, upper int64) {
	for seq := lower; seq <= upper; seq++ {
		w := h.wheel

		// --- 1. Drain pending add commands (non-blocking) ---
	drainAdds:
		for {
			select {
			case cmd := <-w.addCh:
				w.placeAdd(cmd, seq)
			default:
				break drainAdds
			}
		}

		// --- 2. Drain pending cancel commands into lazy-cancel set ---
	drainCancels:
		for {
			select {
			case cmd := <-w.cancelCh:
				w.cancelled[cmd.taskID] = struct{}{}
			default:
				break drainCancels
			}
		}

		// --- 3. Advance cursor and process current slot ---
		w.cursor.Store(seq)
		idx := int(seq) & int(w.mask)
		slot := &w.slots[idx]

		// Compact in-place: re-queue entries that still have rounds left,
		// fire those that are due, and silently drop cancelled ones.
		remaining := slot.entries[:0]
		for _, e := range slot.entries {
			if _, isCancelled := w.cancelled[e.taskID]; isCancelled {
				delete(w.cancelled, e.taskID)
				continue
			}
			if e.rounds > 0 {
				e.rounds--
				remaining = append(remaining, e)
			} else {
				w.onFire(e.taskID, e.topic)
			}
		}
		slot.entries = remaining

		// --- 4. Post-drain: catch tasks added during slot processing.
		// placeAdd handles same-slot/rounds==0 (fire), past-deadline (fire),
		// and future targets (slot append) uniformly.
	postDrain:
		for {
			select {
			case cmd := <-w.addCh:
				w.placeAdd(cmd, seq)
			default:
				break postDrain
			}
		}
	}
}
