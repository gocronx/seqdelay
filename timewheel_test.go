package seqdelay

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// tickInterval used across all tests. Large enough to be reliable on a loaded
// CI machine, small enough to keep the suite fast.
const testTick = 5 * time.Millisecond

// newTestWheel is a helper that builds a wheel with 64 slots and testTick
// granularity. The returned stop function must be deferred.
func newTestWheel(t *testing.T, onFire func(taskID, topic string)) (*timeWheel, func()) {
	t.Helper()
	tw := newTimeWheel(64, testTick, onFire)
	tw.start()
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tw.stop(ctx)
	}
	return tw, stop
}

// TestTimeWheel_AddAndFire checks that a single task fires after its delay.
func TestTimeWheel_AddAndFire(t *testing.T) {
	fired := make(chan string, 10)
	tw, stop := newTestWheel(t, func(id, _ string) { fired <- id })
	defer stop()

	tw.add(3*testTick, "task-1", "test")

	select {
	case id := <-fired:
		if id != "task-1" {
			t.Errorf("expected task-1, got %s", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: task-1 never fired")
	}
}

// TestTimeWheel_RoundsGreaterThanZero verifies that tasks with a delay longer
// than one full wheel revolution (capacity * tick) are handled correctly via
// the rounds countdown.
func TestTimeWheel_RoundsGreaterThanZero(t *testing.T) {
	const capacity = 64

	fired := make(chan string, 10)
	tw := newTimeWheel(capacity, testTick, func(id, _ string) { fired <- id })
	tw.start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tw.stop(ctx)
	}()

	// Delay of 2 full revolutions + a few extra ticks.
	// rounds = floor(ticks / capacity) = floor((2*64 + 10) / 64) = 2
	delay := time.Duration(2*capacity+10) * testTick
	tw.add(delay, "long-task", "topic")

	// Must NOT fire within one revolution.
	select {
	case id := <-fired:
		t.Errorf("task fired too early: %s", id)
	case <-time.After(time.Duration(capacity-5) * testTick):
		// Good: still waiting.
	}

	// Must fire within a generous window after the full delay.
	select {
	case id := <-fired:
		if id != "long-task" {
			t.Errorf("expected long-task, got %s", id)
		}
	case <-time.After(delay + 2*time.Second):
		t.Fatal("timeout: long-task never fired")
	}
}

// TestTimeWheel_CancelBeforeFire confirms that a cancelled task does not fire.
func TestTimeWheel_CancelBeforeFire(t *testing.T) {
	fired := make(chan string, 10)
	tw, stop := newTestWheel(t, func(id, _ string) { fired <- id })
	defer stop()

	tw.add(20*testTick, "cancel-me", "topic")
	// Cancel quickly, well before the task's slot is processed.
	tw.cancel("cancel-me", "topic")

	select {
	case id := <-fired:
		t.Errorf("cancelled task fired: %s", id)
	case <-time.After(30 * testTick * 3):
		// Good: task was suppressed.
	}
}

// TestTimeWheel_MultipleTasksSameSlot ensures all tasks landing in the same
// slot all fire.
func TestTimeWheel_MultipleTasksSameSlot(t *testing.T) {
	const n = 5
	fired := make(chan string, n*2)
	tw, stop := newTestWheel(t, func(id, _ string) { fired <- id })
	defer stop()

	delay := 4 * testTick
	for i := range n {
		tw.add(delay, fmt.Sprintf("task-%d", i), "batch")
	}

	got := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for range n {
		select {
		case id := <-fired:
			got[id] = true
		case <-timeout:
			t.Fatalf("timeout: only got %d/%d tasks", len(got), n)
		}
	}
	for i := range n {
		id := fmt.Sprintf("task-%d", i)
		if !got[id] {
			t.Errorf("task %s did not fire", id)
		}
	}
}

// TestTimeWheel_BatchCatchUp adds many tasks across different slots and
// verifies every one fires. This exercises the batch-handling path where
// Handle() receives lower < upper.
func TestTimeWheel_BatchCatchUp(t *testing.T) {
	const n = 20
	var mu sync.Mutex
	got := make(map[string]bool)
	done := make(chan struct{})

	tw, stop := newTestWheel(t, func(id, _ string) {
		mu.Lock()
		got[id] = true
		if len(got) == n {
			close(done)
		}
		mu.Unlock()
	})
	defer stop()

	for i := range n {
		// Spread tasks across different tick offsets to occupy multiple slots.
		delay := time.Duration(i+1) * testTick
		tw.add(delay, fmt.Sprintf("batch-%d", i), "spread")
	}

	select {
	case <-done:
		// All tasks fired.
	case <-time.After(5 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("timeout: only %d/%d tasks fired", len(got), n)
	}
}

// TestTimeWheel_OnlyFiredOnce checks that a task fires exactly once, not on
// every subsequent revolution.
func TestTimeWheel_OnlyFiredOnce(t *testing.T) {
	count := 0
	var mu sync.Mutex
	tw, stop := newTestWheel(t, func(_, _ string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	defer stop()

	tw.add(2*testTick, "once", "topic")

	// Wait for more than two full revolutions.
	time.Sleep(time.Duration(64*2+10) * testTick)

	mu.Lock()
	c := count
	mu.Unlock()
	if c != 1 {
		t.Errorf("task fired %d times, expected exactly 1", c)
	}
}

// TestTimeWheel_CancelOtherTaskUnharmed verifies that cancelling one task does
// not suppress a different task in the same slot.
func TestTimeWheel_CancelOtherTaskUnharmed(t *testing.T) {
	fired := make(chan string, 10)
	tw, stop := newTestWheel(t, func(id, _ string) { fired <- id })
	defer stop()

	delay := 5 * testTick
	tw.add(delay, "keep-me", "topic")
	tw.add(delay, "drop-me", "topic")
	tw.cancel("drop-me", "topic")

	var got []string
	timeout := time.After(3 * time.Second)
	// Collect for a short window; we expect exactly "keep-me".
	collect := time.After(delay + 10*testTick)
loop:
	for {
		select {
		case id := <-fired:
			got = append(got, id)
		case <-collect:
			break loop
		case <-timeout:
			t.Fatal("timeout waiting for keep-me")
		}
	}

	if len(got) != 1 || got[0] != "keep-me" {
		t.Errorf("fired = %v, want [keep-me]", got)
	}
}

// TestTimeWheel_PlaceAdd_PastDeadlineFiresImmediately is the regression test
// for the producer-preempted-after-cursor-load race that left tasks stranded
// for a full wheel cycle. Simulates a cmd arriving at the handler after the
// cursor has already advanced past its target sequence, and asserts the
// handler fires it immediately instead of appending to a stale slot.
func TestTimeWheel_PlaceAdd_PastDeadlineFiresImmediately(t *testing.T) {
	var fired []string
	tw := newTimeWheel(64, testTick, func(id, _ string) {
		fired = append(fired, id)
	})
	// Note: don't start the wheel — placeAdd is invoked synchronously by the
	// handler goroutine; calling it directly here is safe because no other
	// goroutine touches the wheel state.

	// targetSeq=15, but the handler has already advanced to seq=20.
	tw.placeAdd(addCmd{targetSeq: 15, taskID: "stranded", topic: "t"}, 20)

	if len(fired) != 1 || fired[0] != "stranded" {
		t.Errorf("expected immediate fire of stranded task, got %v", fired)
	}
	for _, slot := range tw.slots {
		if len(slot.entries) != 0 {
			t.Errorf("stranded cmd should not have been appended to any slot")
		}
	}
}

// TestTimeWheel_PlaceAdd_CancelledIsDropped verifies that a cmd whose taskID
// is in the lazy-cancel set is silently dropped, even when its deadline has
// already passed.
func TestTimeWheel_PlaceAdd_CancelledIsDropped(t *testing.T) {
	var fired []string
	tw := newTimeWheel(64, testTick, func(id, _ string) {
		fired = append(fired, id)
	})
	tw.cancelled["stranded"] = struct{}{}

	tw.placeAdd(addCmd{targetSeq: 15, taskID: "stranded", topic: "t"}, 20)

	if len(fired) != 0 {
		t.Errorf("cancelled task should not fire, got %v", fired)
	}
	if _, stillCancelled := tw.cancelled["stranded"]; stillCancelled {
		t.Errorf("cancelled set entry should be consumed after drop")
	}
}
