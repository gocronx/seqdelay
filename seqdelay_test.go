package seqdelay

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

// TestQueue_New_MissingRedis verifies that New returns ErrRedisRequired when
// no Redis client option is provided.
func TestQueue_New_MissingRedis(t *testing.T) {
	_, err := New()
	if err != ErrRedisRequired {
		t.Errorf("expected ErrRedisRequired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Add + Get roundtrip
// ---------------------------------------------------------------------------

func TestQueue_AddAndGet(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(WithRedisClient(client), WithTickInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	task := &Task{
		ID:    "ag-1",
		Topic: "test-add-get",
		Body:  []byte("payload"),
		Delay: 5 * time.Second,
		TTR:   30 * time.Second,
	}

	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := q.Get(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != task.ID {
		t.Errorf("ID mismatch: got %s, want %s", got.ID, task.ID)
	}
	if got.State != StateDelayed {
		t.Errorf("expected StateDelayed, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// Add duplicate
// ---------------------------------------------------------------------------

func TestQueue_Add_Duplicate(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(WithRedisClient(client), WithTickInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	task := &Task{
		ID:    "dup-1",
		Topic: "test-dup",
		Body:  []byte("x"),
		Delay: 5 * time.Second,
		TTR:   30 * time.Second,
	}

	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	if err := q.Add(ctx, task); err != ErrDuplicateTask {
		t.Errorf("expected ErrDuplicateTask, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Add + Pop + Finish lifecycle
// ---------------------------------------------------------------------------

func TestQueue_AddPopFinish(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(
		WithRedisClient(client),
		WithTickInterval(5*time.Millisecond),
		WithPopTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	// Acquire the lock so our instance acts as leader and fires ReadyTask.
	q.lock.TryAcquire(ctx)

	task := &Task{
		ID:    "lifecycle-1",
		Topic: "test-lifecycle",
		Body:  []byte("data"),
		Delay: 50 * time.Millisecond,
		TTR:   30 * time.Second,
	}

	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Wait for the timing wheel to fire and move the task to ready.
	time.Sleep(200 * time.Millisecond)

	popped, err := q.Pop(ctx, task.Topic)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if popped == nil {
		t.Fatal("Pop returned nil — task did not become ready in time")
	}
	if popped.State != StateActive {
		t.Errorf("expected StateActive, got %v", popped.State)
	}

	if err := q.Finish(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	got, err := q.Get(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("Get after Finish: %v", err)
	}
	if got.State != StateFinished {
		t.Errorf("expected StateFinished, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// Cancel delayed task
// ---------------------------------------------------------------------------

func TestQueue_Cancel_Delayed(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(WithRedisClient(client), WithTickInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	task := &Task{
		ID:    "cancel-1",
		Topic: "test-cancel",
		Body:  []byte("bye"),
		Delay: 10 * time.Second,
		TTR:   30 * time.Second,
	}

	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := q.Cancel(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	got, err := q.Get(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != StateCancelled {
		t.Errorf("expected StateCancelled, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// OnExpire callback fires
// ---------------------------------------------------------------------------

func TestQueue_OnExpire_CallbackFires(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(
		WithRedisClient(client),
		WithTickInterval(5*time.Millisecond),
		WithPopTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	// Acquire the leader lock so this instance fires ReadyTask.
	q.lock.TryAcquire(ctx)

	fired := make(chan *Task, 1)
	if err := q.OnExpire("test-expire", func(_ context.Context, t *Task) error {
		fired <- t
		return nil
	}); err != nil {
		t.Fatalf("OnExpire: %v", err)
	}

	task := &Task{
		ID:    "expire-1",
		Topic: "test-expire",
		Body:  []byte("fire me"),
		Delay: 50 * time.Millisecond,
		TTR:   30 * time.Second,
	}
	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("Add: %v", err)
	}

	select {
	case got := <-fired:
		if got.ID != task.ID {
			t.Errorf("callback received ID %s, want %s", got.ID, task.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for OnExpire callback")
	}

	// After the callback returns nil the queue auto-finishes the task.
	time.Sleep(50 * time.Millisecond)
	got, err := q.Get(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("Get after callback: %v", err)
	}
	if got.State != StateFinished {
		t.Errorf("expected StateFinished after successful callback, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// Get returns correct state at each stage
// ---------------------------------------------------------------------------

func TestQueue_Get_StatesProgression(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(
		WithRedisClient(client),
		WithTickInterval(5*time.Millisecond),
		WithPopTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Shutdown(ctx) //nolint:errcheck

	q.lock.TryAcquire(ctx)

	task := &Task{
		ID:    "states-1",
		Topic: "test-states",
		Body:  []byte("watch me"),
		Delay: 50 * time.Millisecond,
		TTR:   30 * time.Second,
	}

	// State: Delayed
	if err := q.Add(ctx, task); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, _ := q.Get(ctx, task.Topic, task.ID)
	if got.State != StateDelayed {
		t.Errorf("after Add: expected StateDelayed, got %v", got.State)
	}

	// Wait for task to become ready, then pop it.
	time.Sleep(200 * time.Millisecond)

	popped, err := q.Pop(ctx, task.Topic)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if popped == nil {
		t.Fatal("Pop returned nil")
	}

	// State: Active
	got, _ = q.Get(ctx, task.Topic, task.ID)
	if got.State != StateActive {
		t.Errorf("after Pop: expected StateActive, got %v", got.State)
	}

	// State: Finished
	if err := q.Finish(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got, _ = q.Get(ctx, task.Topic, task.ID)
	if got.State != StateFinished {
		t.Errorf("after Finish: expected StateFinished, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// ErrTopicConflict on duplicate OnExpire registration
// ---------------------------------------------------------------------------

func TestQueue_OnExpire_Conflict(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	q, err := New(WithRedisClient(client))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Shutdown(context.Background()) //nolint:errcheck

	noop := func(_ context.Context, _ *Task) error { return nil }

	if err := q.OnExpire("conflict-topic", noop); err != nil {
		t.Fatalf("first OnExpire: %v", err)
	}
	if err := q.OnExpire("conflict-topic", noop); err != ErrTopicConflict {
		t.Errorf("expected ErrTopicConflict, got %v", err)
	}
}
