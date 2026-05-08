package seqdelay

import (
	"context"
	"testing"
	"time"
)

// newTestTask returns a minimal valid task for use in tests.
func newTestTask(topic, id string) *Task {
	return &Task{
		ID:        id,
		Topic:     topic,
		Body:      []byte("hello"),
		Delay:     2 * time.Second,
		TTR:       30 * time.Second,
		State:     StateDelayed,
		CreatedAt: time.Now(),
	}
}

// ---------------------------------------------------------------------------
// SaveTask + GetTask roundtrip
// ---------------------------------------------------------------------------

func TestStore_SaveAndGetTask(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "t1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	got, err := s.GetTask(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.ID != task.ID || got.Topic != task.Topic {
		t.Errorf("roundtrip mismatch: got %+v", got)
	}
	if string(got.Body) != string(task.Body) {
		t.Errorf("body mismatch: got %s, want %s", got.Body, task.Body)
	}
}

// ---------------------------------------------------------------------------
// GetTask not found
// ---------------------------------------------------------------------------

func TestStore_GetTask_NotFound(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	_, err := s.GetTask(ctx, "ghost", "no-such-id")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// SaveTask duplicate
// ---------------------------------------------------------------------------

func TestStore_SaveTask_Duplicate(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "dup1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("first SaveTask: %v", err)
	}
	err := s.SaveTask(ctx, task)
	if err != ErrDuplicateTask {
		t.Errorf("expected ErrDuplicateTask, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FinishTask from ACTIVE
// ---------------------------------------------------------------------------

func TestStore_FinishTask_FromActive(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "fin1")
	task.State = StateActive
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	if err := s.FinishTask(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("FinishTask: %v", err)
	}

	got, err := s.GetTask(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("GetTask after finish: %v", err)
	}
	if got.State != StateFinished {
		t.Errorf("expected StateFinished, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// FinishTask from non-ACTIVE (should return ErrInvalidState)
// ---------------------------------------------------------------------------

func TestStore_FinishTask_FromNonActive(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "fin2")
	task.State = StateDelayed
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	err := s.FinishTask(ctx, task.Topic, task.ID)
	if err != ErrInvalidState {
		t.Errorf("expected ErrInvalidState, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CancelTask (from any state)
// ---------------------------------------------------------------------------

func TestStore_CancelTask(t *testing.T) {
	states := []TaskState{StateDelayed, StateReady, StateActive}
	for _, state := range states {
		state := state
		t.Run(state.String(), func(t *testing.T) {
			client := getTestRedis(t)
			s := newStore(client)
			ctx := context.Background()

			task := newTestTask("orders", "cancel-"+state.String())
			task.State = state
			if err := s.SaveTask(ctx, task); err != nil {
				t.Fatalf("SaveTask: %v", err)
			}

			if err := s.CancelTask(ctx, task.Topic, task.ID); err != nil {
				t.Fatalf("CancelTask: %v", err)
			}

			got, err := s.GetTask(ctx, task.Topic, task.ID)
			if err != nil {
				t.Fatalf("GetTask after cancel: %v", err)
			}
			if got.State != StateCancelled {
				t.Errorf("expected StateCancelled, got %v", got.State)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ReadyTask from StateDelayed
// ---------------------------------------------------------------------------

func TestStore_ReadyTask_FromDelayed(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "ready1")
	task.State = StateDelayed
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	if err := s.ReadyTask(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("ReadyTask: %v", err)
	}

	got, err := s.GetTask(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("GetTask after ready: %v", err)
	}
	if got.State != StateReady {
		t.Errorf("expected StateReady, got %v", got.State)
	}

	// Verify ID was pushed onto the ready list.
	items, err := client.LRange(ctx, readyKey(task.Topic), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(items) != 1 || items[0] != task.ID {
		t.Errorf("ready list = %v, want [%s]", items, task.ID)
	}
}

// ---------------------------------------------------------------------------
// ReadyTask from StateActive (redelivery, increments Retries)
// ---------------------------------------------------------------------------

func TestStore_ReadyTask_Redelivery(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "redeliver1")
	task.State = StateActive
	task.MaxRetries = 3
	task.Retries = 0
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	if err := s.ReadyTask(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("ReadyTask: %v", err)
	}

	got, err := s.GetTask(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.State != StateReady {
		t.Errorf("expected StateReady, got %v", got.State)
	}
	if got.Retries != 1 {
		t.Errorf("expected Retries=1, got %d", got.Retries)
	}
}

// ---------------------------------------------------------------------------
// ReadyTask exhausts MaxRetries → StateFinished + ErrInvalidState
// ---------------------------------------------------------------------------

func TestStore_ReadyTask_MaxRetriesExhausted(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "exhaust1")
	task.State = StateActive
	task.MaxRetries = 2
	task.Retries = 1 // one retry already consumed; next will hit the limit
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	err := s.ReadyTask(ctx, task.Topic, task.ID)
	if err != ErrInvalidState {
		t.Errorf("expected ErrInvalidState, got %v", err)
	}

	got, err := s.GetTask(ctx, task.Topic, task.ID)
	if err != nil {
		t.Fatalf("GetTask after exhaustion: %v", err)
	}
	if got.State != StateFinished {
		t.Errorf("expected StateFinished, got %v", got.State)
	}
}

// ---------------------------------------------------------------------------
// ReadyTask from invalid state
// ---------------------------------------------------------------------------

func TestStore_ReadyTask_InvalidState(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "ready-invalid")
	task.State = StateCancelled
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	err := s.ReadyTask(ctx, task.Topic, task.ID)
	if err != ErrInvalidState {
		t.Errorf("expected ErrInvalidState, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// PopTask
// ---------------------------------------------------------------------------

func TestStore_PopTask(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	task := newTestTask("orders", "pop1")
	task.State = StateDelayed
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	if err := s.ReadyTask(ctx, task.Topic, task.ID); err != nil {
		t.Fatalf("ReadyTask: %v", err)
	}

	got, err := s.PopTask(ctx, task.Topic, 2*time.Second)
	if err != nil {
		t.Fatalf("PopTask: %v", err)
	}
	if got == nil {
		t.Fatal("PopTask returned nil task")
	}
	if got.ID != task.ID {
		t.Errorf("PopTask ID = %s, want %s", got.ID, task.ID)
	}
	if got.State != StateActive {
		t.Errorf("expected StateActive, got %v", got.State)
	}
	if got.ActiveAt.IsZero() {
		t.Error("ActiveAt not set after PopTask")
	}
}

// PopTask timeout — should return nil, nil when list is empty.
func TestStore_PopTask_Timeout(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	got, err := s.PopTask(ctx, "empty-topic", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("PopTask: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil task on timeout, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// LoadTopicTasks
// ---------------------------------------------------------------------------

func TestStore_LoadTopicTasks(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	topic := "load-topic"
	ids := []string{"lt1", "lt2", "lt3"}
	for _, id := range ids {
		task := newTestTask(topic, id)
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatalf("SaveTask %s: %v", id, err)
		}
	}

	tasks, err := s.LoadTopicTasks(ctx, topic)
	if err != nil {
		t.Fatalf("LoadTopicTasks: %v", err)
	}
	if len(tasks) != len(ids) {
		t.Errorf("expected %d tasks, got %d", len(ids), len(tasks))
	}
}

// ---------------------------------------------------------------------------
// ListTopics
// ---------------------------------------------------------------------------

func TestStore_ListTopics(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	topics := []string{"alpha", "beta", "gamma"}
	for _, topic := range topics {
		task := newTestTask(topic, "id-"+topic)
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatalf("SaveTask topic=%s: %v", topic, err)
		}
	}

	got, err := s.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}

	seen := make(map[string]bool, len(got))
	for _, tp := range got {
		seen[tp] = true
	}
	for _, tp := range topics {
		if !seen[tp] {
			t.Errorf("topic %q missing from ListTopics result %v", tp, got)
		}
	}
}

// ---------------------------------------------------------------------------
// CleanupIndex
// ---------------------------------------------------------------------------

func TestStore_CleanupIndex(t *testing.T) {
	client := getTestRedis(t)
	s := newStore(client)
	ctx := context.Background()

	topic := "cleanup"
	task := newTestTask(topic, "ci1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	// Verify it is in the index.
	members, _ := client.SMembers(ctx, indexKey(topic)).Result()
	if len(members) != 1 {
		t.Fatalf("expected 1 index member, got %d", len(members))
	}

	if err := s.CleanupIndex(ctx, topic, task.ID); err != nil {
		t.Fatalf("CleanupIndex: %v", err)
	}

	members, _ = client.SMembers(ctx, indexKey(topic)).Result()
	if len(members) != 0 {
		t.Errorf("expected 0 index members after cleanup, got %d", len(members))
	}
}
