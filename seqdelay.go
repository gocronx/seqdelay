package seqdelay

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// Queue is the public entry point for the seqdelay delay-queue library.
// It wires together the Redis store, timing wheel, ready queue, and
// distributed lock into a single coherent unit.
type Queue struct {
	cfg     *config
	store   *store
	wheel   *timeWheel
	ready   *readyQueue
	lock    *distLock
	closed  atomic.Bool
	started atomic.Bool
	stopCh  chan struct{}
}

// New creates a new Queue with the given options.
// Returns ErrRedisRequired when no Redis client is configured.
func New(opts ...Option) (*Queue, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	if cfg.redisClient == nil {
		return nil, ErrRedisRequired
	}

	// Auto-generate an instanceID when the caller did not provide one.
	if cfg.instanceID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		cfg.instanceID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	s := newStore(cfg.redisClient)

	wheel := newTimeWheel(cfg.wheelCapacity, cfg.tickInterval, nil)
	ready := newReadyQueue(s, cfg.maxTopics)
	lock := newDistLock(cfg.redisClient, cfg.instanceID, cfg.lockTTL)

	q := &Queue{
		cfg:    cfg,
		store:  s,
		wheel:  wheel,
		ready:  ready,
		lock:   lock,
		stopCh: make(chan struct{}),
	}

	// Set onFire after Queue creation so it can reference q.lock.
	// The wheel doesn't tick until Start() is called.
	wheel.onFire = func(taskID, topic string) {
		if !q.lock.IsLeader(context.Background()) {
			return
		}
		_ = q.store.ReadyTask(context.Background(), topic, taskID)
	}

	return q, nil
}

// Start begins background processing: task recovery from Redis, the timing
// wheel, and the leader-election heartbeat loop.
func (q *Queue) Start(ctx context.Context) error {
	if err := q.recover(ctx); err != nil {
		return err
	}

	q.started.Store(true)
	q.wheel.start()

	// Leader-election heartbeat goroutine.
	go func() {
		ticker := time.NewTicker(q.cfg.lockTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-q.stopCh:
				q.lock.Release(ctx)
				return
			case <-ticker.C:
				if q.lock.IsLeader(ctx) {
					q.lock.Renew(ctx)
				} else {
					q.lock.TryAcquire(ctx)
				}
			}
		}
	}()

	return nil
}

// recover loads all live tasks from Redis and either re-schedules them in the
// timing wheel or immediately moves them to the ready list.
func (q *Queue) recover(ctx context.Context) error {
	topics, err := q.store.ListTopics(ctx)
	if err != nil {
		// Non-fatal: best-effort recovery.
		return nil
	}

	for _, topic := range topics {
		tasks, err := q.store.LoadTopicTasks(ctx, topic)
		if err != nil {
			continue
		}

		// Pre-fetch the ready list once so StateReady checks below are O(1).
		// Best-effort: on error we treat the set as empty, which means orphans
		// get re-enqueued (safe) and live IDs may get duplicated (harmless —
		// the duplicate just becomes a stale ID once the original is popped).
		readySet, _ := q.store.ReadyListIDs(ctx, topic)

		for _, task := range tasks {
			switch task.State {
			case StateDelayed:
				remaining := time.Until(task.CreatedAt.Add(task.Delay))
				if remaining <= 0 {
					_ = q.store.ReadyTask(ctx, topic, task.ID)
				} else {
					q.wheel.add(remaining, task.ID, task.Topic)
				}
			case StateActive:
				remaining := time.Until(task.ActiveAt.Add(task.TTR))
				if remaining <= 0 {
					_ = q.store.ReadyTask(ctx, topic, task.ID)
				} else {
					q.wheel.add(remaining, task.ID, task.Topic)
				}
			case StateReady:
				// If the ID isn't on the ready list, the previous process
				// crashed between BLPOP and the state update — re-enqueue so
				// the task isn't lost.
				if _, ok := readySet[task.ID]; !ok {
					_ = q.store.RequeueReady(ctx, topic, task.ID)
				}
			case StateFinished, StateCancelled:
				_ = q.store.CleanupIndex(ctx, topic, task.ID)
			}
		}
	}
	return nil
}

// Add validates and persists a new delayed task, then schedules it in the
// timing wheel.
func (q *Queue) Add(ctx context.Context, task *Task) error {
	if q.closed.Load() {
		return ErrClosed
	}
	if err := task.Validate(); err != nil {
		return err
	}

	task.CreatedAt = time.Now()
	task.State = StateDelayed

	if err := q.store.SaveTask(ctx, task); err != nil {
		return err
	}

	q.wheel.add(task.Delay, task.ID, task.Topic)
	return nil
}

// Pop blocks until a task becomes available on the given topic's ready list or
// the configured pop timeout elapses. On success the task is in StateActive and
// a TTR re-delivery timer is armed.
//
// Returns nil, nil when the timeout elapses with no available task.
func (q *Queue) Pop(ctx context.Context, topic string) (*Task, error) {
	if q.closed.Load() {
		return nil, ErrClosed
	}

	task, err := q.store.PopTask(ctx, topic, q.cfg.popTimeout)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, nil
	}

	// Arm TTR re-delivery: if the consumer does not call Finish/Cancel before
	// TTR expires the timing wheel will call ReadyTask again.
	q.wheel.add(task.TTR, task.ID, task.Topic)
	return task, nil
}

// Finish marks a task as completed and cancels its TTR timer.
func (q *Queue) Finish(ctx context.Context, topic, id string) error {
	if q.closed.Load() {
		return ErrClosed
	}
	if err := q.store.FinishTask(ctx, topic, id); err != nil {
		return err
	}
	q.wheel.cancel(id, topic)
	return nil
}

// Cancel transitions a task to StateCancelled and removes it from the timing
// wheel.
func (q *Queue) Cancel(ctx context.Context, topic, id string) error {
	if q.closed.Load() {
		return ErrClosed
	}
	if err := q.store.CancelTask(ctx, topic, id); err != nil {
		return err
	}
	q.wheel.cancel(id, topic)
	return nil
}

// Get retrieves the current state of a task without altering it.
func (q *Queue) Get(ctx context.Context, topic, id string) (*Task, error) {
	if q.closed.Load() {
		return nil, ErrClosed
	}
	return q.store.GetTask(ctx, topic, id)
}

// OnExpire registers fn as the callback invoked when a task on topic becomes
// ready. The drain goroutine will pop tasks and call fn automatically, finishing
// the task on success or leaving it for TTR re-delivery on failure.
func (q *Queue) OnExpire(topic string, fn func(context.Context, *Task) error) error {
	return q.ready.registerCallback(topic, fn)
}

// Shutdown gracefully stops the queue. It marks the queue closed, drains the
// timing wheel (bounded by ctx), stops all drain goroutines, and releases the
// distributed lock.
func (q *Queue) Shutdown(ctx context.Context) error {
	if !q.closed.CompareAndSwap(false, true) {
		return nil // already shut down
	}

	// Signal the leader-election goroutine to stop.
	close(q.stopCh)

	// Stop all per-topic drain goroutines first (they may trigger wheel adds).
	q.ready.stopAll()

	// Stop the timing wheel only if Start() was called.
	if q.started.Load() {
		return q.wheel.stop(ctx)
	}
	return nil
}

// instanceIDFromParts builds a deterministic instance ID from the hostname and
// PID — kept here for documentation; the actual call is inline in New.
func instanceIDFromParts(hostname string, pid int) string {
	return hostname + "-" + strconv.Itoa(pid)
}
