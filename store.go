package seqdelay

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// store is a Redis-backed persistence layer for delay queue tasks.
type store struct {
	client redis.UniversalClient
}

func newStore(client redis.UniversalClient) *store {
	return &store{client: client}
}

// encode serialises a Task to msgpack bytes.
func encode(t *Task) ([]byte, error) {
	return msgpack.Marshal(t)
}

// decode deserialises msgpack bytes into a Task.
func decode(b []byte) (*Task, error) {
	var t Task
	if err := msgpack.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// ---------------------------------------------------------------------------
// Lua scripts
// ---------------------------------------------------------------------------

// luaSave atomically saves a new task only if it does not already exist.
//
// KEYS[1] = taskKey    KEYS[2] = indexKey
// ARGV[1] = encoded task bytes    ARGV[2] = TTL milliseconds    ARGV[3] = task ID
var luaSave = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
    return -1
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SADD', KEYS[2], ARGV[3])
return 1
`)

// luaFinish atomically transitions a task from active to finished using an
// optimistic-lock compare-and-swap.  The caller reads the current raw bytes,
// verifies the state is StateActive, re-encodes with StateFinished, then
// passes both old and new bytes to this script.
//
// KEYS[1] = taskKey    KEYS[2] = indexKey
// ARGV[1] = expected raw bytes (old)    ARGV[2] = new encoded bytes
// ARGV[3] = new TTL milliseconds (60000)    ARGV[4] = task ID
var luaFinish = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return -1 end
if raw ~= ARGV[1] then return -2 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
redis.call('SREM', KEYS[2], ARGV[4])
return 1
`)

// luaCancel atomically cancels a task regardless of its current state.
// Same CAS pattern as luaFinish.
//
// KEYS[1] = taskKey    KEYS[2] = indexKey
// ARGV[1] = expected raw bytes (old)    ARGV[2] = new encoded bytes
// ARGV[3] = new TTL milliseconds (60000)    ARGV[4] = task ID
var luaCancel = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return -1 end
if raw ~= ARGV[1] then return -2 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
redis.call('SREM', KEYS[2], ARGV[4])
return 1
`)

// luaReady atomically transitions a task to the ready state and pushes its ID
// onto the ready list.  The caller resolves state logic in Go and passes the
// new encoded bytes; the script performs the CAS swap and RPUSH atomically.
//
// KEYS[1] = taskKey    KEYS[2] = readyKey
// ARGV[1] = expected raw bytes (old)    ARGV[2] = new encoded bytes
// ARGV[3] = task ID
var luaReady = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return -1 end
if raw ~= ARGV[1] then return -2 end
redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
redis.call('RPUSH', KEYS[2], ARGV[3])
return 1
`)

// ---------------------------------------------------------------------------
// SaveTask
// ---------------------------------------------------------------------------

// SaveTask persists a new task to Redis.  TTL = delay + TTR + 60 s buffer.
// Returns ErrDuplicateTask if a task with the same topic/ID already exists.
func (s *store) SaveTask(ctx context.Context, t *Task) error {
	raw, err := encode(t)
	if err != nil {
		return err
	}

	ttl := t.Delay + t.TTR + 60*time.Second
	ttlMs := ttl.Milliseconds()

	keys := []string{taskKey(t.Topic, t.ID), indexKey(t.Topic)}
	argv := []any{raw, ttlMs, t.ID}

	res, err := luaSave.Run(ctx, s.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}

// ---------------------------------------------------------------------------
// GetTask
// ---------------------------------------------------------------------------

// GetTask retrieves and decodes a task by topic and ID.
// Returns ErrTaskNotFound if the key does not exist or has expired.
func (s *store) GetTask(ctx context.Context, topic, id string) (*Task, error) {
	raw, err := s.client.Get(ctx, taskKey(topic, id)).Bytes()
	if err == redis.Nil {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	return decode(raw)
}

// ---------------------------------------------------------------------------
// FinishTask
// ---------------------------------------------------------------------------

// FinishTask transitions a task from StateActive to StateFinished.
// The finished task is kept for 60 s for observability then expires automatically.
// Returns ErrTaskNotFound if the task does not exist, ErrInvalidState if the
// task is not currently active.
func (s *store) FinishTask(ctx context.Context, topic, id string) error {
	key := taskKey(topic, id)

	oldRaw, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}

	t, err := decode(oldRaw)
	if err != nil {
		return err
	}
	if t.State != StateActive {
		return ErrInvalidState
	}

	t.State = StateFinished
	newRaw, err := encode(t)
	if err != nil {
		return err
	}

	keys := []string{key, indexKey(topic)}
	argv := []any{oldRaw, newRaw, int64(60000), id}

	res, err := luaFinish.Run(ctx, s.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	switch res {
	case -1:
		return ErrTaskNotFound
	case -2:
		// Another writer changed the task between our GET and the CAS — retry once.
		return s.FinishTask(ctx, topic, id)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CancelTask
// ---------------------------------------------------------------------------

// CancelTask transitions a task to StateCancelled from any state.
// The cancelled task is kept for 60 s for observability then expires automatically.
// Returns ErrTaskNotFound if the task does not exist.
func (s *store) CancelTask(ctx context.Context, topic, id string) error {
	key := taskKey(topic, id)

	oldRaw, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}

	t, err := decode(oldRaw)
	if err != nil {
		return err
	}

	t.State = StateCancelled
	newRaw, err := encode(t)
	if err != nil {
		return err
	}

	keys := []string{key, indexKey(topic)}
	argv := []any{oldRaw, newRaw, int64(60000), id}

	res, err := luaCancel.Run(ctx, s.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	switch res {
	case -1:
		return ErrTaskNotFound
	case -2:
		// CAS miss — retry once.
		return s.CancelTask(ctx, topic, id)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ReadyTask
// ---------------------------------------------------------------------------

// ReadyTask transitions a task to StateReady and pushes its ID onto the topic's
// ready list.  Allowed source states are StateDelayed and StateActive.
//
// When the source state is StateActive (redelivery), Retries is incremented.
// If MaxRetries > 0 and Retries >= MaxRetries the task is finished instead and
// ErrInvalidState is returned so the caller knows not to expect the task to be
// re-processed.
func (s *store) ReadyTask(ctx context.Context, topic, id string) error {
	key := taskKey(topic, id)

	oldRaw, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}

	t, err := decode(oldRaw)
	if err != nil {
		return err
	}

	switch t.State {
	case StateDelayed:
		// First delivery — transition straight to ready, no retry increment.
		t.State = StateReady

	case StateActive:
		// Redelivery path.
		t.Retries++
		if t.MaxRetries > 0 && t.Retries >= t.MaxRetries {
			// Exhausted retries — finish the task and signal the caller.
			t.State = StateFinished
			newRaw, encErr := encode(t)
			if encErr != nil {
				return encErr
			}
			keys := []string{key, indexKey(topic)}
			argv := []any{oldRaw, newRaw, int64(60000), id}
			res, luaErr := luaFinish.Run(ctx, s.client, keys, argv...).Int()
			if luaErr != nil {
				return luaErr
			}
			if res == -2 {
				return s.ReadyTask(ctx, topic, id) // CAS miss — retry
			}
			return ErrInvalidState
		}
		t.State = StateReady

	default:
		return ErrInvalidState
	}

	newRaw, err := encode(t)
	if err != nil {
		return err
	}

	keys := []string{key, readyKey(topic)}
	argv := []any{oldRaw, newRaw, id}

	res, err := luaReady.Run(ctx, s.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	switch res {
	case -1:
		return ErrTaskNotFound
	case -2:
		return s.ReadyTask(ctx, topic, id) // CAS miss — retry
	}
	return nil
}

// ---------------------------------------------------------------------------
// PopTask
// ---------------------------------------------------------------------------

// PopTask blocks on the topic's ready list until a task ID arrives or timeout
// elapses.  On success the task state is updated to StateActive and ActiveAt is
// set to the current time.  The TTL is preserved via KEEPTTL.
// Returns ErrTaskNotFound if the task disappears between BLPOP and GET (race).
func (s *store) PopTask(ctx context.Context, topic string, timeout time.Duration) (*Task, error) {
	result, err := s.client.BLPop(ctx, timeout, readyKey(topic)).Result()
	if err == redis.Nil {
		// Timeout elapsed with no item.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// BLPOP returns [key, value]; value is the task ID.
	id := result[1]

	key := taskKey(topic, id)

	oldRaw, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}

	t, err := decode(oldRaw)
	if err != nil {
		return nil, err
	}

	t.State = StateActive
	t.ActiveAt = time.Now()

	newRaw, err := encode(t)
	if err != nil {
		return nil, err
	}

	// Use SET … KEEPTTL so the original expiry is preserved.
	if err := s.client.Set(ctx, key, newRaw, redis.KeepTTL).Err(); err != nil {
		return nil, err
	}

	return t, nil
}

// ---------------------------------------------------------------------------
// LoadTopicTasks
// ---------------------------------------------------------------------------

// LoadTopicTasks returns all live tasks that belong to the given topic by
// reading the index set and fetching each task individually.  Tasks that have
// already expired (key missing) are silently skipped and cleaned up from the
// index.
func (s *store) LoadTopicTasks(ctx context.Context, topic string) ([]*Task, error) {
	ids, err := s.client.SMembers(ctx, indexKey(topic)).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*Task, 0, len(ids))
	for _, id := range ids {
		raw, err := s.client.Get(ctx, taskKey(topic, id)).Bytes()
		if err == redis.Nil {
			// Task has expired; remove the stale index entry.
			_ = s.CleanupIndex(ctx, topic, id)
			continue
		}
		if err != nil {
			return nil, err
		}
		t, err := decode(raw)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// ---------------------------------------------------------------------------
// ListTopics
// ---------------------------------------------------------------------------

// ListTopics scans for all index keys matching the pattern `seqdelay:*:index`
// and extracts the topic name from each key.
func (s *store) ListTopics(ctx context.Context) ([]string, error) {
	var topics []string
	var cursor uint64

	for {
		keys, next, err := s.client.Scan(ctx, cursor, "seqdelay:*:index", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			// Key format: seqdelay:{topic}:index
			// Strip the hash tag braces before returning.
			topic := extractTopic(k)
			if topic != "" {
				topics = append(topics, topic)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return topics, nil
}

// extractTopic parses a topic name out of an index key.
// Input:  "seqdelay:{orders}:index"
// Output: "orders"
func extractTopic(key string) string {
	// key = "seqdelay:{<topic>}:index"
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if start == -1 || end == -1 || end <= start+1 {
		return ""
	}
	return key[start+1 : end]
}

// ---------------------------------------------------------------------------
// CleanupIndex
// ---------------------------------------------------------------------------

// CleanupIndex removes a task ID from the topic's index set.  It is used to
// clean up stale entries when a task key has already expired.
func (s *store) CleanupIndex(ctx context.Context, topic, id string) error {
	return s.client.SRem(ctx, indexKey(topic), id).Err()
}
