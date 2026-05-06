package seqdelay

import "errors"

var (
	ErrDuplicateTask = errors.New("seqdelay: task already exists")
	ErrTaskNotFound  = errors.New("seqdelay: task not found")
	ErrInvalidState  = errors.New("seqdelay: invalid task state for this operation")
	ErrTopicConflict = errors.New("seqdelay: topic already has a callback registered")
	ErrTooManyTopics = errors.New("seqdelay: max topic count exceeded")
	ErrClosed        = errors.New("seqdelay: queue is closed")
	ErrRedisRequired = errors.New("seqdelay: redis connection is required")
	ErrInvalidTask   = errors.New("seqdelay: task ID and Topic are required")
	ErrInvalidDelay  = errors.New("seqdelay: delay must be positive")
)
