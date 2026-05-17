package seqdelay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server exposes the Queue over HTTP using a plain net/http ServeMux.
// All request/response bodies are JSON.
type Server struct {
	queue  *Queue
	addr   string
	server *http.Server
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithServerAddr sets the TCP listen address (default ":8080").
func WithServerAddr(addr string) ServerOption {
	return func(s *Server) { s.addr = addr }
}

// NewServer creates a Server that wraps the given Queue.
func NewServer(q *Queue, opts ...ServerOption) *Server {
	s := &Server{
		queue: q,
		addr:  ":8080",
	}
	for _, o := range opts {
		o(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /add", s.handleAdd)
	mux.HandleFunc("POST /pop", s.handlePop)
	mux.HandleFunc("POST /finish", s.handleFinish)
	mux.HandleFunc("POST /cancel", s.handleCancel)
	mux.HandleFunc("GET /get", s.handleGet)
	mux.HandleFunc("GET /stats", s.handleStats)

	// Admin / management endpoints (used by the seqdelay-admin web console).
	mux.HandleFunc("GET /topics", s.handleListTopics)
	mux.HandleFunc("GET /tasks", s.handleListTasks)

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}
	return s
}

// ListenAndServe starts the HTTP server. It blocks until the server stops.
func (s *Server) ListenAndServe() error {
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

type apiResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
}

type addRequest struct {
	Topic      string `json:"topic"`
	ID         string `json:"id"`
	Body       string `json:"body"`
	DelayMS    int64  `json:"delay_ms"`
	TTRMS      int64  `json:"ttr_ms"`
	MaxRetries int    `json:"max_retries"`
}

type popRequest struct {
	Topic     string `json:"topic"`
	TimeoutMS int64  `json:"timeout_ms"`
}

type topicIDRequest struct {
	Topic string `json:"topic"`
	ID    string `json:"id"`
}

type taskData struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Body  string `json:"body"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// writeJSON encodes v as JSON with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ok writes a standard success response.
func ok(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, apiResponse{Code: 0, Message: "ok"})
}

// errResponse maps a domain error to an HTTP status code and writes the
// corresponding JSON error response.
func errResponse(w http.ResponseWriter, err error) {
	var status int
	switch {
	case errors.Is(err, ErrDuplicateTask):
		status = http.StatusConflict
	case errors.Is(err, ErrTaskNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrInvalidTask), errors.Is(err, ErrInvalidDelay):
		status = http.StatusBadRequest
	case errors.Is(err, ErrTopicConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrClosed):
		status = http.StatusServiceUnavailable
	default:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, apiResponse{Code: status, Message: err.Error()})
}

// decodeJSON decodes the request body into v. Returns false and writes a 400
// response on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Code:    http.StatusBadRequest,
			Message: "invalid JSON: " + err.Error(),
		})
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// handleAdd handles POST /add.
// Body: {"topic":"x","id":"y","body":"...","delay_ms":1000,"ttr_ms":30000,"max_retries":3}
func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req addRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	task := &Task{
		ID:         req.ID,
		Topic:      req.Topic,
		Body:       []byte(req.Body),
		Delay:      time.Duration(req.DelayMS) * time.Millisecond,
		TTR:        time.Duration(req.TTRMS) * time.Millisecond,
		MaxRetries: req.MaxRetries,
	}

	if err := s.queue.Add(r.Context(), task); err != nil {
		errResponse(w, err)
		return
	}
	ok(w)
}

// handlePop handles POST /pop.
// Body: {"topic":"x","timeout_ms":30000}
func (s *Server) handlePop(w http.ResponseWriter, r *http.Request) {
	var req popRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	// Override the queue's default pop timeout if the caller supplied one.
	ctx := r.Context()
	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	task, err := s.queue.Pop(ctx, req.Topic)
	if err != nil {
		errResponse(w, err)
		return
	}
	if task == nil {
		// Timeout with no task.
		writeJSON(w, http.StatusOK, apiResponse{Code: 0, Data: nil})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Code: 0,
		Data: taskData{
			ID:    task.ID,
			Topic: task.Topic,
			Body:  string(task.Body),
		},
	})
}

// handleFinish handles POST /finish.
// Body: {"topic":"x","id":"y"}
func (s *Server) handleFinish(w http.ResponseWriter, r *http.Request) {
	var req topicIDRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.queue.Finish(r.Context(), req.Topic, req.ID); err != nil {
		errResponse(w, err)
		return
	}
	ok(w)
}

// handleCancel handles POST /cancel.
// Body: {"topic":"x","id":"y"}
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var req topicIDRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.queue.Cancel(r.Context(), req.Topic, req.ID); err != nil {
		errResponse(w, err)
		return
	}
	ok(w)
}

// handleGet handles GET /get?topic=x&id=y.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	id := r.URL.Query().Get("id")

	if topic == "" || id == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Code:    http.StatusBadRequest,
			Message: "topic and id query parameters are required",
		})
		return
	}

	task, err := s.queue.Get(r.Context(), topic, id)
	if err != nil {
		errResponse(w, err)
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{Code: 0, Data: task})
}

// topicStats holds per-topic counts broken down by task state.
type topicStats struct {
	Ready     int64 `json:"ready"`
	Delayed   int64 `json:"delayed"`
	Active    int64 `json:"active"`
	Finished  int64 `json:"finished"`
	Cancelled int64 `json:"cancelled"`
	Total     int64 `json:"total"`
}

// computeTopicStats returns per-state counts for the given topic.
// Reads the topic index and tallies live tasks; the ready-list length is taken
// from Redis directly so it stays accurate even when index entries lag.
func (s *Server) computeTopicStats(ctx context.Context, topic string) topicStats {
	stats := topicStats{}
	stats.Ready, _ = s.queue.cfg.redisClient.LLen(ctx, readyKey(topic)).Result()

	tasks, _ := s.queue.store.LoadTopicTasks(ctx, topic)
	for _, t := range tasks {
		stats.Total++
		switch t.State {
		case StateDelayed:
			stats.Delayed++
		case StateActive:
			stats.Active++
		case StateFinished:
			stats.Finished++
		case StateCancelled:
			stats.Cancelled++
		}
	}
	return stats
}

// handleStats handles GET /stats.
// Returns per-topic counts broken down by task state.
//
// Optional query parameter:
//   - topic — return stats only for the named topic.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if topic := r.URL.Query().Get("topic"); topic != "" {
		writeJSON(w, http.StatusOK, apiResponse{
			Code: 0,
			Data: map[string]any{
				"topic": topic,
				"stats": s.computeTopicStats(ctx, topic),
			},
		})
		return
	}

	topics, err := s.queue.store.ListTopics(ctx)
	if err != nil {
		errResponse(w, err)
		return
	}

	stats := make(map[string]topicStats, len(topics))
	for _, topic := range topics {
		stats[topic] = s.computeTopicStats(ctx, topic)
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Code: 0,
		Data: map[string]any{"topics": stats},
	})
}

// topicSummary is the per-topic entry returned by GET /topics.
type topicSummary struct {
	Name  string     `json:"name"`
	Stats topicStats `json:"stats"`
}

// handleListTopics handles GET /topics.
// Returns the list of known topics together with their per-state counts.
func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topics, err := s.queue.store.ListTopics(ctx)
	if err != nil {
		errResponse(w, err)
		return
	}

	list := make([]topicSummary, 0, len(topics))
	for _, topic := range topics {
		list = append(list, topicSummary{
			Name:  topic,
			Stats: s.computeTopicStats(ctx, topic),
		})
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Code: 0,
		Data: map[string]any{
			"topics": list,
			"total":  len(list),
		},
	})
}

// taskListItem is the per-task entry returned by GET /tasks.
// Body is base64-omitted here — callers fetch the full record via /get when needed.
type taskListItem struct {
	ID         string `json:"id"`
	Topic      string `json:"topic"`
	Body       string `json:"body"`
	State      string `json:"state"`
	DelayMS    int64  `json:"delay_ms"`
	TTRMS      int64  `json:"ttr_ms"`
	MaxRetries int    `json:"max_retries"`
	Retries    int    `json:"retries"`
	CreatedAt  int64  `json:"created_at"`
	ActiveAt   int64  `json:"active_at"`
}

// handleListTasks handles GET /tasks.
// Query parameters:
//   - topic     — required, the topic to inspect
//   - state     — optional, filter by task state name (delayed/ready/active/finished/cancelled)
//   - page      — optional, 1-based page number (default 1)
//   - page_size — optional, max 200 (default 20)
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Code:    http.StatusBadRequest,
			Message: "topic query parameter is required",
		})
		return
	}

	stateFilter := r.URL.Query().Get("state")

	page := parsePositiveInt(r.URL.Query().Get("page"), 1)
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 20)
	if pageSize > 200 {
		pageSize = 200
	}

	tasks, err := s.queue.store.LoadTopicTasks(ctx, topic)
	if err != nil {
		errResponse(w, err)
		return
	}

	// Filter + state-based projection in one pass; ready-list IDs need a Redis
	// lookup because they don't have their own state stored.
	readyIDs, _ := s.queue.store.ReadyListIDs(ctx, topic)

	filtered := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		state := t.State.String()
		if _, isReady := readyIDs[t.ID]; isReady && t.State == StateDelayed {
			// Edge case: ID lives on the ready list but task record still says
			// delayed (a moment of inconsistency during the transition). Report
			// it as ready so the UI matches user expectation.
			state = "ready"
		}
		if stateFilter != "" && state != stateFilter {
			continue
		}
		filtered = append(filtered, t)
	}

	total := len(filtered)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	items := make([]taskListItem, 0, end-start)
	for _, t := range filtered[start:end] {
		state := t.State.String()
		if _, isReady := readyIDs[t.ID]; isReady && t.State == StateDelayed {
			state = "ready"
		}
		var activeAt int64
		if !t.ActiveAt.IsZero() {
			activeAt = t.ActiveAt.UnixMilli()
		}
		items = append(items, taskListItem{
			ID:         t.ID,
			Topic:      t.Topic,
			Body:       string(t.Body),
			State:      state,
			DelayMS:    t.Delay.Milliseconds(),
			TTRMS:      t.TTR.Milliseconds(),
			MaxRetries: t.MaxRetries,
			Retries:    t.Retries,
			CreatedAt:  t.CreatedAt.UnixMilli(),
			ActiveAt:   activeAt,
		})
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Code: 0,
		Data: map[string]any{
			"items":     items,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
		},
	})
}

// parsePositiveInt parses s as a positive int, returning fallback on failure or
// when the result is <= 0.
func parsePositiveInt(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}
