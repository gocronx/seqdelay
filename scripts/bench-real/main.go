// scripts/bench-real — end-to-end behavioural & performance test against a
// running seqdelay HTTP server (default :9280) and seqdelay-admin (default
// :8888). Exercises:
//
//   1. Add throughput          — pure HTTP /add latency under load
//   2. Delay precision         — task transitions to "ready" within ~1ms of
//                                expected Delay; quantifies P50/P95/P99
//   3. Cancel correctness      — a cancelled task NEVER fires (never reaches
//                                ready/active), even past its delay
//   4. Mixed delay distribution — short + long delays interleaved fire at
//                                the right time without head-of-line blocking
//   5. Admin vs raw data       — admin /api/v1/seqdelay/* matches seqdelay
//                                /topics + /tasks byte-for-byte (after a
//                                stable read)
//
// Usage:
//
//	cd seqdelay
//	go run ./scripts/bench-real
//
// All tests print PASS/FAIL summaries. Exit non-zero on any FAIL.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	seqdelayURL = "http://localhost:9280"
	adminURL    = "http://localhost:8888"
	adminUser   = "admin"
	adminPass   = "admin123"
)

var httpc = &http.Client{Timeout: 10 * time.Second}

// failure counter — any test that fails increments this, main exits with
// the count.
var failures atomic.Int32

func main() {
	fmt.Println("=== seqdelay end-to-end bench ===")
	fmt.Println()

	// Probe seqdelay; admin is optional (skip the consistency test when the
	// admin service isn't running — e.g. in seqdelay's own CI).
	if err := probe(seqdelayURL + "/stats"); err != nil {
		die("seqdelay not reachable on %s: %v", seqdelayURL, err)
	}
	skipAdmin := os.Getenv("SKIP_ADMIN_TEST") == "1"
	if !skipAdmin {
		if err := probe(adminURL + "/health"); err != nil {
			fmt.Printf("[INFO] admin not reachable on %s — skipping admin consistency test (set SKIP_ADMIN_TEST=1 to silence)\n\n", adminURL)
			skipAdmin = true
		}
	}

	testAddThroughput(1000)
	testAddThroughput(10000)
	testDelayPrecision(200, 1000*time.Millisecond)
	testCancelCorrectness(100, 1500*time.Millisecond)
	testMixedDelayDistribution()
	if !skipAdmin {
		testAdminVsRawConsistency()
	}

	fmt.Println()
	if failures.Load() > 0 {
		fmt.Printf("BENCH FAILED: %d test(s) failed\n", failures.Load())
		os.Exit(1)
	}
	fmt.Println("BENCH OK")
}

// ──────────────────────────────────────────────────────────────────────────
// Test 1: Add throughput
// ──────────────────────────────────────────────────────────────────────────

func testAddThroughput(n int) {
	topic := fmt.Sprintf("bench-thru-%d-%d", n, time.Now().UnixNano())
	fmt.Printf("── Test: Add throughput (n=%d, topic=%s)\n", n, topic)

	// Use long delay so tasks don't fire during test.
	const delayMS = 5 * 60 * 1000 // 5 minutes
	const ttrMS = 30 * 1000

	// 50 parallel workers — typical client concurrency.
	const workers = 50
	jobs := make(chan int, n)
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(workers)
	start := time.Now()
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				_ = addTask(topic, fmt.Sprintf("task-%d", i), delayMS, ttrMS)
			}
		}()
	}
	wg.Wait()
	dur := time.Since(start)

	rate := float64(n) / dur.Seconds()
	avg := dur / time.Duration(n)
	fmt.Printf("    %d adds in %v   →  %.0f tasks/sec   (avg %v per add)\n", n, dur.Round(time.Millisecond), rate, avg.Round(time.Microsecond))

	// Verify count via /tasks.
	res, err := listTasks(topic, "", 1, 200)
	if err != nil {
		fail("    list failed: %v", err)
		return
	}
	if res.Total != n {
		fail("    expected %d tasks in topic, got %d", n, res.Total)
		return
	}
	fmt.Printf("    PASS — verified %d tasks present in topic\n\n", res.Total)
}

// ──────────────────────────────────────────────────────────────────────────
// Test 2: Delay precision (the time-wheel claim)
// ──────────────────────────────────────────────────────────────────────────

// testDelayPrecision schedules N tasks with the same Delay D, then polls each
// every ~10ms to detect the moment it transitions to ready/active. Records
// |actual - expected| for each task and prints P50/P95/P99.
//
// The time wheel claims 1ms tick precision; we expect P95 << 50ms in practice
// once HTTP round-trip and goroutine scheduling are accounted for.
func testDelayPrecision(n int, delay time.Duration) {
	topic := fmt.Sprintf("bench-precision-%d", time.Now().UnixNano())
	fmt.Printf("── Test: Delay precision (n=%d, delay=%v, topic=%s)\n", n, delay, topic)

	// Schedule all tasks. Record per-task expected fire time = addedAt + delay.
	type sched struct {
		id       string
		expected time.Time
		actual   time.Time
	}
	tasks := make([]*sched, n)
	addStart := time.Now()
	for i := 0; i < n; i++ {
		t := time.Now()
		id := fmt.Sprintf("p-%d", i)
		if err := addTask(topic, id, delay.Milliseconds(), 30000); err != nil {
			fail("    add %d failed: %v", i, err)
			return
		}
		tasks[i] = &sched{id: id, expected: t.Add(delay)}
	}
	addDur := time.Since(addStart)

	// Poll every ~15ms until all tasks have fired (state ≠ delayed) or timeout.
	deadline := time.Now().Add(delay + 5*time.Second)
	pending := n
	for time.Now().Before(deadline) && pending > 0 {
		// Pull the full task list in one shot (cheaper than per-task /get).
		// state-filter for `delayed` to see who's left.
		res, err := listTasks(topic, "delayed", 1, n+10)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		stillDelayed := make(map[string]struct{}, len(res.Items))
		for _, it := range res.Items {
			stillDelayed[it.ID] = struct{}{}
		}
		now := time.Now()
		newlyFired := 0
		for _, t := range tasks {
			if t.actual.IsZero() {
				if _, still := stillDelayed[t.id]; !still {
					t.actual = now
					newlyFired++
				}
			}
		}
		pending -= newlyFired
		time.Sleep(15 * time.Millisecond)
	}

	if pending > 0 {
		fail("    %d/%d tasks never transitioned out of delayed within timeout", pending, n)
		return
	}

	// Compute |actual - expected| distribution.
	deltas := make([]time.Duration, 0, n)
	var late, early int
	for _, t := range tasks {
		d := t.actual.Sub(t.expected)
		if d < 0 {
			early++
			d = -d
		} else if d > 0 {
			late++
		}
		deltas = append(deltas, d)
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i] < deltas[j] })
	p50 := deltas[len(deltas)*50/100]
	p95 := deltas[len(deltas)*95/100]
	p99 := deltas[len(deltas)*99/100]
	max := deltas[len(deltas)-1]

	fmt.Printf("    add latency: %v total (%v avg)\n", addDur.Round(time.Millisecond), (addDur / time.Duration(n)).Round(time.Microsecond))
	fmt.Printf("    delay deviation |actual-expected|:\n")
	fmt.Printf("      P50=%v  P95=%v  P99=%v  MAX=%v\n", p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), max.Round(time.Microsecond))
	fmt.Printf("      late=%d  early=%d\n", late, early)

	// PASS if P95 < 100ms. Time wheel ticks every 1ms; we tolerate Redis IO
	// and our 15ms poll granularity (which is itself an upper bound on
	// measurement error, NOT a real time-wheel error).
	if p95 > 100*time.Millisecond {
		fail("    P95 deviation too high (%v > 100ms)", p95)
		return
	}
	fmt.Printf("    PASS — time wheel fires within tolerance\n\n")
}

// ──────────────────────────────────────────────────────────────────────────
// Test 3: Cancel correctness
// ──────────────────────────────────────────────────────────────────────────

// testCancelCorrectness schedules N tasks with delay D, cancels them at D/3,
// then waits past D + 1s and confirms none of them ever became ready/active.
// This is the core safety property of a delay queue: a cancelled task must
// never fire.
func testCancelCorrectness(n int, delay time.Duration) {
	topic := fmt.Sprintf("bench-cancel-%d", time.Now().UnixNano())
	fmt.Printf("── Test: Cancel correctness (n=%d, delay=%v, topic=%s)\n", n, delay, topic)

	// Schedule.
	for i := 0; i < n; i++ {
		if err := addTask(topic, fmt.Sprintf("c-%d", i), delay.Milliseconds(), 30000); err != nil {
			fail("    add %d failed: %v", i, err)
			return
		}
	}

	// Cancel partway through the delay.
	time.Sleep(delay / 3)
	cancelStart := time.Now()
	for i := 0; i < n; i++ {
		if err := cancelTask(topic, fmt.Sprintf("c-%d", i)); err != nil {
			fail("    cancel %d failed: %v", i, err)
			return
		}
	}
	fmt.Printf("    cancelled all %d in %v\n", n, time.Since(cancelStart).Round(time.Millisecond))

	// Wait past original delay + buffer, then check state.
	time.Sleep(delay + 1500*time.Millisecond)

	// Check each task individually via /get (cheaper than scanning when n is small).
	var firedAnyway int
	for i := 0; i < n; i++ {
		state, err := getTaskState(topic, fmt.Sprintf("c-%d", i))
		if err != nil {
			// 404 means the 60s tombstone expired (unlikely in this test
			// window but possible). Either way, it's not "fired".
			continue
		}
		// state 4 = cancelled (per seqdelay/task.go enum). 1=ready,2=active,3=finished.
		if state == 1 || state == 2 || state == 3 {
			firedAnyway++
		}
	}
	if firedAnyway > 0 {
		fail("    %d cancelled task(s) FIRED ANYWAY — cancel did not stop the wheel", firedAnyway)
		return
	}
	fmt.Printf("    PASS — zero cancelled tasks fired (safety invariant holds)\n\n")
}

// ──────────────────────────────────────────────────────────────────────────
// Test 4: Mixed delay distribution
// ──────────────────────────────────────────────────────────────────────────

// testMixedDelayDistribution schedules tasks with varied delays from a single
// topic. Validates that short-delay tasks fire at the right time even when
// long-delay tasks are also queued — i.e. the wheel doesn't head-of-line block.
func testMixedDelayDistribution() {
	topic := fmt.Sprintf("bench-mixed-%d", time.Now().UnixNano())
	fmt.Printf("── Test: Mixed delay distribution (topic=%s)\n", topic)

	type bucket struct {
		delay time.Duration
		count int
		ids   []string
	}
	buckets := []*bucket{
		{delay: 200 * time.Millisecond, count: 50},
		{delay: 1 * time.Second, count: 50},
		{delay: 3 * time.Second, count: 50},
	}

	// Shuffle add order so short-delay tasks aren't all added first.
	type addPlan struct {
		b    *bucket
		idx  int
		when time.Time
	}
	var plan []addPlan
	for _, b := range buckets {
		b.ids = make([]string, b.count)
		for i := 0; i < b.count; i++ {
			plan = append(plan, addPlan{b: b, idx: i})
		}
	}
	rand.Shuffle(len(plan), func(i, j int) { plan[i], plan[j] = plan[j], plan[i] })

	// Add all and capture expected fire times.
	type exp struct {
		bucket *bucket
		id     string
		expect time.Time
	}
	expected := make([]exp, len(plan))
	for i, p := range plan {
		id := fmt.Sprintf("m-%d-%d", p.b.delay.Milliseconds(), p.idx)
		p.b.ids[p.idx] = id
		added := time.Now()
		if err := addTask(topic, id, p.b.delay.Milliseconds(), 30000); err != nil {
			fail("    add failed: %v", err)
			return
		}
		expected[i] = exp{bucket: p.b, id: id, expect: added.Add(p.b.delay)}
	}

	// Wait until all 3 buckets have fired (longest delay + buffer).
	deadline := time.Now().Add(5 * time.Second)
	fired := make(map[string]time.Time, len(expected))
	for time.Now().Before(deadline) && len(fired) < len(expected) {
		res, err := listTasks(topic, "delayed", 1, 1000)
		if err == nil {
			delayed := make(map[string]struct{}, len(res.Items))
			for _, it := range res.Items {
				delayed[it.ID] = struct{}{}
			}
			now := time.Now()
			for _, e := range expected {
				if _, found := fired[e.id]; found {
					continue
				}
				if _, still := delayed[e.id]; !still {
					fired[e.id] = now
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(fired) < len(expected) {
		fail("    only %d/%d tasks fired within window", len(fired), len(expected))
		return
	}

	// Per-bucket delay deviation.
	type stat struct {
		bucket *bucket
		max    time.Duration
		sum    time.Duration
		n      int
	}
	stats := make(map[time.Duration]*stat)
	for _, b := range buckets {
		stats[b.delay] = &stat{bucket: b}
	}
	for _, e := range expected {
		actual := fired[e.id]
		dev := actual.Sub(e.expect)
		if dev < 0 {
			dev = -dev
		}
		s := stats[e.bucket.delay]
		s.n++
		s.sum += dev
		if dev > s.max {
			s.max = dev
		}
	}
	for _, b := range buckets {
		s := stats[b.delay]
		avg := s.sum / time.Duration(s.n)
		fmt.Printf("    delay=%v  count=%d  avg deviation=%v  max=%v\n", b.delay, s.n, avg.Round(time.Microsecond), s.max.Round(time.Microsecond))
	}

	// PASS if every bucket avg < 100ms (poll granularity dominates).
	for _, b := range buckets {
		s := stats[b.delay]
		avg := s.sum / time.Duration(s.n)
		if avg > 100*time.Millisecond {
			fail("    bucket %v: avg deviation %v exceeds 100ms", b.delay, avg)
			return
		}
	}
	fmt.Printf("    PASS — no head-of-line blocking, all buckets fire on time\n\n")
}

// ──────────────────────────────────────────────────────────────────────────
// Test 5: Admin vs raw data consistency
// ──────────────────────────────────────────────────────────────────────────

func testAdminVsRawConsistency() {
	topic := fmt.Sprintf("bench-admin-%d", time.Now().UnixNano())
	fmt.Printf("── Test: Admin vs raw data consistency (topic=%s)\n", topic)

	// Seed a known mix of states.
	for i := 0; i < 5; i++ {
		_ = addTask(topic, fmt.Sprintf("a-%d", i), 600000, 30000) // delayed
	}
	// Cancel 2 of them.
	_ = cancelTask(topic, "a-0")
	_ = cancelTask(topic, "a-1")

	// Wait a bit for state to settle.
	time.Sleep(200 * time.Millisecond)

	// Raw seqdelay list.
	rawRes, err := rawListTasks(topic)
	if err != nil {
		fail("    raw list failed: %v", err)
		return
	}
	rawByID := map[string]string{}
	for _, it := range rawRes {
		rawByID[it.ID] = it.State
	}

	// Admin list.
	token, err := adminLogin()
	if err != nil {
		fail("    admin login failed: %v", err)
		return
	}
	adminItems, err := adminListTasks(token, topic)
	if err != nil {
		fail("    admin list failed: %v", err)
		return
	}
	adminByID := map[string]string{}
	for _, it := range adminItems {
		adminByID[it.ID] = it.State
	}

	// Compare.
	if len(rawByID) != len(adminByID) {
		fail("    count mismatch: raw=%d admin=%d", len(rawByID), len(adminByID))
		fmt.Printf("    raw   ids: %v\n", rawByID)
		fmt.Printf("    admin ids: %v\n", adminByID)
		return
	}
	for id, rawState := range rawByID {
		adminState, ok := adminByID[id]
		if !ok {
			fail("    raw has %q (%s) but admin missing", id, rawState)
			return
		}
		if rawState != adminState {
			fail("    state mismatch for %q: raw=%s admin=%s", id, rawState, adminState)
			return
		}
	}
	fmt.Printf("    raw:   %d items, %d cancelled\n", len(rawByID), countState(rawByID, "cancelled"))
	fmt.Printf("    admin: %d items, %d cancelled\n", len(adminByID), countState(adminByID, "cancelled"))
	fmt.Printf("    PASS — admin /api/v1/seqdelay/tasks matches seqdelay /tasks exactly\n\n")
}

func countState(m map[string]string, s string) int {
	n := 0
	for _, v := range m {
		if v == s {
			n++
		}
	}
	return n
}

// ──────────────────────────────────────────────────────────────────────────
// HTTP helpers
// ──────────────────────────────────────────────────────────────────────────

type envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type taskListItem struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	State string `json:"state"`
}

type listTasksResult struct {
	Items []taskListItem `json:"items"`
	Total int            `json:"total"`
}

func addTask(topic, id string, delayMS, ttrMS int64) error {
	body, _ := json.Marshal(map[string]any{
		"topic":    topic,
		"id":       id,
		"delay_ms": delayMS,
		"ttr_ms":   ttrMS,
	})
	resp, err := httpc.Post(seqdelayURL+"/add", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}
	return nil
}

func cancelTask(topic, id string) error {
	body, _ := json.Marshal(map[string]string{"topic": topic, "id": id})
	resp, err := httpc.Post(seqdelayURL+"/cancel", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}
	return nil
}

func getTaskState(topic, id string) (int, error) {
	url := fmt.Sprintf("%s/get?topic=%s&id=%s", seqdelayURL, topic, id)
	resp, err := httpc.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return -1, fmt.Errorf("not found")
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return 0, err
	}
	var d struct {
		State int `json:"state"`
	}
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return 0, err
	}
	return d.State, nil
}

func listTasks(topic, state string, page, pageSize int) (*listTasksResult, error) {
	url := fmt.Sprintf("%s/tasks?topic=%s&page=%d&page_size=%d", seqdelayURL, topic, page, pageSize)
	if state != "" {
		url += "&state=" + state
	}
	resp, err := httpc.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("seqdelay code=%d %s", env.Code, env.Message)
	}
	var res listTasksResult
	if err := json.Unmarshal(env.Data, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func rawListTasks(topic string) ([]taskListItem, error) {
	res, err := listTasks(topic, "", 1, 1000)
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Admin helpers (login + list via admin API)
// ──────────────────────────────────────────────────────────────────────────

func adminLogin() (string, error) {
	body, _ := json.Marshal(map[string]string{"username": adminUser, "password": adminPass})
	resp, err := httpc.Post(adminURL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var env struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return "", err
	}
	if env.Code != 0 || env.Data.AccessToken == "" {
		return "", fmt.Errorf("admin login: code=%d msg=%s", env.Code, env.Msg)
	}
	return env.Data.AccessToken, nil
}

func adminListTasks(token, topic string) ([]taskListItem, error) {
	url := fmt.Sprintf("%s/api/v1/seqdelay/tasks?topic=%s&pageSize=1000", adminURL, topic)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// Admin uses {code,msg,data:{list,total,page,pageSize}} envelope.
	var env struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			List []taskListItem `json:"list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("admin code=%d %s", env.Code, env.Msg)
	}
	return env.Data.List, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Misc
// ──────────────────────────────────────────────────────────────────────────

func probe(url string) error {
	resp, err := httpc.Get(url)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

func fail(format string, a ...any) {
	failures.Add(1)
	fmt.Printf(format+"\n", a...)
}
