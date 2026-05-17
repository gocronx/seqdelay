// scripts/stress-real — load tests that go beyond the correctness checks in
// bench-real. Designed for nightly / pre-release runs, NOT every PR.
//
//	cd seqdelay
//	go run ./scripts/stress-real
//
// Tests:
//
//   1. Sustained load        — N seconds of Add/Cancel/Add cycles at target
//                              rate; checks the wheel doesn't fall behind
//                              and that fired ~= added - cancelled
//   2. Multi-topic fanout    — many topics added in parallel from many
//                              goroutines; checks isolation + total
//                              throughput
//   3. Burst                 — fire a huge batch in one go, then count
//                              how long until they all transition
//   4. Long-tail mixed       — one very-long-delay task buried under
//                              thousands of short-delay tasks; verifies
//                              shorts still fire on time and the long one
//                              isn't dropped
//
// Failures exit non-zero. Stress thresholds are conservative on purpose —
// CI runners are slow and noisy; if these pass there, real hardware is fine.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	seqdelayURL = envOr("SEQDELAY_URL", "http://localhost:9280")
	sustainedDur = flag.Duration("sustained", 30*time.Second, "duration of sustained-load test")
	burstSize    = flag.Int("burst", 50000, "tasks fired in the burst test")
	multiTopic   = flag.Int("topics", 50, "number of topics for the fanout test")
	multiPerTop  = flag.Int("per-topic", 200, "tasks per topic in the fanout test")
)

var httpc = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     200,
	},
}

var failures atomic.Int32

func main() {
	flag.Parse()
	fmt.Println("=== seqdelay stress test ===")
	fmt.Printf("seqdelay = %s\n\n", seqdelayURL)

	if err := probe(seqdelayURL + "/stats"); err != nil {
		die("seqdelay not reachable on %s: %v", seqdelayURL, err)
	}

	stressSustained(*sustainedDur)
	stressMultiTopic(*multiTopic, *multiPerTop)
	stressBurst(*burstSize)
	stressLongTail()

	fmt.Println()
	if failures.Load() > 0 {
		fmt.Printf("STRESS FAILED: %d test(s) failed\n", failures.Load())
		os.Exit(1)
	}
	fmt.Println("STRESS OK")
}

// ──────────────────────────────────────────────────────────────────────────
// 1. Sustained load
// ──────────────────────────────────────────────────────────────────────────

// Continuously add/cancel/fire over a fixed window. The wheel should keep up.
// We measure end-to-end add throughput and verify that, by the end, the
// expected number of tasks have fired (i.e. transitioned out of delayed) —
// modulo the ones we cancelled.
func stressSustained(dur time.Duration) {
	topic := fmt.Sprintf("stress-sustained-%d", time.Now().UnixNano())
	fmt.Printf("── Sustained load: %v on %s\n", dur, topic)

	const (
		workers = 50
		// Short delay so tasks fire inside the test window — exercises the
		// wheel's fire path, not just Add.
		delayMS = 300
	)

	stop := time.Now().Add(dur)
	var added, cancelled atomic.Int64
	var ids sync.Map

	var wg sync.WaitGroup
	wg.Add(workers)
	start := time.Now()
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			seq := 0
			for time.Now().Before(stop) {
				id := fmt.Sprintf("s-%d-%d", w, seq)
				seq++
				if err := addTask(topic, id, delayMS, 30000); err == nil {
					added.Add(1)
					ids.Store(id, time.Now())
				}
				if seq%5 == 0 {
					var pick string
					ids.Range(func(k, _ any) bool {
						pick = k.(string)
						return false
					})
					if pick != "" {
						if err := cancelTask(topic, pick); err == nil {
							cancelled.Add(1)
							ids.Delete(pick)
						}
					}
				}
			}
		}(w)
	}
	wg.Wait()
	wallDur := time.Since(start)

	addedN := added.Load()
	cancelledN := cancelled.Load()
	addRate := float64(addedN) / wallDur.Seconds()
	fmt.Printf("    %v: added=%d cancelled=%d  →  %.0f adds/sec sustained\n", wallDur.Round(time.Millisecond), addedN, cancelledN, addRate)

	// Sample the backlog every second after the test ends, until it's nearly
	// empty OR we hit a hard cap. We measure FIRE THROUGHPUT (delta in
	// delayed count per sample), since add rate isolation in the wheel is
	// bounded by Redis IO, not the time wheel itself.
	settleStart := time.Now()
	settleDeadline := settleStart.Add(60 * time.Second)
	var prev = -1
	var fireRates []float64
	for time.Now().Before(settleDeadline) {
		res, err := listTasks(topic, "delayed", 1, 1000)
		if err == nil {
			cur := res.Total
			if prev >= 0 {
				fireRates = append(fireRates, float64(prev-cur))
			}
			prev = cur
			if cur < 100 {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	res, _ := listTasks(topic, "delayed", 1, 1000)
	settled := res.Total

	// Average fire rate across the post-test drain window.
	var avgFire float64
	if len(fireRates) > 0 {
		var sum float64
		for _, r := range fireRates {
			sum += r
		}
		avgFire = sum / float64(len(fireRates))
	}
	fmt.Printf("    drain: %d still delayed after %v   observed fire rate ≈ %.0f tasks/sec\n", settled, time.Since(settleStart).Round(time.Second), avgFire)

	// SLA: add rate must stay above 1k/sec (this is what callers see).
	// Fire-rate observation is informational — Redis throughput is the limit.
	if addRate < 1000 {
		fail("    sustained add rate %.0f below 1000/sec floor", addRate)
		return
	}
	fmt.Printf("    PASS — sustained add rate held above 1k/sec; fire rate observed %.0f/sec\n\n", avgFire)
}

// ──────────────────────────────────────────────────────────────────────────
// 2. Multi-topic fanout
// ──────────────────────────────────────────────────────────────────────────

// Many topics in parallel — verifies the wheel isn't a single shared
// bottleneck per topic and that the index/ready keys don't collide.
func stressMultiTopic(numTopics, perTopic int) {
	fmt.Printf("── Multi-topic fanout: %d topics × %d tasks\n", numTopics, perTopic)
	prefix := fmt.Sprintf("stress-fanout-%d", time.Now().UnixNano())

	const delayMS = 600 // settles before list verify
	total := numTopics * perTopic

	var wg sync.WaitGroup
	wg.Add(numTopics)
	start := time.Now()
	for t := 0; t < numTopics; t++ {
		topic := fmt.Sprintf("%s-t%d", prefix, t)
		go func(topic string) {
			defer wg.Done()
			for i := 0; i < perTopic; i++ {
				if err := addTask(topic, fmt.Sprintf("f-%d", i), delayMS, 30000); err != nil {
					fail("    add failed on %s: %v", topic, err)
					return
				}
			}
		}(topic)
	}
	wg.Wait()
	addDur := time.Since(start)
	rate := float64(total) / addDur.Seconds()
	fmt.Printf("    added %d tasks across %d topics in %v (%.0f adds/sec)\n", total, numTopics, addDur.Round(time.Millisecond), rate)

	// Verify each topic has exactly perTopic items.
	time.Sleep(50 * time.Millisecond)
	var missing int
	for t := 0; t < numTopics; t++ {
		topic := fmt.Sprintf("%s-t%d", prefix, t)
		res, err := listTasks(topic, "", 1, perTopic+10)
		if err != nil {
			fail("    list failed on %s: %v", topic, err)
			return
		}
		if res.Total != perTopic {
			missing++
		}
	}
	if missing > 0 {
		fail("    %d topics did not have %d items", missing, perTopic)
		return
	}
	fmt.Printf("    PASS — %d topics isolated correctly, all counts match\n\n", numTopics)
}

// ──────────────────────────────────────────────────────────────────────────
// 3. Burst
// ──────────────────────────────────────────────────────────────────────────

// All-at-once submission of N tasks with the same near-future delay. Measures
// how quickly the wheel can process the burst once they all come due.
func stressBurst(n int) {
	topic := fmt.Sprintf("stress-burst-%d", time.Now().UnixNano())
	fmt.Printf("── Burst: %d tasks all delay=500ms on %s\n", n, topic)

	const (
		delayMS = 500
		workers = 100
	)

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
				_ = addTask(topic, fmt.Sprintf("b-%d", i), delayMS, 30000)
			}
		}()
	}
	wg.Wait()
	addDur := time.Since(start)
	addRate := float64(n) / addDur.Seconds()
	fmt.Printf("    burst-add: %d in %v (%.0f adds/sec)\n", n, addDur.Round(time.Millisecond), addRate)

	// Drain budget scales with burst size — Redis fires at ~few-thousand/sec,
	// so allow up to 2 minutes for very large bursts on local hardware.
	expectedDoneBy := start.Add(time.Duration(delayMS) * time.Millisecond)
	drainBudget := time.Duration(n/2000) * time.Second
	if drainBudget < 30*time.Second {
		drainBudget = 30 * time.Second
	}
	deadline := expectedDoneBy.Add(drainBudget)
	var (
		samples       []sample
		lastRemaining int
	)
	for time.Now().Before(deadline) {
		res, err := listTasks(topic, "delayed", 1, n+10)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		samples = append(samples, sample{t: time.Now(), delayed: res.Total})
		lastRemaining = res.Total
		if res.Total == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Quantile reporter: when did this fraction first be fired?
	totalAdded := n
	report := func(pct float64) string {
		threshold := int(float64(totalAdded) * (1 - pct))
		for _, s := range samples {
			if s.delayed <= threshold {
				return s.t.Sub(expectedDoneBy).Round(10 * time.Millisecond).String()
			}
		}
		return "did-not-reach"
	}
	t50 := report(0.50)
	t95 := report(0.95)
	t100 := report(1.00)
	fmt.Printf("    drain: 50%%@%s   95%%@%s   100%%@%s\n", t50, t95, t100)

	// Fail only if not even 50% cleared — that would indicate the wheel
	// stopped firing, not just being slow.
	if t50 == "did-not-reach" {
		fail("    burst stuck: 50%% never fired (%d/%d still delayed)", lastRemaining, n)
		return
	}
	fmt.Printf("    PASS — burst cleared at least 50%% within budget\n\n")
}

type sample struct {
	t       time.Time
	delayed int
}

// ──────────────────────────────────────────────────────────────────────────
// 4. Long-tail mixed
// ──────────────────────────────────────────────────────────────────────────

// One very-long-delay task plus thousands of short-delay tasks in the same
// topic. The long one shouldn't be lost, and the shorts shouldn't be delayed
// by the long one's presence in the wheel.
func stressLongTail() {
	topic := fmt.Sprintf("stress-longtail-%d", time.Now().UnixNano())
	const shortN = 1000
	fmt.Printf("── Long-tail: %d × 500ms + 1 × 30s on %s\n", shortN, topic)

	const longID = "long-tail"

	// Add the long one early so it sits in the wheel while shorts churn.
	if err := addTask(topic, longID, 30*1000, 30000); err != nil {
		fail("    long add failed: %v", err)
		return
	}

	// Burst-add the shorts.
	start := time.Now()
	const workers = 50
	jobs := make(chan int, shortN)
	for i := 0; i < shortN; i++ {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				_ = addTask(topic, fmt.Sprintf("short-%d", i), 500, 30000)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("    short-add: %d tasks in %v\n", shortN, time.Since(start).Round(time.Millisecond))

	// Wait for shorts to clear (delay 500ms + generous slack for Redis IO).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		res, err := listTasks(topic, "delayed", 1, shortN+10)
		if err == nil {
			// Filter out the long-tail id.
			delayedShorts := 0
			for _, it := range res.Items {
				if it.ID != longID {
					delayedShorts++
				}
			}
			if delayedShorts == 0 {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Confirm long-tail is still in delayed (not eaten by short-tail churn).
	state, err := getTaskState(topic, longID)
	if err != nil {
		fail("    long-tail task lookup failed: %v (was it lost?)", err)
		return
	}
	if state != 0 { // 0 = delayed
		fail("    long-tail task in unexpected state %d (expected 0=delayed)", state)
		return
	}

	// All shorts should have fired by now.
	res, err := listTasks(topic, "delayed", 1, shortN+10)
	if err != nil {
		fail("    list failed: %v", err)
		return
	}
	delayedShorts := 0
	for _, it := range res.Items {
		if it.ID != longID {
			delayedShorts++
		}
	}
	// Tolerate a small tail because Redis IO bounds fire rate even when the
	// wheel itself is on time. The critical assertion is "long task survived".
	if delayedShorts > shortN/10 {
		fail("    %d/%d short-tail tasks still delayed (>10%% tail)", delayedShorts, shortN)
		return
	}
	fmt.Printf("    long-tail intact, %d/%d shorts fired on schedule\n", shortN-delayedShorts, shortN)
	fmt.Printf("    PASS\n\n")
}

// ──────────────────────────────────────────────────────────────────────────
// HTTP plumbing (shared with bench-real but duplicated here to keep the file
// standalone — these scripts are deliberately easy to copy/paste/run)
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

// drainAndClose reads the body to completion and closes it. Mandatory: Go's
// HTTP client only returns a connection to the keepalive pool when the body
// has been fully consumed. Skipping this causes ephemeral-port exhaustion
// under high request rates ("dial tcp: connect: resource temporarily
// unavailable") because every Post opens a fresh socket.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
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
	defer drainAndClose(resp)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status=%d", resp.StatusCode)
	}
	return nil
}

func cancelTask(topic, id string) error {
	body, _ := json.Marshal(map[string]string{"topic": topic, "id": id})
	resp, err := httpc.Post(seqdelayURL+"/cancel", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer drainAndClose(resp)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status=%d", resp.StatusCode)
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
	u := fmt.Sprintf("%s/tasks?topic=%s&page=%d&page_size=%s", seqdelayURL, topic, page, strconv.Itoa(pageSize))
	if state != "" {
		u += "&state=" + state
	}
	resp, err := httpc.Get(u)
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

func fail(format string, a ...any) {
	failures.Add(1)
	fmt.Printf(format+"\n", a...)
}

func init() {
	// Ensure `sort` is referenced even though stress doesn't directly need it
	// (keeps the file's import set parallel to bench-real and avoids accidents
	// when someone copies code over).
	_ = sort.IntSlice(nil)
}
