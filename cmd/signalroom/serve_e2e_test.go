package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	binaryPath string
	buildOnce  sync.Once
	buildErr   error
)

func TestMain(m *testing.M) {
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "signalroom-build-")
		if err != nil {
			buildErr = err
			return
		}
		binaryPath = filepath.Join(dir, "signalroom")
		buildErr = exec.Command("go", "build", "-o", binaryPath, ".").Run()
	})
	if buildErr != nil {
		fmt.Fprintln(os.Stderr, "build test binary:", buildErr)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

type serverProc struct {
	cmd    *exec.Cmd
	addr   string
	logs   *safeBuffer
	waitCh chan error
	done   chan struct{}
}

func (p *serverProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// startServer launches serve on an ephemeral port and blocks until the
// readiness line publishes the real address — no fixed sleeps.
func startServer(t *testing.T, dataDir string) *serverProc {
	t.Helper()
	logs := &safeBuffer{}
	cmd := exec.Command(binaryPath, "serve", "--addr", "127.0.0.1:0", "--data", dataDir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve: %v", err)
	}
	waitCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		waitCh <- cmd.Wait()
		close(done)
	}()

	ready := make(chan string, 1)
	scan := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			logs.Write([]byte(line + "\n"))
			if addr, ok := strings.CutPrefix(line, "signalroom: listening on "); ok {
				select {
				case ready <- strings.TrimSpace(addr):
				default:
				}
			}
		}
	}
	go scan(stdout)
	go scan(stderr)

	select {
	case addr := <-ready:
		proc := &serverProc{cmd: cmd, addr: addr, logs: logs, waitCh: waitCh, done: done}
		t.Cleanup(func() { proc.killIfAlive() })
		return proc
	case err := <-waitCh:
		t.Fatalf("serve exited before becoming ready (%v); output:\n%s", err, logs.String())
		return nil
	case <-time.After(5 * time.Second):
		t.Fatalf("server never became ready; output:\n%s", logs.String())
		return nil
	}
}

func (p *serverProc) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signal: %v", err)
	}
}

func (p *serverProc) waitExit(t *testing.T, wantCode int) {
	t.Helper()
	select {
	case err := <-p.waitCh:
		_ = err // captured below via ProcessState
	case <-time.After(5 * time.Second):
		t.Fatalf("process did not exit; output:\n%s", p.logs.String())
	}
	state := p.cmd.ProcessState
	if state == nil {
		t.Fatal("process state unavailable after wait")
	}
	got := state.ExitCode()
	if got != wantCode {
		t.Fatalf("want exit %d, got %d; output:\n%s", wantCode, got, p.logs.String())
	}
}

func (p *serverProc) killIfAlive() {
	if p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.done
}

func httpDo(t *testing.T, method, url, body string) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

func postEvents(t *testing.T, p *serverProc, body string) map[string]int {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/events", body)
	if status >= 400 {
		t.Fatalf("POST status %d: %s", status, raw)
	}
	var out map[string]int
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func getEvents(t *testing.T, p *serverProc, query string) []map[string]any {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/events"+query, "")
	if status != http.StatusOK {
		t.Fatalf("GET status %d: %s", status, raw)
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return out
}

func waitTCPPortClosed(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("port %s still accepting connections", addr)
}

// pageResult is one decoded GET /events/page response.
type pageResult struct {
	Events     []map[string]any `json:"events"`
	NextCursor *string          `json:"next_cursor"`
}

func getPage(t *testing.T, p *serverProc, query string) (int, pageResult) {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/events/page"+query, "")
	var out pageResult
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode page %q: %v", raw, err)
	}
	return status, out
}

func pageEventIDs(page pageResult) string {
	parts := make([]string, len(page.Events))
	for i, event := range page.Events {
		parts[i] = event["id"].(string)
	}
	return strings.Join(parts, ",")
}

func walkAllPages(t *testing.T, p *serverProc, firstQuery string) string {
	t.Helper()
	var got []string
	query := firstQuery
	cursor := ""
	for {
		var q string
		if cursor == "" {
			q = query
		} else {
			q = "?cursor=" + cursor
		}
		status, page := getPage(t, p, q)
		if status != http.StatusOK {
			t.Fatalf("page status %d", status)
		}
		for _, event := range page.Events {
			got = append(got, event["id"].(string))
		}
		if page.NextCursor == nil {
			return strings.Join(got, ",")
		}
		cursor = *page.NextCursor
	}
}

const sampleEvents = `{"events":[
	{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z"},
	{"id":"e2","service":"api","severity":"info","message":"ok","at":"2026-10-01T10:00:00Z"}
]}`

func TestServePersistsAcrossKillAndDedupesRetry(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	counts := postEvents(t, p, sampleEvents)
	if counts["created"] != 2 {
		t.Fatalf("created=%d", counts["created"])
	}

	// Hard kill: no graceful shutdown, fsync had already confirmed durability.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1) // killed by signal
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	got := getEvents(t, p2, "")
	if len(got) != 2 || got[0]["id"] != "e1" {
		t.Fatalf("recovery after SIGKILL: %+v", got)
	}

	// Retry of the fully confirmed batch dedupes after restart.
	retry := postEvents(t, p2, sampleEvents)
	if retry["created"] != 0 || retry["replayed"] != 2 {
		t.Fatalf("retry after restart: %+v", retry)
	}
	if len(getEvents(t, p2, "")) != 2 {
		t.Fatal("replayed retry must not duplicate events")
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0) // graceful shutdown exits cleanly
}

func TestServeRecoversUnconfirmedDurableBatch(t *testing.T) {
	dataDir := t.TempDir()
	// A complete frame that never received an HTTP confirmation (e.g. the
	// connection vanished after fsync). Restart must surface and dedupe it.
	payload := []byte(`[{"id":"orphan","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]`)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if err := os.MkdirAll(filepath.Join(dataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServer(t, dataDir)
	got := getEvents(t, p, "")
	if len(got) != 1 || got[0]["id"] != "orphan" {
		t.Fatalf("unconfirmed durable batch must be recovered: %+v", got)
	}
	retry := postEvents(t, p, `{"events":[{"id":"orphan","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	if retry["created"] != 0 || retry["replayed"] != 1 {
		t.Fatalf("recovered event must dedupe retries: %+v", retry)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeLabelsSurviveKillAndDedupeRetry(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	labeled := `{"events":[
		{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"e2","service":"api","severity":"info","message":"ok","at":"2026-10-01T10:00:00Z"}
	]}`
	if counts := postEvents(t, p, labeled); counts["created"] != 2 {
		t.Fatalf("created=%v", counts)
	}

	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	got := getEvents(t, p2, "")
	if len(got) != 2 {
		t.Fatalf("recovery after SIGKILL: %+v", got)
	}
	labels, ok := got[0]["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" || labels["version"] != "v2" {
		t.Fatalf("labels must survive restart: %+v", got[0])
	}
	if _, present := got[1]["labels"]; present {
		t.Fatalf("label-less event must stay label-less: %+v", got[1])
	}

	// A retry with reordered, whitespace-padded labels replays after restart.
	retry := postEvents(t, p2, `{"events":[{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z","labels":{" version ":"v2","env":" prod "}}]}`)
	if retry["created"] != 0 || retry["replayed"] != 1 {
		t.Fatalf("label retry after restart: %+v", retry)
	}
	// A changed label still conflicts after restart.
	status, raw := httpDo(t, http.MethodPost, "http://"+p2.addr+"/events",
		`{"events":[{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v3"}}]}`)
	if status != http.StatusConflict {
		t.Fatalf("changed labels must conflict after restart: %d %s", status, raw)
	}

	// Label filters work on recovered data.
	filtered := getEvents(t, p2, "?label=env=prod")
	if len(filtered) != 1 || filtered[0]["id"] != "e1" {
		t.Fatalf("label filter after restart: %+v", filtered)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServeSecondInstanceRejected(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postEvents(t, first, sampleEvents)

	logs := &safeBuffer{}
	second := exec.Command(binaryPath, "serve", "--addr", "127.0.0.1:0", "--data", dataDir)
	second.Stdout = logs
	second.Stderr = logs
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- second.Wait() }()
	select {
	case err := <-waitCh:
		if err == nil {
			t.Fatal("second instance must exit non-zero while directory is locked")
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("second instance must fail non-zero: %v", err)
		}
		if !strings.Contains(logs.String(), "in use") {
			t.Fatalf("error must explain directory is in use:\n%s", logs.String())
		}
	case <-time.After(5 * time.Second):
		second.Process.Kill()
		t.Fatal("second instance hung instead of failing on the lock")
	}

	// The first instance is untouched and keeps serving its data.
	if got := getEvents(t, first, ""); len(got) != 2 {
		t.Fatalf("failed takeover must not disturb first instance: %d events", len(got))
	}

	// After graceful exit, a new instance can claim the directory.
	first.signal(t, syscall.SIGTERM)
	first.waitExit(t, 0)
	waitTCPPortClosed(t, first.addr)
	third := startServer(t, dataDir)
	if got := getEvents(t, third, ""); len(got) != 2 {
		t.Fatalf("post-exit restart must recover data: %+v", got)
	}
	third.signal(t, syscall.SIGTERM)
	third.waitExit(t, 0)
}

func TestServeTornTailTrimmedAndDoesNotPollute(t *testing.T) {
	dataDir := t.TempDir()

	first := startServer(t, dataDir)
	postEvents(t, first, `{"events":[{"id":"good","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	path := filepath.Join(dataDir, "events.log")
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Append a torn partial frame (full header, truncated payload).
	payload := []byte(`[{"id":"torn","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]`)
	torn := make([]byte, 8+4)
	binary.BigEndian.PutUint32(torn[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(torn[4:8], crc32.ChecksumIEEE(payload))
	copy(torn[8:], payload[:4])
	if err := os.WriteFile(path, append(existing, torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	second := startServer(t, dataDir)
	got := getEvents(t, second, "")
	if len(got) != 1 || got[0]["id"] != "good" {
		t.Fatalf("torn tail must be ignored, got %+v", got)
	}

	// New writes after the trimmed tail must land in a clean frame and
	// survive another restart.
	postEvents(t, second, `{"events":[{"id":"after","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"}]}`)
	second.signal(t, syscall.SIGTERM)
	second.waitExit(t, 0)
	waitTCPPortClosed(t, second.addr)

	third := startServer(t, dataDir)
	got = getEvents(t, third, "")
	if len(got) != 2 || got[0]["id"] != "good" || got[1]["id"] != "after" {
		t.Fatalf("trimmed tail polluted later writes: %+v", got)
	}
	third.signal(t, syscall.SIGTERM)
	third.waitExit(t, 0)
}

func TestServeFailsOnMidCorruptionWithoutMutation(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postEvents(t, first, `{"events":[{"id":"good","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	path := filepath.Join(dataDir, "events.log")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	firstLen := 8 + int(binary.BigEndian.Uint32(original[0:4]))

	payload := []byte(`[{"id":"bad","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]`)
	bad := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(bad[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(bad[4:8], crc32.ChecksumIEEE(payload))
	copy(bad[8:], payload)
	bad[8] ^= 0xFF // checksum mismatch in a complete middle frame
	corrupt := append(original[:firstLen], bad...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	logs := &safeBuffer{}
	failing := exec.Command(binaryPath, "serve", "--addr", "127.0.0.1:0", "--data", dataDir)
	failing.Stdout = logs
	failing.Stderr = logs
	if err := failing.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- failing.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("corrupt log must prevent startup")
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("corrupt log must exit non-zero: %v", err)
		}
	case <-ctx.Done():
		failing.Process.Kill()
		t.Fatal("server stayed up despite corrupt log")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed startup must not modify the corrupt data")
	}
}

func TestServeHTTPErrorsAreJSON(t *testing.T) {
	p := startServer(t, t.TempDir())
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/events", `{"events":[]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d", status)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("error body must contain non-empty error: %s", raw)
	}
	status, raw = httpDo(t, http.MethodGet, "http://"+p.addr+"/events?since=bad", "")
	if status != http.StatusBadRequest {
		t.Fatalf("status %d", status)
	}
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("error body: %s", raw)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

// postSimpleEvent submits one minimal info event with the given id and time.
func postSimpleEvent(t *testing.T, p *serverProc, id, at string) {
	t.Helper()
	body := fmt.Sprintf(`{"events":[{"id":%q,"service":"svc","severity":"info","message":"m","at":%q}]}`, id, at)
	postEvents(t, p, body)
}

func TestServePageWalksAndPreservesLabels(t *testing.T) {
	p := startServer(t, t.TempDir())
	labeled := `{"events":[
		{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"e2","service":"api","severity":"info","message":"ok","at":"2026-10-01T10:00:00Z"},
		{"id":"e3","service":"gateway","severity":"info","message":"ok","at":"2026-10-01T11:00:00Z","labels":{"env":"prod"}}
	]}`
	postEvents(t, p, labeled)

	status, first := getPage(t, p, "?limit=2")
	if status != http.StatusOK || pageEventIDs(first) != "e1,e2" || first.NextCursor == nil {
		t.Fatalf("first page: %d %+v", status, first)
	}
	labels, ok := first.Events[0]["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" || labels["version"] != "v2" {
		t.Fatalf("full labels missing: %+v", first.Events[0])
	}
	if _, present := first.Events[1]["labels"]; present {
		t.Fatalf("label-less event must omit labels: %+v", first.Events[1])
	}

	status, last := getPage(t, p, "?cursor="+*first.NextCursor+"&limit=2")
	if status != http.StatusOK || pageEventIDs(last) != "e3" || last.NextCursor != nil {
		t.Fatalf("last page: %d %+v", status, last)
	}
}

func TestServePageCursorSurvivesKillWithoutLaterWrites(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	for i := range 5 {
		postSimpleEvent(t, p, fmt.Sprintf("e%d", i), fmt.Sprintf("2026-10-01T09:0%d:00Z", i))
	}

	// Freeze the whole set and read only the first page before the crash.
	status, first := getPage(t, p, "?limit=2")
	if status != http.StatusOK || pageEventIDs(first) != "e0,e1" {
		t.Fatalf("first page: %d %+v", status, first)
	}
	cursor := *first.NextCursor

	// Hard kill with no further writes: the cursor must remain valid.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, second := getPage(t, p2, "?cursor="+cursor+"&limit=2")
	if status != http.StatusOK || pageEventIDs(second) != "e2,e3" {
		t.Fatalf("resume after SIGKILL: %d %+v", status, second)
	}

	// Write something brand new, then kill again; the frozen walk must still
	// finish on e4 and never admit the new event.
	postSimpleEvent(t, p2, "later", "2026-10-01T08:00:00Z") // earlier time
	p2.signal(t, syscall.SIGKILL)
	p2.waitExit(t, -1)
	waitTCPPortClosed(t, p2.addr)

	p3 := startServer(t, dataDir)
	status, third := getPage(t, p3, "?cursor="+*second.NextCursor+"&limit=10")
	if status != http.StatusOK || pageEventIDs(third) != "e4" || third.NextCursor != nil {
		t.Fatalf("frozen walk after restart must ignore new writes: %d %+v", status, third)
	}
	// A new first page sees the current data, including historical events.
	if got := walkAllPages(t, p3, "?limit=2"); got != "later,e0,e1,e2,e3,e4" {
		t.Fatalf("fresh walk sees current set: %s", got)
	}
	p3.signal(t, syscall.SIGTERM)
	p3.waitExit(t, 0)
}

func TestServePageHistoricalDataUsableAfterUpgrade(t *testing.T) {
	// An existing data directory containing only an events.log (no cursor
	// key, as it would look before this feature existed) must work for
	// paging immediately, with historical events in the first page.
	dataDir := t.TempDir()
	payload := []byte(`[{"id":"old","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]`)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServer(t, dataDir)
	status, page := getPage(t, p, "?limit=10")
	if status != http.StatusOK || pageEventIDs(page) != "old" || page.NextCursor != nil {
		t.Fatalf("historical event must page without re-ingestion: %d %+v", status, page)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "cursor.key")); err != nil {
		t.Fatalf("cursor key must be created on first upgraded start: %v", err)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServePageCursorRejectedFromOtherDirectory(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	a := startServer(t, dirA)
	postSimpleEvent(t, a, "e0", "2026-10-01T09:00:00Z")
	postSimpleEvent(t, a, "e1", "2026-10-01T09:01:00Z")
	_, first := getPage(t, a, "?limit=1")
	cursor := *first.NextCursor

	b := startServer(t, dirB)
	postSimpleEvent(t, b, "e0", "2026-10-01T09:00:00Z")
	status, raw := httpDo(t, http.MethodGet, "http://"+b.addr+"/events/page?cursor="+cursor+"&limit=1", "")
	if status != http.StatusBadRequest {
		t.Fatalf("foreign cursor must be 400, got %d: %s", status, raw)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("foreign cursor error body: %s", raw)
	}
	a.signal(t, syscall.SIGTERM)
	a.waitExit(t, 0)
	b.signal(t, syscall.SIGTERM)
	b.waitExit(t, 0)
}

func TestServePageBadLimitAndCursorAre400(t *testing.T) {
	p := startServer(t, t.TempDir())
	postSimpleEvent(t, p, "e0", "2026-10-01T09:00:00Z")
	for _, query := range []string{
		"?limit=0",
		"?limit=1001",
		"?limit=x",
		"?limit=1&limit=2",
		"?cursor=",
		"?cursor=garbage",
		"?cursor=a.b.c",
	} {
		status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/events/page"+query, "")
		if status != http.StatusBadRequest {
			t.Fatalf("%s want 400, got %d: %s", query, status, raw)
		}
		var errBody map[string]string
		if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
			t.Fatalf("%s error body: %s", query, raw)
		}
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServePageFilterMismatchIs400(t *testing.T) {
	p := startServer(t, t.TempDir())
	postSimpleEvent(t, p, "e0", "2026-10-01T09:00:00Z")
	postSimpleEvent(t, p, "e1", "2026-10-01T09:01:00Z")

	status, first := getPage(t, p, "?service=svc&limit=1")
	if status != http.StatusOK || pageEventIDs(first) != "e0" {
		t.Fatalf("first page: %d %+v", status, first)
	}
	// Equivalent restatement is accepted (whitespace tolerated by rules).
	status, okPage := getPage(t, p, "?cursor="+*first.NextCursor+"&service=%20svc%20&limit=1")
	if status != http.StatusOK || pageEventIDs(okPage) != "e1" {
		t.Fatalf("equivalent filters must continue: %d %+v", status, okPage)
	}
	// A different filter is rejected.
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/events/page?cursor="+*first.NextCursor+"&service=other&limit=1", "")
	if status != http.StatusBadRequest {
		t.Fatalf("mismatched filters must be 400, got %d: %s", status, raw)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}
