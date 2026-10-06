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

func TestServeFailsOnConflictWithoutMutation(t *testing.T) {
	dataDir := t.TempDir()

	// Two complete batches carry the same event id with different content,
	// and a torn frame trails them. Startup must fail before the tail is
	// trimmed.
	frame := func(payload string) []byte {
		raw := []byte(payload)
		out := make([]byte, 8+len(raw))
		binary.BigEndian.PutUint32(out[0:4], uint32(len(raw)))
		binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(raw))
		copy(out[8:], raw)
		return out
	}
	first := frame(`[{"id":"e1","service":"svc","severity":"info","message":"first","at":"2026-10-01T09:00:00Z"}]`)
	second := frame(`[{"id":"e1","service":"svc","severity":"info","message":"second","at":"2026-10-01T09:00:00Z"}]`)
	tornPayload := []byte(`[{"id":"torn","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]`)
	torn := frame(string(tornPayload))[:8+4]
	content := append(append(append([]byte(nil), first...), second...), torn...)
	path := filepath.Join(dataDir, "events.log")
	if err := os.WriteFile(path, content, 0o644); err != nil {
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
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("conflicting log must prevent startup")
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("conflicting log must exit non-zero: %v", err)
		}
	case <-time.After(5 * time.Second):
		failing.Process.Kill()
		t.Fatal("server stayed up despite conflicting records")
	}

	out := logs.String()
	if !strings.Contains(out, `"e1"`) || !strings.Contains(out, "different content") {
		t.Fatalf("error must name the conflicting id and the content clash:\n%s", out)
	}
	if strings.Contains(out, "listening on") {
		t.Fatalf("failed startup must not announce readiness:\n%s", out)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, content) {
		t.Fatal("failed startup must not modify events.log, torn tail included")
	}
}

func TestServeRecoversIdenticalDuplicateRecords(t *testing.T) {
	dataDir := t.TempDir()

	// The same event recorded in two complete batches (e.g. a confirmed
	// batch retried before a crash) recovers as a single event.
	frame := func(payload string) []byte {
		raw := []byte(payload)
		out := make([]byte, 8+len(raw))
		binary.BigEndian.PutUint32(out[0:4], uint32(len(raw)))
		binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(raw))
		copy(out[8:], raw)
		return out
	}
	event := `{"id":"e1","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}`
	content := append(frame(`[`+event+`]`), frame(`[`+event+`]`)...)
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServer(t, dataDir)
	got := getEvents(t, p, "")
	if len(got) != 1 || got[0]["id"] != "e1" {
		t.Fatalf("identical duplicates must recover as one event: %+v", got)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
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

// pageResponse is the decoded body of GET /events/page.
type pageResponse struct {
	Events     []map[string]any `json:"events"`
	NextCursor *string          `json:"next_cursor"`
}

func pageGet(t *testing.T, p *serverProc, query string) (int, pageResponse) {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/events/page"+query, "")
	var out pageResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return status, out
}

func TestServePageSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"},
		{"id":"e3","service":"s","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"}
	]}`)

	// First page.
	status, out := pageGet(t, p, "?limit=1")
	if status != http.StatusOK {
		t.Fatalf("page: %d", status)
	}
	if len(out.Events) != 1 || out.Events[0]["id"] != "e1" {
		t.Fatalf("first page: %v", out.Events)
	}
	if out.NextCursor == nil {
		t.Fatal("first page should have a next_cursor")
	}

	// Hard kill and restart: the cursor must remain usable.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, out = pageGet(t, p2, "?limit=1&cursor="+*out.NextCursor)
	if status != http.StatusOK {
		t.Fatalf("page after restart: %d", status)
	}
	if len(out.Events) != 1 || out.Events[0]["id"] != "e2" {
		t.Fatalf("page after restart: %v", out.Events)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServePageCursorRejectedByOtherDir(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	p1 := startServer(t, dir1)
	p2 := startServer(t, dir2)
	postEvents(t, p1, `{"events":[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}
	]}`)

	_, out := pageGet(t, p1, "?limit=1")
	if out.NextCursor == nil {
		t.Fatal("expected a next_cursor")
	}

	// dir1's cursor must be rejected by dir2.
	status, raw := httpDo(t, http.MethodGet, "http://"+p2.addr+"/events/page?limit=1&cursor="+*out.NextCursor, "")
	if status != http.StatusBadRequest {
		t.Fatalf("cursor from another dir should be 400, got %d %s", status, raw)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("error body: %s", raw)
	}
	p1.signal(t, syscall.SIGTERM)
	p1.waitExit(t, 0)
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServePageWorksOnLegacyDataDir(t *testing.T) {
	// A data directory written by an older version has events.log but no
	// cursor.key or snapshots/. The new server must generate the key and
	// page through the recovered events.
	dataDir := t.TempDir()
	payload := []byte(`[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]`)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServer(t, dataDir)
	status, out := pageGet(t, p, "?limit=1")
	if status != http.StatusOK {
		t.Fatalf("page on legacy dir: %d", status)
	}
	if len(out.Events) != 1 || out.Events[0]["id"] != "e1" {
		t.Fatalf("legacy dir first page: %v", out.Events)
	}
	if out.NextCursor == nil {
		t.Fatal("expected a next_cursor")
	}
	status, out = pageGet(t, p, "?limit=1&cursor="+*out.NextCursor)
	if status != http.StatusOK {
		t.Fatalf("legacy dir second page: %d", status)
	}
	if len(out.Events) != 1 || out.Events[0]["id"] != "e2" {
		t.Fatalf("legacy dir second page: %v", out.Events)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServePreviewStableAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[
		{"id":"e1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"prod"}},
		{"id":"e2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:05Z","labels":{"env":"prod"}},
		{"id":"e3","service":"api","severity":"info","message":"m","at":"2026-10-01T10:00:05Z"}
	]}`)

	body := `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:03:00Z",` +
		`"window_seconds":60,"threshold":1,"trigger_windows":2,"recover_windows":1,` +
		`"group_labels":["env"]}`
	_, before := httpDo(t, http.MethodPost, "http://"+p.addr+"/alerts/preview", body)

	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, after := httpDo(t, http.MethodPost, "http://"+p2.addr+"/alerts/preview", body)
	if status != http.StatusOK {
		t.Fatalf("preview after restart: %d %s", status, after)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("preview differs across restart:\n%s\n%s", before, after)
	}

	// Replaying an identical batch after restart leaves the preview unchanged.
	postEvents(t, p2, `{"events":[`+
		`{"id":"e1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"prod"}},`+
		`{"id":"e2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:05Z","labels":{"env":"prod"}},`+
		`{"id":"e3","service":"api","severity":"info","message":"m","at":"2026-10-01T10:00:05Z"}]}`)
	_, replayed := httpDo(t, http.MethodPost, "http://"+p2.addr+"/alerts/preview", body)
	if !bytes.Equal(before, replayed) {
		t.Fatalf("replaying events changed the preview:\n%s\n%s", before, replayed)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServePageSnapshotStableAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"}
	]}`)

	// First page, limit 1.
	_, out := pageGet(t, p, "?limit=1")
	if out.NextCursor == nil {
		t.Fatal("expected a next_cursor")
	}

	// Restart, then ingest a new event with an earlier time.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)
	p2 := startServer(t, dataDir)
	postEvents(t, p2, `{"events":[{"id":"e0","service":"s","severity":"info","message":"m","at":"2026-10-01T08:00:00Z"}]}`)

	// The old cursor must continue the old snapshot, not see e0.
	_, out = pageGet(t, p2, "?limit=1&cursor="+*out.NextCursor)
	if len(out.Events) != 1 || out.Events[0]["id"] != "e2" {
		t.Fatalf("old cursor must not see post-restart event: %v", out.Events)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}
