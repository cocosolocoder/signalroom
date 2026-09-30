package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// repoRoot walks up from the test source dir to the module root.
func repoRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("could not find repo root")
		}
		dir = parent
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "signalroom")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/signalroom")
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	return bin
}

type serverProc struct {
	cmd    *exec.Cmd
	stdout *bufio.Reader
}

func startServer(t *testing.T, bin, dir string) *serverProc {
	t.Helper()
	cmd := exec.Command(bin, "serve", "--addr", "127.0.0.1:0", "--data", dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return &serverProc{cmd: cmd, stdout: bufio.NewReader(stdout)}
}

// waitForAddr reads stdout until the listening line appears.
func (s *serverProc) waitForAddr(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := s.stdout.ReadString('\n')
		if err != nil {
			t.Fatalf("read stdout: %v", err)
		}
		if strings.HasPrefix(line, "signalroom listening on ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "signalroom listening on "))
		}
	}
	t.Fatal("timed out waiting for listening address")
	return ""
}

func (s *serverProc) kill(t *testing.T) {
	t.Helper()
	if err := s.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	s.cmd.Wait()
}

func (s *serverProc) terminate(t *testing.T) int {
	s.cmd.Process.Signal(syscall.SIGTERM)
	err := s.cmd.Wait()
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	return -1
}

func postEvent(t *testing.T, addr, body string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := http.Post("http://"+addr+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return resp.StatusCode, result
}

func getEvents(t *testing.T, addr string) (int, []map[string]interface{}) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return resp.StatusCode, result
}

func TestServeLifecycle(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()
	body := `{"events":[{"id":"evt-1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T10:00:00Z"}]}`

	// Start, post, kill -9.
	s1 := startServer(t, bin, dir)
	addr1 := s1.waitForAddr(t)
	status, _ := postEvent(t, addr1, body)
	if status != http.StatusOK {
		t.Fatalf("post status=%d", status)
	}
	s1.kill(t)

	// Restart: events survived.
	s2 := startServer(t, bin, dir)
	addr2 := s2.waitForAddr(t)
	_, events := getEvents(t, addr2)
	if len(events) != 1 || events[0]["id"] != "evt-1" {
		t.Fatalf("events did not survive restart: %v", events)
	}

	// Replay after restart is idempotent.
	status, result := postEvent(t, addr2, body)
	if status != http.StatusOK || result["replayed"] != float64(1) {
		t.Fatalf("replay after restart: status=%d result=%v", status, result)
	}

	// Second instance on the same data dir fails without changing data.
	s3 := startServer(t, bin, dir)
	if err := s3.cmd.Wait(); err == nil {
		t.Fatal("second instance should fail to start")
	}
	// s3 did not print a listening line; its stdout was consumed by Wait.

	// Graceful shutdown exits 0 and data survives another restart.
	if code := s2.terminate(t); code != 0 {
		t.Fatalf("graceful shutdown exit code=%d, want 0", code)
	}

	s4 := startServer(t, bin, dir)
	addr4 := s4.waitForAddr(t)
	_, events = getEvents(t, addr4)
	if len(events) != 1 {
		t.Fatalf("events did not survive graceful restart: %v", events)
	}
	s4.kill(t)
}

func TestDemo(t *testing.T) {
	bin := buildBinary(t)
	out, err := exec.Command(bin, "demo").CombinedOutput()
	if err != nil {
		t.Fatalf("demo failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Signalroom") || !strings.Contains(string(out), "当前事件时间线") {
		t.Fatalf("unexpected demo output: %s", out)
	}
}

func TestUsageErrors(t *testing.T) {
	bin := buildBinary(t)
	for _, args := range [][]string{
		{},
		{"serve"},
		{"serve", "--addr", ":8080"},
		{"serve", "--data", t.TempDir()},
		{"unknown"},
	} {
		cmd := exec.Command(bin, args...)
		if err := cmd.Run(); err == nil {
			t.Fatalf("expected failure for args %v", args)
		}
	}
}
