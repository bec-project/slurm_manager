package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/alicebob/miniredis/v2"
	redisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

type recorded struct {
	topic   string
	message map[string]any
}
type fakePublisher struct {
	mu       sync.Mutex
	events   []recorded
	failKind string
}

func (p *fakePublisher) Publish(_ context.Context, topic string, message map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, recorded{topic, message})
	if message["msg_type"] == p.failKind {
		return errors.New("Redis unavailable")
	}
	return nil
}
func config(out, errout *bytes.Buffer) Config {
	return Config{JobID: "test-job", HeartbeatInterval: 20 * time.Millisecond, GracePeriod: 100 * time.Millisecond, Stdout: out, Stderr: errout}
}
func lastStatus(t *testing.T, p *fakePublisher) string {
	t.Helper()
	if len(p.events) == 0 {
		t.Fatal("no events")
	}
	last := p.events[len(p.events)-1]
	if last.topic != "info/test-job/status" {
		t.Fatal(last.topic)
	}
	return last.message["status"].(string)
}
func TestScriptsOutputAndCleanup(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(string(rune('0'+code)), func(t *testing.T) {
			temp := t.TempDir()
			t.Setenv("TMPDIR", temp)
			t.Setenv("HOME", "")
			p := &fakePublisher{}
			var out, errout bytes.Buffer
			script := "printf 'stage=%s\\n' \"$0\"\nprintf 'value=%s\\n' \"$TEST_VALUE\"\nprintf 'error line\\n' >&2\nprintf 'no newline'\n"
			if code != 0 {
				script += "exit 7\n"
			}
			actual, err := Run(context.Background(), config(&out, &errout), Payload{Script: script, Activation: "export TEST_VALUE='literal $value and spaces'\ntest -n \"$HOME\"\n"}, p)
			if actual != code {
				t.Fatalf("exit=%d err=%v", actual, err)
			}
			if code == 0 && err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "value=literal $value and spaces") || errout.String() != "error line\n" {
				t.Fatalf("%s %s", out.String(), errout.String())
			}
			entries, err := os.ReadDir(temp)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging leaked: %v %v", entries, err)
			}
			logs := map[string]bool{}
			for _, e := range p.events {
				if e.message["msg_type"] == "log" {
					logs[e.message["log"].(string)] = true
				}
			}
			if !logs["no newline"] || !logs["error line"] {
				t.Fatalf("missing logs %v", logs)
			}
			status := lastStatus(t, p)
			if code == 0 && status != "finished" {
				t.Fatal(status)
			}
			if code != 0 && !strings.HasPrefix(status, "error:") {
				t.Fatal(status)
			}
		})
	}
}
func TestHeartbeatsWhileRunning(t *testing.T) {
	var out, errout bytes.Buffer
	p := &fakePublisher{}
	code, err := Run(context.Background(), config(&out, &errout), Payload{Script: "sleep 0.15\n"}, p)
	if code != 0 || err != nil {
		t.Fatalf("%d %v", code, err)
	}
	count := 0
	for _, e := range p.events {
		if e.message["msg_type"] == "heartbeat" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("only %d heartbeats", count)
	}
	if lastStatus(t, p) != "finished" {
		t.Fatal("missing terminal status")
	}
}
func TestCancellationKillsTermIgnoringWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var out, errout bytes.Buffer
	p := &fakePublisher{}
	start := time.Now()
	code, _ := Run(ctx, config(&out, &errout), Payload{Script: "trap '' TERM\nwhile :; do sleep 0.05; done\n"}, p)
	if code != 143 || lastStatus(t, p) != "cancelled" {
		t.Fatalf("exit %d", code)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("cancellation did not stop workload")
	}
}
func TestActivationFailureIsReported(t *testing.T) {
	var out, errout bytes.Buffer
	p := &fakePublisher{}
	code, _ := Run(context.Background(), config(&out, &errout), Payload{Activation: "return 9\n", Script: "echo should-not-run\n"}, p)
	if code != 9 || out.Len() != 0 {
		t.Fatalf("%d %s", code, out.String())
	}
	if lastStatus(t, p) != "error:workload exited with code 9" {
		t.Fatal("wrong status")
	}
}
func TestInitialRedisFailureDoesNotRunWorkload(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "executed")
	var out, errout bytes.Buffer
	p := &fakePublisher{failKind: "status"}
	code, err := Run(context.Background(), config(&out, &errout), Payload{Script: "touch " + marker + "\n"}, p)
	if code != 1 || err == nil {
		t.Fatalf("%d %v", code, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("workload ran without Redis")
	}
}
func TestLogFailureStopsWorkload(t *testing.T) {
	var out, errout bytes.Buffer
	p := &fakePublisher{failKind: "log"}
	start := time.Now()
	code, err := Run(context.Background(), config(&out, &errout), Payload{Script: "echo hello\nsleep 30\n"}, p)
	if code != 1 || err == nil {
		t.Fatalf("%d %v", code, err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("workload continued after feedback failed")
	}
	if lastStatus(t, p) != "error:Redis feedback failed" {
		t.Fatal("wrong status")
	}
}
func TestLongLineIsBoundedAndComplete(t *testing.T) {
	var output bytes.Buffer
	var logs []string
	w := &lineWriter{local: &output, stream: "stdout", publish: func(_ string, m map[string]any) error { logs = append(logs, m["log"].(string)); return nil }}
	input := strings.Repeat("a", maxChunk*3+7)
	_, _ = w.Write([]byte(input))
	w.flush()
	if strings.Join(logs, "") != input || output.String() != input {
		t.Fatal("long output lost")
	}
}

func TestRedisWireFeedbackWithoutPing(t *testing.T) {
	server := miniredis.RunT(t)
	// Production ACLs need only pub/sub, not PING or key inspection.
	server.Server().SetPreHook(func(peer *redisserver.Peer, command string, _ ...string) bool {
		if strings.EqualFold(command, "ping") {
			peer.WriteError("NOPERM PING disabled")
			return true
		}
		return false
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subscriber := redis.NewClient(&redis.Options{Addr: server.Addr(), Protocol: 2, DisableIdentity: true, MaxRetries: -1})
	defer subscriber.Close()
	subscription := subscriber.Subscribe(ctx, "info/test-job/log", "info/test-job/status", "info/test-job/heartbeat")
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	publisher := NewRedisPublisher(server.Addr())
	defer publisher.Close()
	var out, errout bytes.Buffer
	code, err := Run(ctx, config(&out, &errout), Payload{Script: "printf 'hello Redis\n'; printf 'stderr tail' >&2\n"}, publisher)
	if code != 0 || err != nil {
		t.Fatalf("%d %v", code, err)
	}
	logs := map[string]bool{}
	heartbeat := false
	for {
		message, err := subscription.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(message.Payload), &body); err != nil {
			t.Fatal(err)
		}
		switch body["msg_type"] {
		case "log":
			logs[body["log"].(string)] = true
		case "heartbeat":
			heartbeat = true
		case "status":
			if body["status"] == "finished" {
				if !logs["hello Redis"] || !logs["stderr tail"] || !heartbeat {
					t.Fatalf("logs %v heartbeat %v", logs, heartbeat)
				}
				return
			}
		}
	}
}

func TestResolveHomeUsesNodeAccountLookupWithoutUserEnvironment(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USER", "")
	t.Setenv("LOGNAME", "")
	directory := t.TempDir()
	program := "#!/bin/sh\n[ \"$1\" = passwd ] || exit 1\n[ \"$2\" = " + strconv.Itoa(os.Getuid()) + " ] || exit 1\nprintf 'siteuser:x:" + strconv.Itoa(os.Getuid()) + ":1000::/das/home/siteuser:/bin/bash\\n'\n"
	if err := os.WriteFile(filepath.Join(directory, "getent"), []byte(program), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	home, err := resolveHome()
	if err != nil || home != "/das/home/siteuser" {
		t.Fatalf("HOME=%q err=%v", home, err)
	}
}

func TestResolveHomePreservesProvidedHome(t *testing.T) {
	t.Setenv("HOME", "/remote/already-set")
	t.Setenv("PATH", t.TempDir())
	home, err := resolveHome()
	if err != nil || home != "/remote/already-set" {
		t.Fatalf("HOME=%q err=%v", home, err)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(data []byte) (int, error) { return f(data) }

type slowFailPublisher struct{ fakePublisher }

func (p *slowFailPublisher) Publish(ctx context.Context, topic string, message map[string]any) error {
	if message["msg_type"] == "log" {
		time.Sleep(5 * time.Millisecond)
	}
	return p.fakePublisher.Publish(ctx, topic, message)
}

func TestRedisFailureSkipsRemainingBufferedPublications(t *testing.T) {
	var out, errout bytes.Buffer
	publisher := &slowFailPublisher{fakePublisher: fakePublisher{failKind: "log"}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := Run(ctx, config(&out, &errout), Payload{Script: "printf 'line\\n%.0s' {1..200}\n"}, publisher)
	if code != 1 || err == nil {
		t.Fatalf("code=%d err=%v", code, err)
	}
	attempts := 0
	for _, event := range publisher.events {
		if event.message["msg_type"] == "log" {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatalf("made %d network publications after feedback failed", attempts)
	}
	if lastStatus(t, &publisher.fakePublisher) != "error:Redis feedback failed" {
		t.Fatal("missing terminal failure")
	}
}

func TestArchiveWriteFailuresFailTheJob(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(strconv.FormatBool(short), func(t *testing.T) {
			var out, errout bytes.Buffer
			cfg := config(&out, &errout)
			cfg.Stdout = writerFunc(func(data []byte) (int, error) {
				if short {
					return len(data) - 1, nil
				}
				return 0, io.ErrClosedPipe
			})
			publisher := &fakePublisher{}
			code, err := Run(context.Background(), cfg, Payload{Script: "printf 'archival result\\n'\n"}, publisher)
			expected := io.ErrClosedPipe
			if short {
				expected = io.ErrShortWrite
			}
			if code != 1 || !errors.Is(err, expected) {
				t.Fatalf("code=%d err=%v", code, err)
			}
			if lastStatus(t, publisher) != "error:Slurm output write failed" {
				t.Fatal("archive failure reported as success")
			}
		})
	}
}

func TestUnicodeAcrossChunkBoundariesSurvivesJSON(t *testing.T) {
	for _, value := range []string{"é", "€", "🧪"} {
		for prefix := maxChunk - 3; prefix < maxChunk; prefix++ {
			input := strings.Repeat("a", prefix) + value + strings.Repeat("b", maxChunk)
			var out bytes.Buffer
			var logs []string
			w := &lineWriter{local: &out, stream: "stdout", publish: func(_ string, message map[string]any) error {
				encoded, err := json.Marshal(message)
				if err != nil {
					return err
				}
				var decoded map[string]any
				if err := json.Unmarshal(encoded, &decoded); err != nil {
					return err
				}
				text := decoded["log"].(string)
				if !utf8.ValidString(message["log"].(string)) {
					t.Fatal("split UTF-8 rune")
				}
				logs = append(logs, text)
				return nil
			}}
			if _, err := w.Write([]byte(input)); err != nil {
				t.Fatal(err)
			}
			w.flush()
			if strings.Join(logs, "") != input || out.String() != input {
				t.Fatalf("corrupted %q at prefix %d", value, prefix)
			}
		}
	}
}
