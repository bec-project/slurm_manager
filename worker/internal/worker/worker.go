// Package worker runs uploaded Bash scripts and publishes their lifecycle to Redis.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

// Payload contains only the user scripts, never the submitter's credentials.
type Payload struct {
	Script     string `json:"script"`
	Activation string `json:"activation"`
}

// Config identifies one job and configures feedback intervals, termination grace,
// and local stdout/stderr destinations. Durations must be positive; nil writers discard output.
type Config struct {
	JobID             string
	HeartbeatInterval time.Duration
	GracePeriod       time.Duration
	Stdout            io.Writer
	Stderr            io.Writer
}

// Publisher sends a parsed message to one job topic. Implementations must bound
// blocking I/O and honor context cancellation.
type Publisher interface {
	Publish(context.Context, string, map[string]any) error
}

// RedisPublisher uses a serialized connection pool and bounded requests, without retries.
type RedisPublisher struct{ client *redis.Client }

// NewRedisPublisher configures a Redis host:port without opening a connection.
// The first Publish connects; requests have a three-second deadline and no retries.
func NewRedisPublisher(address string) *RedisPublisher {
	return &RedisPublisher{client: redis.NewClient(&redis.Options{
		Addr: address, Protocol: 2, MaxRetries: -1, PoolSize: 1,
		DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		ContextTimeoutEnabled: true, DisableIdentity: true,
	})}
}

// Close releases pooled connections and returns any Redis client close error.
func (p *RedisPublisher) Close() error { return p.client.Close() }

// Publish JSON-encodes message and sends it to topic within three seconds or the
// earlier context deadline. Encoding and Redis transport errors are returned.
func (p *RedisPublisher) Publish(ctx context.Context, topic string, message map[string]any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return p.client.Publish(ctx, topic, data).Err()
}

const shellSetup = `
if ! type module >/dev/null 2>&1; then
    for module_init in /etc/profile.d/modules.sh /usr/share/Modules/init/bash; do
        if [ -r "$module_init" ]; then
            source "$module_init" || exit $?
            break
        fi
    done
fi
source "$1" || exit $?
exec /bin/bash "$2"
`

// Run stages payload scripts privately, sources activation, and executes one workload.
// Config supplies the job identity, timing, and archive writers; publisher receives
// log, heartbeat, and terminal messages. Cancelling ctx terminates the process group.
// The returned code is the workload exit status, 143 for cancellation, or 1 for a
// worker/output/Redis failure. The error describes failures; staged files are removed.
func Run(ctx context.Context, config Config, payload Payload, publisher Publisher) (int, error) {
	if config.JobID == "" || config.HeartbeatInterval <= 0 || config.GracePeriod <= 0 {
		return 1, errors.New("job ID and positive timing values are required")
	}
	if config.Stdout == nil {
		config.Stdout = io.Discard
	}
	if config.Stderr == nil {
		config.Stderr = io.Discard
	}
	directory, err := os.MkdirTemp("", "bec-slurm-worker-")
	if err != nil {
		return 1, err
	}
	defer os.RemoveAll(directory)
	activation := filepath.Join(directory, "activate.sh")
	script := filepath.Join(directory, "workload.sh")
	if err := os.WriteFile(activation, []byte(payload.Activation), 0600); err != nil {
		return 1, err
	}
	if err := os.WriteFile(script, []byte(payload.Script), 0600); err != nil {
		return 1, err
	}
	home, err := resolveHome()
	if err != nil {
		return 1, err
	}

	workContext, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	var publishLock sync.Mutex
	var publishError error
	publish := func(kind string, message map[string]any) error {
		publishLock.Lock()
		defer publishLock.Unlock()
		// Once feedback fails, drain buffered output without repeated network waits.
		// Allow one final status attempt so a recovered connection can report failure.
		if publishError != nil && kind != "status" {
			return publishError
		}
		message["msg_type"] = kind
		topic := "info/" + config.JobID + "/" + kind
		err := publisher.Publish(context.Background(), topic, message)
		if err != nil && publishError == nil {
			publishError = err
			cancelWork()
		}
		return err
	}
	if err := publish("status", map[string]any{"status": "running"}); err != nil {
		return 1, fmt.Errorf("cannot publish job start: %w", err)
	}
	heartbeatDone := make(chan struct{})
	stopHeartbeat := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			if err := publish("heartbeat", map[string]any{"timestamp": float64(time.Now().UnixNano()) / 1e9}); err != nil {
				return
			}
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
			}
		}
	}()

	stdout := &lineWriter{local: config.Stdout, stream: "stdout", publish: publish, onError: cancelWork}
	stderr := &lineWriter{local: config.Stderr, stream: "stderr", publish: publish, onError: cancelWork}
	command := exec.Command("/bin/bash", "-c", shellSetup, "bec-slurm-worker", activation, script)
	command.Env = append(os.Environ(), "HOME="+home)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = config.GracePeriod
	var runError error
	if workContext.Err() == nil {
		runError = command.Start()
		if runError == nil {
			processDone := make(chan struct{})
			watcherDone := make(chan struct{})
			go func() {
				defer close(watcherDone)
				select {
				case <-workContext.Done():
					terminateProcessGroup(command.Process.Pid, config.GracePeriod)
				case <-processDone:
					if workContext.Err() != nil {
						terminateProcessGroup(command.Process.Pid, config.GracePeriod)
						return
					}
					_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				}
			}()
			runError = command.Wait()
			close(processDone)
			<-watcherDone
		}
	} else {
		runError = workContext.Err()
	}
	stdout.flush()
	stderr.flush()
	close(stopHeartbeat)
	<-heartbeatDone

	code, status := 0, "finished"
	if ctx.Err() != nil {
		code, status = 143, "cancelled"
	} else if runError != nil {
		code = 1
		var exitError *exec.ExitError
		if errors.As(runError, &exitError) && exitError.ExitCode() > 0 {
			code = exitError.ExitCode()
		}
		status = fmt.Sprintf("error:workload exited with code %d", code)
	}
	outputError := errors.Join(stdout.err, stderr.err)
	if outputError != nil {
		code, status = 1, "error:Slurm output write failed"
	}
	publishLock.Lock()
	failure := publishError
	publishLock.Unlock()
	if failure != nil {
		code, status = 1, "error:Redis feedback failed"
	}
	terminalError := publish("status", map[string]any{"status": status})
	if failure != nil {
		return code, fmt.Errorf("Redis feedback failed: %w", failure)
	}
	if terminalError != nil {
		return 1, fmt.Errorf("cannot publish terminal status: %w", terminalError)
	}
	return code, errors.Join(runError, outputError)
}

// terminateProcessGroup gives surviving descendants their grace period even when
// the leader has exited. Return early only when the entire process group is gone.
func terminateProcessGroup(pid int, grace time.Duration) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH) {
			return
		}
		select {
		case <-deadline.C:
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return
		case <-poll.C:
		}
	}
}

// resolveHome uses the node's NSS lookup for site accounts (LDAP/SSSD).
// A static Go binary cannot rely on user.Current or libc's user lookup.
func resolveHome() (string, error) {
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}
	uid := strconv.Itoa(os.Getuid())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "getent", "passwd", uid)
	command.WaitDelay = time.Second
	if output, err := command.Output(); err == nil {
		fields := strings.Split(strings.TrimSpace(string(output)), ":")
		if len(fields) == 7 && fields[2] == uid && filepath.IsAbs(fields[5]) {
			return fields[5], nil
		}
	}
	// Local accounts still work on nodes without getent.
	if account, err := user.LookupId(uid); err == nil && filepath.IsAbs(account.HomeDir) {
		return account.HomeDir, nil
	}
	return "", errors.New("cannot determine compute-node HOME: getent passwd and local account lookup failed")
}

// lineWriter preserves unterminated lines and bounds buffering for very long lines.
type lineWriter struct {
	local   io.Writer
	stream  string
	buffer  []byte
	publish func(string, map[string]any) error
	onError func()
	err     error
}

const maxChunk = 64 * 1024

func (w *lineWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	written, err := w.local.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = fmt.Errorf("%s archive: %w", w.stream, err)
		if w.onError != nil {
			w.onError()
		}
		return written, w.err
	}
	size := len(data)
	for len(data) > 0 {
		count := min(len(data), maxChunk-len(w.buffer))
		w.buffer = append(w.buffer, data[:count]...)
		data = data[count:]
		for {
			newline := bytes.IndexByte(w.buffer, '\n')
			if newline < 0 {
				break
			}
			w.send(w.buffer[:newline])
			w.buffer = w.buffer[newline+1:]
		}
		if len(w.buffer) == maxChunk {
			// Keep an incomplete UTF-8 suffix for the next chunk instead of
			// letting JSON replace each half with a replacement character.
			end := len(w.buffer)
			for start := end - utf8.UTFMax; start < end; start++ {
				if utf8.RuneStart(w.buffer[start]) && !utf8.FullRune(w.buffer[start:]) {
					end = start
					break
				}
			}
			w.send(w.buffer[:end])
			w.buffer = w.buffer[end:]
		}
	}
	return size, nil
}
func (w *lineWriter) send(data []byte) {
	_ = w.publish("log", map[string]any{"log": string(data), "metadata": map[string]string{"stream": w.stream}})
}
func (w *lineWriter) flush() {
	if len(w.buffer) > 0 {
		w.send(w.buffer)
		w.buffer = nil
	}
}
