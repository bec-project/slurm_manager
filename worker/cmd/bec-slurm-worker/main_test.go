package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bec-project/slurm_manager/worker/internal/worker"
	"github.com/redis/go-redis/v9"
)

// Exercise main in a subprocess because the executable returns the workload's exit code.
func TestCLIHelper(t *testing.T) {
	if os.Getenv("SLURM_WORKER_TEST_HELPER") != "1" {
		return
	}
	os.Args = []string{"bec-slurm-worker", "--redis-tls=false", "--redis-address", os.Getenv("BEC_SLURM_WORKER_TEST_REDIS")}
	main()
}

func TestCLIExecutesUploadedPayload(t *testing.T) {
	server := miniredis.RunT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), Protocol: 2, DisableIdentity: true})
	defer client.Close()
	sub := client.Subscribe(ctx, "info/cli-job/log", "info/cli-job/status")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, executable, "-test.run=^TestCLIHelper$")
	command.Env = append(os.Environ(), "SLURM_WORKER_TEST_HELPER=1", "BEC_SLURM_WORKER_TEST_REDIS="+server.Addr())
	command.Stdin = strings.NewReader(`{"job_id":"cli-job","script":"test \"$SLURM_WORKER_JOBID\" = cli-job || exit 91; printf 'hello from CLI\\n'","activation":"export WORKLOAD_TEST=1"}`)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if string(output) != "hello from CLI\n" {
		t.Fatalf("stdout %q", output)
	}
	gotLog := false
	for {
		message, err := sub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(message.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["msg_type"] == "log" && payload["log"] == "hello from CLI" {
			gotLog = true
		}
		if payload["status"] == "finished" {
			if !gotLog {
				t.Fatal("terminal status arrived without stdout")
			}
			return
		}
	}
}

func TestCLIRejectsMissingJobID(t *testing.T) {
	for _, payload := range []string{
		`{"script":"true","activation":""}`,
		`{"job_id":"","script":"true","activation":""}`,
	} {
		t.Run(payload, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, executable, "-test.run=^TestCLIHelper$")
			command.Env = append(os.Environ(), "SLURM_WORKER_TEST_HELPER=1")
			command.Stdin = strings.NewReader(payload)
			output, err := command.CombinedOutput()
			exitError, ok := err.(*exec.ExitError)
			if !ok || exitError.ExitCode() != 2 {
				t.Fatalf("expected exit code 2, got %v: %s", err, output)
			}
			if !strings.Contains(string(output), "Job payload must contain a nonempty job_id") {
				t.Fatalf("unexpected validation error: %s", output)
			}
		})
	}
}

func TestRedisTLSConfig(t *testing.T) {
	config := redisTLSConfig(true)
	if config == nil || config.InsecureSkipVerify || config.MinVersion < tls.VersionTLS12 || config.RootCAs != nil || len(config.Certificates) != 0 {
		t.Fatalf("invalid system-trust config: %+v", config)
	}
	if config := redisTLSConfig(false); config != nil {
		t.Fatalf("plaintext config: %+v", config)
	}
}

func TestWorkerTLSFeedback(t *testing.T) {
	// httptest supplies a short-lived test certificate valid for 127.0.0.1.
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := certServer.TLS.Certificates[0]
	certServer.Close()
	server := miniredis.NewMiniRedis()
	if err := server.StartTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	// Trust the self-signed fixture only for this test; production uses system trust.
	roots := x509.NewCertPool()
	roots.AddCert(certServer.Certificate())
	config := redisTLSConfig(true)
	config.RootCAs = roots
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), TLSConfig: config, Protocol: 2, DisableIdentity: true})
	defer client.Close()
	sub := client.Subscribe(ctx, "info/cli-job/status")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	publisher := worker.NewRedisPublisher(server.Addr(), config)
	defer publisher.Close()
	code, err := worker.Run(ctx, worker.Config{JobID: "cli-job", HeartbeatInterval: time.Second, GracePeriod: time.Second}, worker.Payload{Script: "true"}, publisher)
	if code != 0 || err != nil {
		t.Fatalf("worker failed: %d %v", code, err)
	}
	for {
		message, err := sub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(message.Payload, `"status":"finished"`) {
			break
		}
	}
	// Neither an untrusted certificate nor a wrong hostname may be accepted.
	for _, config := range []*tls.Config{{MinVersion: tls.VersionTLS12}, {RootCAs: config.RootCAs, ServerName: "wrong.example", MinVersion: tls.VersionTLS12}} {
		publisher := worker.NewRedisPublisher(server.Addr(), config)
		err := publisher.Publish(ctx, "info/test/status", map[string]any{"status": "finished"})
		publisher.Close()
		if err == nil {
			t.Fatal("unverified TLS connection succeeded")
		}
	}
}
