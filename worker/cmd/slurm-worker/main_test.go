package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Exercise main in a subprocess because the executable returns the workload's exit code.
func TestCLIHelper(t *testing.T) {
	if os.Getenv("SLURM_WORKER_TEST_HELPER") != "1" {
		return
	}
	os.Args = []string{"slurm-worker", "--job-id", "cli-job", "--redis-address", os.Getenv("SLURM_WORKER_TEST_REDIS")}
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
	command.Env = append(os.Environ(), "SLURM_WORKER_TEST_HELPER=1", "SLURM_WORKER_TEST_REDIS="+server.Addr())
	command.Stdin = strings.NewReader(`{"script":"printf 'hello from CLI\\n'","activation":"export WORKLOAD_TEST=1"}`)
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
