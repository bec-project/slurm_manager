// bec-slurm-worker executes one uploaded Bash workload and forwards feedback to Redis.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bec-project/slurm_manager/worker/internal/worker"
)

func main() {
	address := flag.String("redis-address", "localhost:6379", "Redis host:port")
	redisTLS := flag.Bool("redis-tls", true, "Use TLS with certificate and hostname verification")
	jobID := flag.String("job-id", "", "Opaque manager job ID used for Redis topics")
	heartbeat := flag.Duration("heartbeat", time.Second, "Heartbeat interval")
	grace := flag.Duration("grace", 5*time.Second, "Time to terminate workload before killing it")
	flag.Parse()
	if *jobID == "" || *heartbeat <= 0 || *grace <= 0 || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "--job-id and positive timing values are required; payload is JSON on stdin")
		os.Exit(2)
	}
	var payload worker.Payload
	if err := json.NewDecoder(os.Stdin).Decode(&payload); err != nil {
		fmt.Fprintln(os.Stderr, "Cannot decode job payload:", err)
		os.Exit(2)
	}
	tlsConfig := redisTLSConfig(*redisTLS)
	publisher := worker.NewRedisPublisher(*address, tlsConfig)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	code, err := worker.Run(ctx, worker.Config{
		JobID: *jobID, HeartbeatInterval: *heartbeat, GracePeriod: *grace,
		Stdout: os.Stdout, Stderr: os.Stderr,
	}, payload, publisher)
	stop()
	_ = publisher.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bec-slurm-worker:", err)
	}
	os.Exit(code)
}

// redisTLSConfig verifies the server certificate using system trust.
func redisTLSConfig(enabled bool) *tls.Config {
	if !enabled {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}
