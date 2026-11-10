// Command taskforge-loadgen submits a weighted mix of demo jobs to a TaskForge
// API at a fixed rate. It drives the SLO burn demo in docs/SLO.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/loadgen"
)

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:8080", "TaskForge API base URL")
	rate := flag.Float64("rate", 2, "jobs submitted per second")
	duration := flag.Duration("duration", time.Minute, "how long to submit")
	mixSpec := flag.String("mix", "echo=70,sleep:200=20,fail=10", "weighted mix: echo, fail, sleep:<ms>, flaky:<n>")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "random seed for the mix")
	flag.Parse()

	mix, err := loadgen.ParseMix(*mixSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	random := rand.New(rand.NewPCG(*seed, *seed^0x9e3779b97f4a7c15))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	stats, err := loadgen.Run(ctx, client, *baseURL, *rate, mix, func() int { return random.IntN(mix.TotalWeight()) })
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("submitted=%d failed=%d seed=%d\n", stats.Submitted, stats.Failed, *seed)
}
