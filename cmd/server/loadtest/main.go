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
	"sync"
	"sync/atomic"
	"time"
)

type queryRequest struct {
	Tenant string `json:"tenant"`
	SQL    string `json:"sql"`
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	if p <= 0 {
		return latencies[0]
	}
	if p >= 100 {
		return latencies[len(latencies)-1]
	}
	idx := int((p / 100.0) * float64(len(latencies)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	return latencies[idx]
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/api/query", "Target query endpoint URL")
	auth := flag.String("auth", "", "Bearer token (optional)")
	tenant := flag.String("tenant", "default", "Tenant")
	sqlText := flag.String("sql", "SELECT 1", "SQL query to execute")
	total := flag.Int("requests", 1000, "Total number of requests")
	concurrency := flag.Int("concurrency", 20, "Concurrent workers")
	timeout := flag.Duration("timeout", 5*time.Second, "HTTP client timeout")
	flag.Parse()

	if *total <= 0 {
		fmt.Fprintln(os.Stderr, "requests must be > 0")
		os.Exit(2)
	}
	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "concurrency must be > 0")
		os.Exit(2)
	}

	payload, err := json.Marshal(&queryRequest{Tenant: *tenant, SQL: *sqlText})
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal payload: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: *timeout}
	jobs := make(chan int, *concurrency)
	latCh := make(chan time.Duration, *total)

	var okCount atomic.Int64
	var errCount atomic.Int64
	var httpErrCount atomic.Int64

	worker := func() {
		for range jobs {
			start := time.Now()
			req, reqErr := http.NewRequest(http.MethodPost, *url, bytes.NewReader(payload))
			if reqErr != nil {
				errCount.Add(1)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if *auth != "" {
				req.Header.Set("Authorization", "Bearer "+*auth)
			}

			resp, doErr := client.Do(req)
			if doErr != nil {
				errCount.Add(1)
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			lat := time.Since(start)
			latCh <- lat

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				okCount.Add(1)
			} else {
				httpErrCount.Add(1)
			}
		}
	}

	startAll := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	for i := 0; i < *total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(latCh)
	totalDur := time.Since(startAll)

	latencies := make([]time.Duration, 0, len(latCh))
	for d := range latCh {
		latencies = append(latencies, d)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	avg := time.Duration(0)
	if len(latencies) > 0 {
		avg = sum / time.Duration(len(latencies))
	}

	rps := float64(*total) / totalDur.Seconds()

	fmt.Printf("target=%s\n", *url)
	fmt.Printf("requests=%d concurrency=%d\n", *total, *concurrency)
	fmt.Printf("ok=%d http_errors=%d transport_errors=%d\n", okCount.Load(), httpErrCount.Load(), errCount.Load())
	fmt.Printf("total_time=%s rps=%.2f\n", totalDur, rps)
	fmt.Printf("latency avg=%s p50=%s p95=%s p99=%s max=%s\n",
		avg,
		percentile(latencies, 50),
		percentile(latencies, 95),
		percentile(latencies, 99),
		percentile(latencies, 100),
	)
}
