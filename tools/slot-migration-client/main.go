// A cluster GET client with per-phase and per-second HDR latency measurements.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/go-redis/v9"
)

type bucket struct {
	Lower int64 `json:"lower_us"`
	Upper int64 `json:"upper_us"`
	Count int64 `json:"count"`
}

type stats struct {
	hist                     *hdr.Histogram
	count, errors, badValues int64
	latencySum               int64
}

type summary struct {
	Count     int64    `json:"count"`
	Errors    int64    `json:"errors"`
	BadValues int64    `json:"bad_values"`
	P50       int64    `json:"p50_us"`
	P95       int64    `json:"p95_us"`
	P99       int64    `json:"p99_us"`
	P999      int64    `json:"p999_us"`
	Max       int64    `json:"max_us"`
	Buckets   []bucket `json:"buckets"`
}

type worker struct {
	mu       sync.Mutex
	phases   map[string]*stats
	interval *stats
}

func newStats() *stats { return &stats{hist: hdr.New(1, 60_000_000, 3)} }

func merge(destination, source *stats) {
	destination.hist.Merge(source.hist)
	destination.count += source.count
	destination.errors += source.errors
	destination.badValues += source.badValues
	destination.latencySum += source.latencySum
}

func summarize(s *stats) summary {
	r := summary{Count: s.count, Errors: s.errors, BadValues: s.badValues,
		P50: s.hist.ValueAtQuantile(50), P95: s.hist.ValueAtQuantile(95),
		P99: s.hist.ValueAtQuantile(99), P999: s.hist.ValueAtQuantile(99.9), Max: s.hist.Max()}
	for _, bar := range s.hist.Distribution() {
		if bar.Count != 0 {
			r.Buckets = append(r.Buckets, bucket{bar.From, bar.To, bar.Count})
		}
	}
	return r
}

func main() {
	seed := flag.String("seed", "127.0.0.1:6380", "cluster seed address")
	prefix := flag.String("prefix", "probe:", "resident hash-tag key prefix")
	keys := flag.Int("keys", 16384, "number of resident keys, suffixes 0..keys-1")
	valueSize := flag.Int("value-size", 1024, "expected value size")
	rate := flag.Int("rate", 50000, "offered requests per second, global across connections")
	connections := flag.Int("connections", 10, "concurrent GET workers, pipeline depth one")
	controlPort := flag.Int("control-port", 17380, "local phase control HTTP port")
	output := flag.String("output", "client.json", "final JSON statistics")
	intervalPath := flag.String("intervals", "client-intervals.jsonl", "one-second HDR log")
	flag.Parse()
	runtime.GOMAXPROCS(1)
	if *rate <= 0 || *keys <= 0 {
		panic("positive rate and key count required")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{*seed}, Protocol: 2, PoolSize: *connections, MaxRedirects: 3,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, DialTimeout: 5 * time.Second,
		DisableIdentity: true,
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	allKeys := make([]string, *keys)
	for i := range allKeys {
		allKeys[i] = *prefix + strconv.Itoa(i)
	}
	expectedValue := bytes.Repeat([]byte("x"), *valueSize)
	for _, key := range []string{allKeys[0], allKeys[len(allKeys)-1]} {
		value, err := client.Get(ctx, key).Bytes()
		if err != nil || !bytes.Equal(value, expectedValue) {
			panic(fmt.Sprintf("resident key verification: %s %v", key, err))
		}
	}
	var phase atomic.Value
	phase.Store("startup")
	var stop atomic.Bool
	var wg sync.WaitGroup
	workers := make([]*worker, *connections)
	var phaseMu sync.Mutex
	phaseTimes := map[string][2]float64{}
	setPhase := func(name string) float64 {
		phaseMu.Lock()
		defer phaseMu.Unlock()
		now := float64(time.Now().UnixNano()) / 1e9
		old := phase.Load().(string)
		if pair, ok := phaseTimes[old]; ok {
			pair[1] = now
			phaseTimes[old] = pair
		}
		phase.Store(name)
		phaseTimes[name] = [2]float64{now, 0}
		return now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/phase", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "phase name required", 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"time": setPhase(name), "phase": name})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ready") })
	server := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", *controlPort), Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	// A shared one-millisecond pacing window caps the offered load. Clients remain
	// closed-loop at pipeline one; missed capacity is recorded as lower achieved QPS.
	var paceMu sync.Mutex
	paceStart := time.Now()
	var window int64 = -1
	var remaining int64
	nextPermit := func() {
		for {
			paceMu.Lock()
			now := time.Now()
			tick := int64(now.Sub(paceStart) / time.Millisecond)
			if tick != window {
				window = tick
				remaining = (tick+1)*int64(*rate)/1000 - tick*int64(*rate)/1000
			}
			if remaining > 0 {
				remaining--
				paceMu.Unlock()
				return
			}
			wait := time.Millisecond - time.Duration(now.Sub(paceStart)%time.Millisecond)
			paceMu.Unlock()
			time.Sleep(wait)
		}
	}
	for i := range workers {
		workerIndex := i
		workers[i] = &worker{phases: map[string]*stats{}, interval: newStats()}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := workers[workerIndex]
			rng := uint64(workerIndex+1) * 0x9e3779b97f4a7c15
			for !stop.Load() {
				nextPermit()
				if stop.Load() {
					return
				}
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				key := allKeys[rng%uint64(len(allKeys))]
				started := time.Now()
				value, err := client.Get(ctx, key).Bytes()
				latency := time.Since(started).Microseconds()
				if latency < 1 {
					latency = 1
				}
				name := phase.Load().(string)
				w.mu.Lock()
				s := w.phases[name]
				if s == nil {
					s = newStats()
					w.phases[name] = s
				}
				for _, target := range []*stats{s, w.interval} {
					target.count++
					target.latencySum += latency
					if err != nil {
						target.errors++
					} else if !bytes.Equal(value, expectedValue) {
						target.badValues++
					}
					if err := target.hist.RecordValue(latency); err != nil {
						panic(err)
					}
				}
				w.mu.Unlock()
			}
		}()
	}
	intervalFile, err := os.Create(*intervalPath)
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(intervalFile)
	ticker := time.NewTicker(time.Second)
	last := time.Now()
	collectInterval := func() {
		now := time.Now()
		aggregate := newStats()
		for _, w := range workers {
			w.mu.Lock()
			merge(aggregate, w.interval)
			w.interval = newStats()
			w.mu.Unlock()
		}
		encoder.Encode(map[string]any{"start": float64(last.UnixNano()) / 1e9, "end": float64(now.UnixNano()) / 1e9, "stats": summarize(aggregate)})
		last = now
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("READY cluster GET workload")
running:
	for {
		select {
		case <-ticker.C:
			collectInterval()
		case <-signals:
			break running
		}
	}
	stop.Store(true)
	wg.Wait()
	ticker.Stop()
	collectInterval()
	setPhase("stopped")
	combined := map[string]*stats{}
	for _, w := range workers {
		for name, s := range w.phases {
			if combined[name] == nil {
				combined[name] = newStats()
			}
			merge(combined[name], s)
		}
	}
	results := map[string]summary{}
	for name, s := range combined {
		results[name] = summarize(s)
	}
	data := map[string]any{"phases": results, "phase_times": phaseTimes, "rate": *rate, "connections": *connections,
		"pipeline": 1, "client": "go-redis/v9 v9.12.1", "hdr_significant_figures": 3}
	f, err := os.Create(*output)
	if err != nil {
		panic(err)
	}
	if err = json.NewEncoder(f).Encode(data); err != nil {
		panic(err)
	}
	f.Close()
	intervalFile.Close()
	server.Close()
	client.Close()
}
