package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/redis/go-redis/v9"
)

type RecordHeader struct {
	Client  uint32
	Time    uint64
	DbIndex uint32
	HasMore uint32
}

type Record struct {
	RecordHeader
	values []interface{} // instead of []string to unwrap into variadic
}

// Determine earliest time
func DetermineBaseTime(files []string) time.Time {
	var minTime uint64 = math.MaxUint64
	for _, file := range files {
		parseRecords(file, func(r Record) bool {
			if r.Time < minTime {
				minTime = r.Time
			}
			return false
		})
	}
	return time.Unix(0, int64(minTime))
}

// Handles a single connection/client
type ClientWorker struct {
	redis     *redis.Client
	incoming  chan Record
	processed uint
	pipe      redis.Pipeliner
}

// Pipeline length ranges for summary
var pipelineRanges = []struct {
	label string
	min   int
	max   int // inclusive, except last
}{
	{"0-29", 0, 29},
	{"30-79", 30, 79},
	{"80-199", 80, 199},
	{"200+", 200, 1 << 30},
}

// Handles a single file and distributes messages to clients
type FileWorker struct {
	clientGroup sync.WaitGroup
	timeOffset  time.Duration
	// stats for output, updated by clients, read by rendering goroutine
	processed uint64
	delayed   uint64
	parsed    uint64
	clients   uint64

	latencyDigest *tdigest.TDigest
	latencyMu     sync.Mutex

	latencySum   float64 // sum of all batch latencies (microseconds)
	latencyCount uint64  // number of batches

	// per-pipeline-range latency digests
	perRange map[string]*tdigest.TDigest
}

// Helper function to track latency and update digests
func trackLatency(worker *FileWorker, batchLatency float64, size int) {
	worker.latencyMu.Lock()
	defer worker.latencyMu.Unlock()
	worker.latencyDigest.Add(batchLatency, 1)
	worker.latencySum += batchLatency
	worker.latencyCount++
	// Add to per-range digest
	if worker.perRange != nil {
		for _, rng := range pipelineRanges {
			if size >= rng.min && size <= rng.max {
				worker.perRange[rng.label].Add(batchLatency, 1)
				break
			}
		}
	}
}

func (c *ClientWorker) Run(pace bool, worker *FileWorker) {
	for msg := range c.incoming {
		if c.processed == 0 && msg.DbIndex != 0 {
			// There is no easy way to switch, we rely on connection pool consisting only of one connection
			c.redis.Do(context.Background(), []interface{}{"SELECT", fmt.Sprint(msg.DbIndex)})
		}

		lag := time.Until(worker.HappensAt(time.Unix(0, int64(msg.Time))))
		if lag < 0 {
			atomic.AddUint64(&worker.delayed, 1)
		}

		if pace {
			time.Sleep(lag)
		}

		c.pipe.Do(context.Background(), msg.values...).Result()
		atomic.AddUint64(&worker.processed, 1)

		if msg.HasMore == 0 {
			size := c.pipe.Len()
			start := time.Now()
			c.pipe.Exec(context.Background())
			batchLatency := float64(time.Since(start).Microseconds())
			trackLatency(worker, batchLatency, size)
			c.processed += uint(size)
		}
	}

	if size := c.pipe.Len(); size >= 0 {
		start := time.Now()
		c.pipe.Exec(context.Background())
		batchLatency := float64(time.Since(start).Microseconds())
		trackLatency(worker, batchLatency, size)
		c.processed += uint(size)
	}

	worker.clientGroup.Done()
}

func NewClient(w *FileWorker, pace bool) *ClientWorker {
	client := &ClientWorker{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true}),
		incoming: make(chan Record, *fClientBuffer),
	}
	client.pipe = client.redis.Pipeline()

	atomic.AddUint64(&w.clients, 1)
	w.clientGroup.Add(1)
	go client.Run(pace, w)
	return client
}

func (w *FileWorker) Run(file string, wg *sync.WaitGroup) {
	w.latencyDigest = tdigest.NewWithCompression(1000)
	w.perRange = make(map[string]*tdigest.TDigest)
	for _, rng := range pipelineRanges {
		w.perRange[rng.label] = tdigest.NewWithCompression(500)
	}
	clients := make(map[uint32]*ClientWorker, 0)
	recordId := uint64(0)
	err := parseRecords(file, func(r Record) bool {
		client, ok := clients[r.Client]
		if !ok {
			client = NewClient(w, *fPace)
			clients[r.Client] = client
		}
		cmdName := strings.ToLower(r.values[0].(string))
		recordId += 1
		if cmdName != "eval" && recordId < uint64(*fSkip) {
			return true
		}
		atomic.AddUint64(&w.parsed, 1)
		client.incoming <- r
		return true
	})

	if err != nil {
		log.Fatalf("Could not parse records for file %s: %v", file, err)
	}

	for _, client := range clients {
		close(client.incoming)
	}
	w.clientGroup.Wait()
	wg.Done()
}

func (w *FileWorker) HappensAt(recordTime time.Time) time.Time {
	return recordTime.Add(w.timeOffset)
}
