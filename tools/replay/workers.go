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
		}, *fIgnoreParseErrors)
	}
	return time.Unix(0, int64(minTime))
}

// Handles a single connection/client
type ClientWorker struct {
	redis     *redis.Client
	compare     *redis.Client
	incoming  chan Record
	processed uint
	pipe      redis.Pipeliner
	comparePipe redis.Pipeliner
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

var compareIgnoreCmds = []string{
    "HELLO",
    "AUTH",
    "SELECT",
    "INFO",
    "TIME",
    "CLIENT",
    "CONFIG",
}

// Handles a single file and distributes messages to clients
type FileWorker struct {
	clientGroup sync.WaitGroup
	timeOffset  time.Duration
	skipUntil   uint64
	stopUntil   uint64 // timestamp when to stop processing traffic (0 = no limit)
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

func ignoreCompareCmd(c redis.Cmder) bool {
    args := c.Args()
    if len(args) == 0 {
        return true
    }
    name := strings.ToUpper(fmt.Sprint(args[0]))
    for _, ign := range compareIgnoreCmds {
        if name == ign {
            return true
        }
    }
    return false
}

func cmdAsString(c redis.Cmder) string {
    args := c.Args()
    if len(args) == 0 {
        return "<no-args>"
    }

    name := strings.ToUpper(fmt.Sprint(args[0]))
    if len(args) == 1 {
        return name
    }

    parts := make([]string, 0, len(args) - 1)
    for _, a := range args[1:] {
        s := fmt.Sprint(a)
        parts = append(parts, s)
    }
    return name + " " + strings.Join(parts, " ")
}

func cmdResultString(cm redis.Cmder) string {
    if err := cm.Err(); err != nil {
        if err == redis.Nil {
            return "(nil)"
        }
        return "ERR: " + err.Error()
    }

    if cmd, ok := cm.(*redis.Cmd); ok {
        v := cmd.Val()
        s := fmt.Sprintf("%v", v)
        return s
    }

    return fmt.Sprintf("<unknown Cmder %T>", cm)
}

func compareCmdResults(a, b []redis.Cmder, lastMsg Record) {
    if len(a) != len(b) {
        log.Fatalf("[COMPARE] mismatch count: primary=%d compare=%d (last client=%d time=%d)", len(a), len(b), lastMsg.Client, lastMsg.Time)
        return
    }

    for i := range a {
		if (ignoreCompareCmd(a[i])) {
			continue
		}
		pa := cmdResultString(a[i])
        pb := cmdResultString(b[i])
        if pa != pb {
			log.Fatalf("[COMPARE] mismatch at idx %d cmd=%s\n  primary=%s\n  compare=%s\n  (client=%d time=%d)", i, cmdAsString(a[i]), pa, pb, lastMsg.Client, lastMsg.Time)
		}
    }
}

func (c *ClientWorker) Run(pace bool, worker *FileWorker) {
	for msg := range c.incoming {
		if c.processed == 0 && msg.DbIndex != 0 {
			// There is no easy way to switch, we rely on connection pool consisting only of one connection
			c.redis.Do(context.Background(), []interface{}{"SELECT", fmt.Sprint(msg.DbIndex)})
			if c.compare != nil {
        		c.compare.Do(context.Background(), []interface{}{"SELECT", fmt.Sprint(msg.DbIndex)})
    		}
		}

		lag := time.Until(worker.HappensAt(time.Unix(0, int64(msg.Time))))
		if lag < 0 {
			atomic.AddUint64(&worker.delayed, 1)
		}

		if pace {
			time.Sleep(lag)
		}

		c.pipe.Do(context.Background(), msg.values...).Result()
		if c.comparePipe != nil {
    		c.comparePipe.Do(context.Background(), msg.values...).Result()
		}

		atomic.AddUint64(&worker.processed, 1)

		if msg.HasMore == 0 {
			size := c.pipe.Len()
			start := time.Now()
			cmds, _ := c.pipe.Exec(context.Background())
			batchLatency := float64(time.Since(start).Microseconds())
			trackLatency(worker, batchLatency, size)
			c.processed += uint(size)

    		if c.comparePipe != nil {
        		ccmds, _ := c.comparePipe.Exec(context.Background())
        		compareCmdResults(cmds, ccmds, msg)
    		}
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

	if *fCompareHost != "" {
        client.compare = redis.NewClient(&redis.Options{Addr: *fCompareHost, PoolSize: 1, DisableIndentity: true})
        client.comparePipe = client.compare.Pipeline()
    }

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

		if w.skipUntil > 0 && r.Time < w.skipUntil {
			return true
		}

		atomic.AddUint64(&w.parsed, 1)

		if w.stopUntil > 0 && r.Time > w.stopUntil {
			return true
		}

		client.incoming <- r
		return true
	}, *fIgnoreParseErrors)

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
