package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/redis/go-redis/v9"
)

// Listener type identifiers as stored in traffic log v3 (must match
// facade::Connection::ListenerType in the C++ code). MAIN_RESP and ADMIN_RESP
// both speak RESP but live on different ports on the source server.
const (
	ListenerMainRESP  uint8 = 1
	ListenerMemcache  uint8 = 2
	ListenerAdminRESP uint8 = 3
)

// Flags field layout:
//
//	bit 0     : 1 if the next record belongs to the same batch as this one (HasMore)
//	bits 1-31 : reserved, must be zero
//
// The listener type is stored once in the file header (see parsing.go), not per record.
type RecordHeader struct {
	Client  uint32
	Time    uint64
	DbIndex uint32
	Flags   uint32
}

func (h RecordHeader) HasMore() bool {
	return h.Flags&1 != 0
}

type Record struct {
	RecordHeader
	values []interface{} // instead of []string to unwrap into variadic
}

// Determine earliest time
func DetermineBaseTime(files []string) time.Time {
	var minTime uint64 = math.MaxUint64
	for _, file := range files {
		parseRecords(file, nil, func(r Record) bool {
			if r.Time < minTime {
				minTime = r.Time
			}
			return false
		}, *fIgnoreParseErrors) //nolint:errcheck
	}
	return time.Unix(0, int64(minTime))
}

// Handles a single connection/client
type ClientWorker struct {
	// RESP path (used for resp and admin listeners).
	redis       *redis.Client
	compare     *redis.Client
	pipe        redis.Pipeliner
	comparePipe redis.Pipeliner

	// Memcache path (used for memcache listener).
	mc           *mcClient
	listenerType uint8

	incoming  chan Record
	processed uint
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

	parts := make([]string, 0, len(args)-1)
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
		if ignoreCompareCmd(a[i]) {
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
	if c.listenerType == ListenerMemcache {
		c.runMC(pace, worker)
	} else {
		c.runRedis(pace, worker)
	}
	worker.clientGroup.Done()
}

func (c *ClientWorker) runRedis(pace bool, worker *FileWorker) {
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

		if !msg.HasMore() {
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

	// Final flush: only run Exec if there is something pending (the input channel
	// closed mid-batch). Also drain the compare pipeline so its connection does not
	// keep commands buffered; we don't have a last-message context to attribute a
	// mismatch to, so we only compare when both pipelines have the same length.
	if size := c.pipe.Len(); size > 0 {
		start := time.Now()
		cmds, _ := c.pipe.Exec(context.Background())
		batchLatency := float64(time.Since(start).Microseconds())
		trackLatency(worker, batchLatency, size)
		c.processed += uint(size)

		if c.comparePipe != nil && c.comparePipe.Len() == size {
			ccmds, _ := c.comparePipe.Exec(context.Background())
			compareCmdResults(cmds, ccmds, Record{})
		} else if c.comparePipe != nil {
			// Lengths diverged — still drain so the comparePipe doesn't leak commands.
			c.comparePipe.Exec(context.Background())
		}
	}
}

// runMC replays memcache-listener records via the memcache text protocol.
// The ASCII protocol has no multiplexing, so each command is issued synchronously
// on the per-client connection (see mcClient in memcache.go) and the
// pipeline-range latency digest uses batch size = 1.
func (c *ClientWorker) runMC(pace bool, worker *FileWorker) {
	for msg := range c.incoming {
		lag := time.Until(worker.HappensAt(time.Unix(0, int64(msg.Time))))
		if lag < 0 {
			atomic.AddUint64(&worker.delayed, 1)
		}
		if pace {
			time.Sleep(lag)
		}

		start := time.Now()
		dispatchMC(c.mc, msg.values)
		trackLatency(worker, float64(time.Since(start).Microseconds()), 1)
		atomic.AddUint64(&worker.processed, 1)
		c.processed++
	}
}

// dispatchMC maps a recorded memcache command to the raw wire commands sent
// by the original client. See memcache.go for the underlying TCP client.
//
// Record layout written by facade::LogMemcacheTraffic:
//
//	SET/ADD/REPLACE/APPEND/PREPEND : [cmd, key, value, flags, expire_ts]
//	CAS                            : [cas, key, value, flags, expire_ts, cas_unique]
//	INCR/DECR                      : [cmd, key, delta]
//	GAT/GATS                       : [cmd, expire_ts, key+]
//	GET/GETS/DELETE/FLUSHALL/...   : [cmd, *args]
//
// Unknown commands are dropped silently.
func dispatchMC(mc *mcClient, values []interface{}) {
	if len(values) == 0 {
		return
	}
	name, ok := values[0].(string)
	if !ok {
		return
	}
	arg := func(i int) string {
		if i >= len(values) {
			return ""
		}
		s, _ := values[i].(string)
		return s
	}
	argU64 := func(i int) uint64 {
		v, _ := strconv.ParseUint(arg(i), 10, 64)
		return v
	}
	argI64 := func(i int) int64 {
		v, _ := strconv.ParseInt(arg(i), 10, 64)
		return v
	}
	argU32 := func(i int) uint32 {
		return uint32(argU64(i))
	}

	lc := strings.ToLower(name)
	switch lc {
	case "set", "add", "replace", "append", "prepend":
		mc.Store(lc, arg(1), argU32(3), argI64(4), []byte(arg(2)))
	case "cas":
		mc.Cas(arg(1), argU32(3), argI64(4), argU64(5), []byte(arg(2)))
	case "get", "gets":
		keys := stringArgs(values, 1)
		if len(keys) > 0 {
			mc.Retrieve(lc, keys)
		}
	case "gat", "gats":
		keys := stringArgs(values, 2)
		if len(keys) > 0 {
			mc.RetrieveGat(lc, argI64(1), keys)
		}
	case "delete":
		if k := arg(1); k != "" {
			mc.Delete(k, stringArgs(values, 2))
		}
	case "incr", "decr":
		mc.IncrDecr(lc, arg(1), argU64(2))
	case "flush_all":
		mc.FlushAll()
	case "quit", "version", "stats", "mn":
		// No-op: control / meta-NOOP commands, not useful for replay.
	default:
		// Defensive: parser normalises meta commands (ms/md/ma/mg/me) to their
		// regular counterparts before logging, so only unknown/corrupted records
		// can land here. Surface them so replay isn't silently skipping data.
		log.Printf("replay: unknown memcache command %q skipped", name)
	}
}

// stringArgs returns values[start:] coerced to []string (non-string entries
// are skipped).
func stringArgs(values []interface{}, start int) []string {
	out := make([]string, 0, len(values)-start)
	for i := start; i < len(values); i++ {
		if s, ok := values[i].(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func NewClient(w *FileWorker, pace bool, listenerType uint8) *ClientWorker {
	client := &ClientWorker{
		listenerType: listenerType,
		incoming:     make(chan Record, *fClientBuffer),
	}

	// Protocol is decided per file from its header (see FileWorker.Run), so the
	// same -host can be reused across invocations that target different backends
	// of the same Dragonfly instance (main RESP port for RESP records, memcache
	// port for memcache records). Mixing file types in one invocation is the
	// caller's responsibility — the tool connects as the file says.
	if listenerType == ListenerMemcache {
		client.mc = newMCClient(*fHost)
	} else {
		// MAIN_RESP and ADMIN_RESP both speak RESP — they share -host. Replaying
		// admin-listener traffic against a non-admin port may lose privileged
		// commands; the caller is expected to point -host at the appropriate port.
		client.redis = redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true})
		client.pipe = client.redis.Pipeline()
		// -compare-host only makes sense for the main-listener path.
		if listenerType == ListenerMainRESP && *fCompareHost != "" {
			client.compare = redis.NewClient(&redis.Options{Addr: *fCompareHost, PoolSize: 1, DisableIndentity: true})
			client.comparePipe = client.compare.Pipeline()
		}
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
	var listenerType uint8
	err := parseRecords(file, func(lt uint8) {
		listenerType = lt
	}, func(r Record) bool {
		client, ok := clients[r.Client]
		if !ok {
			// Listener type is uniform for the whole file (file-header), so every
			// client spawned for this FileWorker targets the same backend.
			client = NewClient(w, *fPace, listenerType)
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
