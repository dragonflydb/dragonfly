package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pterm/pterm"
	"github.com/redis/go-redis/v9"
)

var fHost = flag.String("host", "127.0.0.1:6379", "Redis host")
var fClientBuffer = flag.Int("buffer", 100, "How many records to buffer per client")

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
}

func (c ClientWorker) Run(worker *FileWorker) {
	for msg := range c.incoming {
		if c.processed == 0 && msg.DbIndex != 0 {
			// There is no easy way to switch, we rely on connection pool consisting only of one connection
			c.redis.Do(context.Background(), []interface{}{"SELECT", fmt.Sprint(msg.DbIndex)})
		}

		lag := time.Until(worker.HappensAt(time.Unix(0, int64(msg.Time))))
		if lag < 0 {
			atomic.AddUint64(&worker.delayed, 1)
		}
		time.Sleep(lag)

		c.redis.Do(context.Background(), msg.values...).Result()
		atomic.AddUint64(&worker.processed, 1)
		c.processed += 1
	}
	worker.clientGroup.Done()
}

func NewClient(w *FileWorker) *ClientWorker {
	client := &ClientWorker{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true}),
		incoming: make(chan Record, *fClientBuffer),
	}
	atomic.AddUint64(&w.clients, 1)
	w.clientGroup.Add(1)
	go client.Run(w)
	return client
}

func (w *FileWorker) Run(file string, wg *sync.WaitGroup) {
	clients := make(map[uint32]*ClientWorker, 0)
	err := parseRecords(file, func(r Record) bool {
		client, ok := clients[r.Client]
		if !ok {
			client = NewClient(w)
			clients[r.Client] = client
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

func RenderTable(area *pterm.AreaPrinter, files []string, workers []FileWorker) {
	tableData := pterm.TableData{{"file", "parsed", "processed", "delayed", "clients"}}
	for i := range workers {
		tableData = append(tableData, []string{
			files[i],
			fmt.Sprint(atomic.LoadUint64(&workers[i].parsed)),
			fmt.Sprint(atomic.LoadUint64(&workers[i].processed)),
			fmt.Sprint(atomic.LoadUint64(&workers[i].delayed)),
			fmt.Sprint(atomic.LoadUint64(&workers[i].clients)),
		})
	}
	content, _ := pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(tableData).Srender()
	area.Update(content)
}

func main() {
	flag.Parse()
	files := flag.Args()

	timeOffset := time.Now().Add(500 * time.Millisecond).Sub(DetermineBaseTime(files))
	fmt.Println("Offset -> ", timeOffset)

	// Start a worker for every file. They take care of spawning client workers.
	var wg sync.WaitGroup
	workers := make([]FileWorker, len(files))
	for i := range workers {
		workers[i] = FileWorker{timeOffset: timeOffset}
		wg.Add(1)
		go workers[i].Run(files[i], &wg)
	}

	wgDone := make(chan bool)
	go func() {
		wg.Wait()
		wgDone <- true
	}()

	// Render table while running
	area, _ := pterm.DefaultArea.WithCenter().Start()
	for running := true; running; {
		select {
		case <-wgDone:
			running = false
		case <-time.After(100 * time.Millisecond):
			RenderTable(area, files, workers)
		}
	}

	RenderTable(area, files, workers) // to show last stats
}
