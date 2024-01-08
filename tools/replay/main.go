package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
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
	redis    *redis.Client
	incoming chan Record
}

// Handles a single file and distributes messages to clients
type FileWorker struct {
	clientGroup sync.WaitGroup
	timeOffset  time.Duration
	// stats for output, updated by clients, read by rendering goroutine
	processed atomic.Uint64
	delayed   atomic.Uint64
	parsed    atomic.Uint64
	clients   atomic.Uint64
}

func (c ClientWorker) Run(worker *FileWorker) {
	for msg := range c.incoming {
		lag := time.Until(worker.HappensAt(time.Unix(0, int64(msg.Time))))
		if lag < 0 {
			worker.delayed.Add(1)
		}
		time.Sleep(lag)

		c.redis.Do(context.Background(), msg.values...).Result()
		worker.processed.Add(1)
	}
	worker.clientGroup.Done()
}

func NewClient(w *FileWorker) *ClientWorker {
	client := &ClientWorker{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true}),
		incoming: make(chan Record, *fClientBuffer),
	}
	w.clients.Add(1)
	w.clientGroup.Add(1)
	go client.Run(w)
	return client
}

func (w *FileWorker) Run(file string, wg *sync.WaitGroup) {
	clients := make(map[uint32]*ClientWorker, 0)
	parseRecords(file, func(r Record) bool {
		client, ok := clients[r.Client]
		if !ok {
			client = NewClient(w)
			clients[r.Client] = client
		}
		w.parsed.Add(1)

		client.incoming <- r
		return true
	})

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
			fmt.Sprint(workers[i].parsed.Load()),
			fmt.Sprint(workers[i].processed.Load()),
			fmt.Sprint(workers[i].delayed.Load()),
			fmt.Sprint(workers[i].clients.Load()),
		})
	}
	content, _ := pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(tableData).Srender()
	area.Update(content)
}

func main() {
	flag.Parse()
	files := os.Args[1:]

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
