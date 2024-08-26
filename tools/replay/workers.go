package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

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

func (c *ClientWorker) Run(worker *FileWorker) {
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

		c.pipe.Do(context.Background(), msg.values...).Result()
		atomic.AddUint64(&worker.processed, 1)

		if msg.HasMore == 0 {
			size := c.pipe.Len()
			c.pipe.Exec(context.Background())
			c.processed += uint(size)
		}
	}

	if size := c.pipe.Len(); size >= 0 {
		c.pipe.Exec(context.Background())
		c.processed += uint(size)
	}

	worker.clientGroup.Done()
}

func NewClient(w *FileWorker) *ClientWorker {
	client := &ClientWorker{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true}),
		incoming: make(chan Record, *fClientBuffer),
	}
	client.pipe = client.redis.Pipeline()

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
