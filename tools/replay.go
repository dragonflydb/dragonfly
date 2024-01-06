package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
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
	values []interface{}
}

var kBigEmptyBytes = make([]byte, 100_000)

func parseStrings(file io.Reader) (out []interface{}, err error) {
	var num, strLen uint32
	err = binary.Read(file, binary.LittleEndian, &num)
	if err != nil {
		return nil, err
	}

	out = make([]interface{}, num)
	for i := uint32(0); i < num; i++ {
		err = binary.Read(file, binary.LittleEndian, &strLen)
		if err != nil {
			return nil, err
		}

		if strLen == 0 {
			err = binary.Read(file, binary.LittleEndian, &strLen)
			if err != nil {
				return nil, err
			}
			out[i] = kBigEmptyBytes[:strLen]
			continue
		}

		buf := make([]byte, strLen)
		_, err := io.ReadFull(file, buf)
		if err != nil {
			return nil, err
		}

		out[i] = string(buf)
	}
	return
}

func parseRecords(filename string, cb func(Record) bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		var rec Record
		err := binary.Read(reader, binary.LittleEndian, &rec.RecordHeader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		rec.values, err = parseStrings(reader)
		if err != nil {
			return err
		}

		if !cb(rec) {
			return nil
		}
	}

	return nil
}

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

func ApproxLatency() time.Duration {
	redis := redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1})

	var total time.Duration
	for i := 0; i < 10; i += 1 {
		before := time.Now()
		redis.Ping(context.Background()).Result()
		took := time.Since(before)
		total += took
	}
	return total / time.Duration(10)
}

type Client struct {
	redis    *redis.Client
	incoming chan Record
}

func (c Client) Run(worker *Worker) {
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

func NewClient(w *Worker) *Client {
	client := &Client{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost, PoolSize: 1, DisableIndentity: true}),
		incoming: make(chan Record, *fClientBuffer),
	}
	w.clients.Add(1)
	w.clientGroup.Add(1)
	go client.Run(w)
	return client
}

type Worker struct {
	clientGroup   sync.WaitGroup
	timeOffset    time.Duration
	approxLatency time.Duration
	processed     atomic.Uint64
	delayed       atomic.Uint64
	parsed        atomic.Uint64
	clients       atomic.Uint64
}

func (w *Worker) Run(file string, wg *sync.WaitGroup) {
	clients := make(map[uint32]*Client, 0)
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

func (w *Worker) HappensAt(recordTime time.Time) time.Time {
	return recordTime.Add(w.timeOffset).Add(-w.approxLatency)
}

func RenderTable(area *pterm.AreaPrinter, files []string, workers []Worker) {
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
	approxyLatency := ApproxLatency()

	fmt.Println("Offset -> ", timeOffset)
	fmt.Println("Approx latency -> ", approxyLatency)

	var wg sync.WaitGroup
	workers := make([]Worker, len(files))
	for i := range workers {
		workers[i] = Worker{
			timeOffset:    timeOffset,
			approxLatency: approxyLatency,
		}
		wg.Add(1)
		go workers[i].Run(files[i], &wg)
	}

	wgDone := make(chan bool)
	go func() {
		wg.Wait()
		wgDone <- true
	}()

	area, _ := pterm.DefaultArea.WithCenter().Start()
	for running := true; running; {
		select {
		case <-wgDone:
			running = false
		case <-time.After(100 * time.Millisecond):
			RenderTable(area, files, workers)
		}
	}

	RenderTable(area, files, workers)
}
