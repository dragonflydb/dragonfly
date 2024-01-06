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

type RecordHeader struct {
	Client  uint32
	Time    uint64
	HasMore uint32
}

type Record struct {
	RecordHeader
	values []interface{}
}

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
			out[i] = ""
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

type Worker struct {
	timeOffset    time.Duration
	approxLatency time.Duration
	processed     atomic.Uint64
	delayed       atomic.Uint64
	lag           atomic.Uint64
}

type Client struct {
	redis    *redis.Client
	incoming chan []interface{}
}

func (c Client) Run() {
	for msg := range c.incoming {
		c.redis.Do(context.Background(), msg...).Result()
	}
}

func NewClient() *Client {
	return &Client{
		redis:    redis.NewClient(&redis.Options{Addr: *fHost}),
		incoming: make(chan []interface{}, 10),
	}
}

func (w *Worker) Run(file string, wg *sync.WaitGroup) {
	clients := make(map[uint32]*Client, 0)
	parseRecords(file, func(r Record) bool {
		client, ok := clients[r.Client]
		if !ok {
			client = NewClient()
			go client.Run()
			clients[r.Client] = client
		}

		happensAt := time.Unix(0, int64(r.Time)).Add(w.timeOffset).Add(-w.approxLatency)
		lag := time.Until(happensAt)
		if lag < 0 {
			w.delayed.Add(1)
		}
		w.lag.Store(uint64(lag))
		time.Sleep(lag)

		client.incoming <- r.values
		w.processed.Add(1)
		return true
	})

	for _, client := range clients {
		close(client.incoming)
	}
	wg.Done()
}

func main() {
	flag.Parse()
	files := os.Args[1:]

	timeOffset := time.Now().Add(100 * time.Millisecond).Sub(DetermineBaseTime(files))
	approxyLatency := ApproxLatency()

	fmt.Println("Offset +100ms -> ", timeOffset)
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

	go func() {
		area, _ := pterm.DefaultArea.WithCenter().Start()
		for {
			tableData := pterm.TableData{{"file", "processed", "delayed", "lag"}}
			for i := range workers {
				tableData = append(tableData, []string{
					files[i], fmt.Sprint(workers[i].processed.Load()),
					fmt.Sprint(workers[i].delayed.Load()),
					fmt.Sprint(time.Duration(workers[i].lag.Load())),
				})
			}

			content, _ := pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(tableData).Srender()
			area.Update(content)

			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()
}
