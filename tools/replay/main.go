package main

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pterm/pterm"
)

var fHost = flag.String("host", "127.0.0.1:6379", "Redis host")
var fClientBuffer = flag.Int("buffer", 100, "How many records to buffer per client")

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

func Run(files []string) {
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

func Print(files []string) {
	type StreamTop struct {
		record Record
		ch     chan Record
	}

	// Start file reader goroutines
	var wg sync.WaitGroup
	wg.Add(len(files))

	tops := make([]StreamTop, len(files))
	for i, file := range files {
		tops[i].ch = make(chan Record, 100)
		go func(ch chan Record, file string) {
			parseRecords(file, func(r Record) bool {
				ch <- r
				return true
			})
			close(ch)
			wg.Done()
		}(tops[i].ch, file)
	}

	// Pick record with minimum time from each channel
	for {
		minTime := ^uint64(0)
		minIndex := -1
		for i := range tops {
			if tops[i].record.Time == 0 {
				if r, ok := <-tops[i].ch; ok {
					tops[i].record = r
				}
			}

			if rt := tops[i].record.Time; rt > 0 && rt < minTime {
				minTime = rt
				minIndex = i
			}
		}

		if minIndex == -1 {
			break
		}

		fmt.Println(tops[minIndex].record.values...)
		tops[minIndex].record = Record{}
	}

	wg.Wait()
}

func Analyze(files []string) {
	total := 0
	chained := 0
	clients := 0
	cmdCounts := make(map[string]uint)

	// count stats
	for _, file := range files {
		fileClients := make(map[uint32]bool)

		parseRecords(file, func(r Record) bool {
			total += 1
			if r.HasMore > 0 {
				chained += 1
			}

			fileClients[r.Client] = true
			cmdCounts[r.values[0].(string)] += 1

			return true
		})

		clients += len(fileClients)
	}

	// sort commands by frequencies
	type Freq struct {
		cmd   string
		count uint
	}
	var sortedCmds []Freq
	for cmd, count := range cmdCounts {
		sortedCmds = append(sortedCmds, Freq{cmd, count})
	}
	sort.Slice(sortedCmds, func(i, j int) bool {
		return sortedCmds[i].count > sortedCmds[j].count
	})

	// Print all the info
	fmt.Println("Total commands", total)
	fmt.Println("Has more%", 100*float32(chained)/float32(total))
	fmt.Println("Total clients", clients)

	for _, freq := range sortedCmds {
		fmt.Printf("%8d | %v \n", freq.count, freq.cmd)
	}
}

func main() {
	flag.Parse()
	cmd := flag.Arg(0)
	files := flag.Args()[1:]

	switch strings.ToLower(cmd) {
	case "run":
		Run(files)
	case "print":
		Print(files)
	case "analyze":
		Analyze(files)
	}
}
