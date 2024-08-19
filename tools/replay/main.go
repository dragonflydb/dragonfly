package main

import (
	"flag"
	"fmt"
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
