package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pterm/pterm"
	"github.com/redis/go-redis/v9"
)

var nEntries = flag.Int("n", 50000, "Number of vectors")
var nQueries = flag.Int("q", 1000, "Number of total queries")
var nQueryJobs = flag.Int("t", 8, "Query threads (jobs)")
var nDim = flag.Int("d", 100, "Vector dimension")
var nTop = flag.Int("k", 10, "Top K vectors selected")

var fPort = flag.Int("p", 6379, "Port")
var fHost = flag.String("h", "localhost", "Host")

func formatLargeNumber(n int) string {
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000.0)
	} else {
		return fmt.Sprintf("%.1fM", float64(n)/1000000.0)
	}
}

// Convert float slice to byte slice without copies
func VecToSlice(vec []float32) []byte {
	rawPtr := unsafe.Pointer(&vec[0])
	byteLen := len(vec) * int(unsafe.Sizeof(vec[0]))
	var out []byte
	sheader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	sheader.Data = uintptr(rawPtr)
	sheader.Len = byteLen
	sheader.Cap = byteLen
	return out
}

// Generate random vector
func RandVec(dim uint) []float32 {
	res := make([]float32, dim)
	for i := range res {
		res[i] = rand.Float32()
	}
	return res
}

// Create index
func CreateIndex(ctx context.Context, rdb *redis.Client, dim uint) error {
	hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: int(dim), DistanceMetric: "L2"}
	schema := redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}
	_, err := rdb.FTCreate(ctx, "idx", &redis.FTCreateOptions{}, &schema).Result()
	return err
}

func WaitForIndex(ctx context.Context, rdb *redis.Client) {
	info, _ := rdb.Info(ctx).Result()
	if strings.Contains(info, "dragonfly") {
		return
	}

	for {
		idxInfo, err := rdb.FTInfo(ctx, "idx").Result()
		if err != nil {
			panic(err)
		}
		if idxInfo.PercentIndexed >= 1.0 {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// Fill with random vectors
func Fill(ctx context.Context, rdb *redis.Client, prefix string, entries uint, dim uint) {
	const kBatchSize = uint(100)
	for i := uint(0); i < entries/kBatchSize; i += 1 {
		p := rdb.Pipeline()
		for j := 0; j < int(kBatchSize); j += 1 {
			key := fmt.Sprint(prefix, i, ":", j)
			vec := RandVec(dim)
			p.HSet(ctx, key, "v", VecToSlice(vec))
		}
		_, err := p.Exec(ctx)
		if err != nil {
			panic(err)
		}
	}
}

// Distribute Fill() over workers
func FillParallel(ctx context.Context, rdb *redis.Client, entries uint, dim uint) {
	const kJobs = uint(8)
	wg := sync.WaitGroup{}
	wg.Add(int(kJobs))

	for i := uint(0); i < kJobs; i += 1 {
		li := i
		go func() {
			Fill(ctx, rdb, fmt.Sprint("k", li, ":"), entries/kJobs, dim)
			wg.Done()
		}()
	}
	wg.Wait()
}

// Perform queries and measure latencies
func Query(ctx context.Context, rdb *redis.Client, queries uint, limit uint, dim uint) []time.Duration {
	latencies := make([]time.Duration, queries)
	query := fmt.Sprintf("*=>[KNN %v @v $vec]", limit)

	for i := range latencies {
		searchOptions := &redis.FTSearchOptions{
			DialectVersion: 2,
			NoContent:      true,
			Params:         map[string]interface{}{"vec": VecToSlice(RandVec(dim))},
		}

		start := time.Now()
		res, err := rdb.FTSearchWithArgs(ctx, "idx", query, searchOptions).Result()
		if err != nil {
			panic(err)
		}
		if res.Total != int(limit) {
			panic("Didn't hit limit")
		}
		latencies[i] = time.Since(start)
	}
	return latencies
}

// Call Query() from multiple workers and combine latencies sorted
func RunQueries(ctx context.Context, rdb *redis.Client) (time.Duration, []time.Duration) {
	jobs := uint(*nQueryJobs)
	jobQueries := uint(*nQueries) / jobs
	latencies := make([][]time.Duration, jobs)

	start := time.Now()
	wg := sync.WaitGroup{}

	wg.Add(len(latencies))
	for i := range latencies {
		li := i
		go func() {
			latencies[li] = Query(ctx, rdb, jobQueries, uint(*nTop), uint(*nDim))
			wg.Done()
		}()
	}

	wg.Wait()
	took := time.Since(start)

	// Unify all latencies and sort them
	allLatencies := make([]time.Duration, 0, 1000)
	for _, sub := range latencies {
		allLatencies = append(allLatencies, sub...)
	}
	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})
	return took, allLatencies
}

func Print(took time.Duration, latencies []time.Duration) {
	qps := float64(*nQueries) / took.Seconds()
	style := pterm.NewStyle(pterm.Bold)

	pterm.Print("Entries(n): ", formatLargeNumber(*nEntries), " Queries(q): ", formatLargeNumber(*nQueries), " ")
	pterm.Print("Dimension(d): ", *nDim, " Threads(t): ", *nQueryJobs, " Top(k): ", *nTop)
	pterm.Println()
	pterm.Println()

	pterm.Println("Took:", took)
	pterm.Print("QPS: ")
	style.Println(int(qps))

	p50 := float64(latencies[len(latencies)/2]) / float64(time.Millisecond)
	pterm.Printf("P50: %.2f ms", p50)

	pterm.Print(" P95: ")
	p95 := float64(latencies[int(float32(len(latencies))*0.95)]) / float64(time.Millisecond)
	style.Printf("%.2f ms", p95)

	p99 := float64(latencies[int(float32(len(latencies))*0.99)]) / float64(time.Millisecond)
	pterm.Printf(" P99: %.2f ms \n", p99)
}

func CheckData(ctx context.Context, rdb *redis.Client) {
	size, _ := rdb.DBSize(ctx).Result()
	if size*2 < int64(*nEntries) {
		rdb.FlushAll(ctx).Result()
		pterm.Println("Filling database")
		FillParallel(ctx, rdb, uint(*nEntries), uint(*nDim))
	}
}

func CheckIndex(ctx context.Context, rdb *redis.Client) {
	indices, _ := rdb.FT_List(ctx).Result()
	if slices.Contains(indices, "idx") {
		pterm.Println("Index exists")
	} else {
		pterm.Println("Creating index with", formatLargeNumber(*nEntries), "entries")
		start := time.Now()
		err := CreateIndex(ctx, rdb, uint(*nDim))
		if err != nil {
			panic(err)
		}
		WaitForIndex(ctx, rdb)
		pterm.Println("Created index in", time.Since(start))
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Randomized vector search benchmark")
		flag.PrintDefaults()
	}
	flag.Parse()

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Protocol:    2,
		ReadTimeout: -1, // due to possibly long index construction
		Addr:        fmt.Sprint(*fHost, ":", *fPort),
	})

	CheckData(ctx, rdb)
	CheckIndex(ctx, rdb)

	pterm.Println("Running queries")
	took, latencies := RunQueries(ctx, rdb)
	Print(took, latencies)
}
