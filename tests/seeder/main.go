package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/atomic"
)

type Stats struct {
	Requests *atomic.Uint64
	Errors   *atomic.Uint64
}

func client(addr string, pass string, keys uint64, readOnly bool, setRatio int, stats *Stats) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Println(addr)
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{addr},
		Password: pass,
		// MaxRedirects: -1,
		// MaxRetries: -1,
		// DialTimeout: time.Second,
		// ReadTimeout: time.Second*5,
		// WriteTimeout: time.Second*5,
		// MinRetryBackoff: -1,
		// MaxRetryBackoff: -1,
		ReadOnly:      readOnly,
		RouteRandomly: readOnly,
		// TLSConfig: &tls.Config{},
	})

	keyIdx := rand.Uint64() % keys
	for {
		if err := request(keyIdx, setRatio, client, rand); err != nil {
			if stats.Errors.Load() == 0 {
				fmt.Println(err)
			}

			stats.Errors.Inc()
		}
		stats.Requests.Inc()

		keyIdx++
		keyIdx = keyIdx % keys
	}
}

func request(keyIdx uint64, setRatio int, client *redis.ClusterClient, rand *rand.Rand) error {
	if setRatio != 0 && rand.Intn(setRatio) == 0 {
		return client.Set(context.TODO(), fmt.Sprintf("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:%d", keyIdx), "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 0).Err()
	}
	return client.Get(context.TODO(), fmt.Sprintf("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:%d", keyIdx)).Err()
}

func main() {
	addr := os.Args[1]
	pass := os.Args[2]
	keys, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		panic(err)
	}
	clients, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}
	setRatio, err := strconv.Atoi(os.Args[6])
	if err != nil {
		panic(err)
	}

	stats := &Stats{
		Requests: atomic.NewUint64(0),
		Errors:   atomic.NewUint64(0),
	}
	for i := 0; i != clients; i++ {
		go client(addr, pass, keys, os.Args[5] == "true", setRatio, stats)
	}

	for {
		<-time.After(time.Second)

		requests := stats.Requests.Swap(0)
		errors := stats.Errors.Swap(0)
		fmt.Println(requests, errors)

		errorRate := float64(0)
		if requests > 0 {
			errorRate = float64(errors) / float64(requests)
		}
		fmt.Printf("%s: %d/%d (%.1f)\n", time.Now().Format("15:04:05.00000"), errors, requests, errorRate)
	}
}
