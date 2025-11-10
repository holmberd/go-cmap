package cmap

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// GOMAXPROCS=4 go clean -testcache && go test -bench=BenchmarkCMap -benchtime=10s -benchmem .

const benchItems = 1 << 16

// BenchmarkCMapGetOnlyHits simulates a workload where the
// lookups are for keys that always exist in the map.
func BenchmarkCMapGetOnlyHits(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	// Pre-populate the map with a fixed set of keys.
	const keyspace = 1024
	v := []byte("dag")
	for i := range keyspace {
		c.Insert([]byte(strconv.Itoa(i)), v)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own random number source to avoid lock contention.
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			// Look up a random, pre-existing key. This guarantees a cache hit.
			k := []byte(strconv.Itoa(rng.Intn(keyspace)))
			if _, ok, err := c.Get(k); err != nil || !ok {
				panic(fmt.Errorf("failed to get key %q: %w", k, err))
			}
		}
	})
}

// BenchmarkCMapGetBalancedHits simulates a workload where lookups are
// with a 50/50 mix of hits and misses.
func BenchmarkCMapGetBalanced(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	// Pre-populate the map with keys from 0 to keyspace-1.
	const keyspace = 1024
	v := []byte("dag")
	for i := range keyspace {
		c.Insert([]byte(strconv.Itoa(i)), v)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			// Look up a random key in the range [keyspace, 2*keyspace-1], (50/50).
			k := []byte(strconv.Itoa(rng.Intn(keyspace) + keyspace))
			if _, _, err := c.Get(k); err != nil {
				panic(fmt.Errorf("failed to get key %q: %w", k, err))
			}
		}
	})
}

// BenchmarkCMapGetOrInsertBalanced simulates a high-churn workload
// with a 50/50 mix of reads and writes.
func BenchmarkCMapGetOrInsertBalanced(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	const keyspace = 1024
	v := []byte("dag")
	for i := range keyspace {
		c.Insert([]byte(strconv.Itoa(i)), v)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := []byte(strconv.Itoa(rng.Intn(keyspace)))
			if rng.Intn(2) == 0 {
				if _, ok, err := c.Get(k); err != nil || !ok {
					panic(fmt.Errorf("failed to get key %q: %w", k, err))
				}
			} else {
				// Some inserts will be updates.
				if _, err := c.Insert(k, v); err != nil {
					panic(fmt.Errorf("failed to insert key %q: %w", k, err))
				}
			}
		}
	})
	s := &Stats{}
	c.UpdateStats(s)
	b.ReportMetric(float64(s.Compactions), "compactions")
}

// threadCounter is a helper for the adversarial benchmark to assign a unique-ish
// ID to each parallel goroutine.
var threadCounter int64

// BenchmarkCMapAdversarial simulates a worst-case scenario with high contention. Many
// goroutines repeatedly update and delete a small set of hot keys.
func BenchmarkCMapAdversarial(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	// Create a small number of hot keys, one for each CPU core,
	// to ensures maximum contention.
	numHotKeys := runtime.GOMAXPROCS(0)
	hotKeys := make([][]byte, numHotKeys)
	for i := range numHotKeys {
		hotKeys[i] = []byte(strconv.Itoa(i))
	}
	v := []byte("dag")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Simple thread counter assigns a unique index to each goroutine.
		gID := atomic.AddInt64(&threadCounter, 1) - 1
		k := hotKeys[int(gID)%numHotKeys]
		var err error

		for pb.Next() {
			if _, err = c.Insert(k, v); err != nil {
				panic(fmt.Errorf("failed to insert key %q: %w", k, err))
			}
			// Immediately delete to create churn.
			if _, err = c.Delete(k); err != nil {
				panic(fmt.Errorf("failed to delete key %q: %w", k, err))
			}
		}
	})
	s := &Stats{}
	c.UpdateStats(s)
	b.ReportMetric(float64(s.Compactions), "compactions")
}

// BenchmarkCMapInsertThroughput measures Insert throughput in a
// high conention, update-heavy workload.
func BenchmarkCMapInsertThroughput(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	// Normalized for operation throughput, million of ops.
	b.SetBytes(benchItems)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("dag")
		var err error
		for pb.Next() {
			for range benchItems {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				_, err = c.Insert(k, v)
				if err != nil {
					panic(fmt.Errorf("failed to insert %q: %w", k, err))
				}
			}
		}
	})
	b.ReportMetric(float64(benchItems), "items/op")
	s := &Stats{}
	c.UpdateStats(s)
	b.ReportMetric(float64(s.Compactions/uint64(b.N)), "compactions/op")
}

// BenchmarkCMapGetThroughput measures the throughput of Get
// operations under high contention.
func BenchmarkCMapGetThroughput(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Clear()

	// Pre-populate the map with keys.
	k := []byte("\x00\x00\x00\x00")
	v := []byte("dag")
	for range benchItems {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		_, err := c.Insert(k, v)
		if err != nil {
			panic(fmt.Errorf("failed to insert: %w", err))
		}
	}
	b.ReportAllocs()
	b.SetBytes(benchItems)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		var err error
		var ok bool
		for pb.Next() {
			for range benchItems {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				_, ok, err = c.Get(k)
				if err != nil || !ok {
					panic(fmt.Errorf("failed to get key %q: %w", k, err))
				}
			}
		}
	})
	b.ReportMetric(float64(benchItems), "items/op")
}
