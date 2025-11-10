# CMap
CMap provides a fast, concurrent-safe map implementation in Go, designed to minimize GC overhead when storing a large number of entries.

## Features
- **Concurrent Safety**: Concurrent write and read operations can be performed by multiple goroutines without data races.
- **Low GC Overhead**: Designed to minimize GC overhead when storing a large number of entries.
- **Compaction**: Reclaims space from deleted entries using an efficient background compaction mechanism.
- **Tunable**: Configurable memory footprint vs CPU performance to suit different workloads.

## Benchmarks
```
GOMAXPROCS=4 go test github.com/holmberd/go-cmap -bench=BenchmarkCMap -benchtime=10s
goos: linux
goarch: amd64
pkg: github.com/holmberd/go-cmap
cpu: 13th Gen Intel(R) Core(TM) i5-1345U
BenchmarkCMapGetOnlyHits-12               449242102      27.72 ns/op       23 B/op        2 allocs/op
BenchmarkCMapGetBalanced-12               910117215      18.73 ns/op       16 B/op        2 allocs/op
BenchmarkCMapGetOrInsertBalanced-12        78020580      130.2 ns/op       35 B/op        2 allocs/op    12140 compactions
BenchmarkCMapInsertAdversarial-12         457474414      38.88 ns/op       23 B/op        1 allocs/op    95275 compactions

BenchmarkCMapInsertThroughput-12               2326    4852899 ns/op    13.50 MB/s    65536 items/op     23.00 compactions/op
BenchmarkCMapGetThroughput-12                  6318    1897250 ns/op    34.54 MB/s    65536 items/op

[Throughput (MB/s) is normalized to million of ops.]
```

## Example Usage
```go
import (
	"fmt"
	"log"

	"github.com/holmberd/go-cmap"
)

func main() {
	// Create a new map with the default configuration.
	// You can also pass a custom cmap.Config.
	c, err := cmap.New()
	if err != nil {
		log.Fatalf("failed to create cmap: %v", err)
	}
	defer c.Clear()

	key := []byte("user:123")
	value := []byte("{"name":"dag"}")

	_, err := c.Insert(key, value)
	if err != nil {
		log.Fatalf("failed to insert: %v", err)
	}

	val, ok, err := c.Get(key)
	if err != nil {
		log.Fatalf("failed to get: %v", err)
	}
	if ok {
		fmt.Printf("got value: %s\n", val)
	}

	ok, err = c.Delete(key)
	if err != nil {
		log.Fatalf("failed to delete: %v", err)
	}
	if ok {
		fmt.Println("deleted key")
	}

	ok = c.Has(key)
	if !ok {
		fmt.Println("key is gone")
	}
}
```

## Architecture
CMap achieves high concurrency by sharding its keyspace into separate buckets, allowing multiple goroutines to operate on distinct sets of keys. Each bucket uses an internal append-only, log-structured entry buffer to achieve fast, low-contention writes.

Each bucket is designed to minimize memory overhead and reduce garbage collector pressure. Instead of storing full entry objects in Go maps, each bucket maintains an index that maps a key's 64-bit hash to its byte offset within a contiguous buffer. All entry data is packed into this underlying buffer in fixed-size memory chunks, and each chunk is allocated using `unix.Mmap`, which reserves a block of memory that is not part of the Go Heap memory.

This approach ensures that the garbage collector only needs to track the small index map and chunk pointers, rather than scanning every individual entry. This significantly improves performance and reduces GC pauses, especially when managing a large number of entries.

The buffer uses amortized background compaction to reclaim memory from deleted entries, and adaptive chunk resizing to reduce the amount of chunks required as the buffer grows and shrinks. See buffer's [README.md](https://github.com/holmberd/go-cmap/internal/buffer/README.md) for a more detailed breakdown of the buffer's architecture.

## Limitations
- Max Key size is 64KB.
- Max Value size is 25MB.
- Key collisions follows last write wins, but reads are protected.
- For use-cases where the median entry size is larger than the max supported chunk size, use a custom `cmap.ChunkPool` that supports larger chunk sizes for better performance.
