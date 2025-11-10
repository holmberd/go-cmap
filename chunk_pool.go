package cmap

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

const chunksPerAlloc = 1

const (
	KiB = 1024
	MiB = KiB * KiB

	ChunkSize64K  = 64 * KiB
	ChunkSize512K = 512 * KiB
	ChunkSize2M   = 2 * MiB
)

// chunkSizes represents supported chunk sizes in KiB for the buffer ordered by smallest to largest.
//   - The smallest chunk size should be small enough to avoid significant memory waste when
//     a buffer contains only a few small entries.
//   - The largest chunk size should be large enough to minimize the number of chunks GOGC has to scan.
var chunkSizes = [3]int{
	ChunkSize64K,
	ChunkSize512K,
	ChunkSize2M,
}

func init() {
	// Runtime assertion.
	if !sort.IntsAreSorted(chunkSizes[:]) {
		panic(errors.New("chunk sizes must be sorted in ascending order"))
	}
}

type ChunkPoolConfig struct {
	// Number of free chunks for each chunk size the pool can hold before starting to release memory.
	FreeThresholds [len(chunkSizes)]int
}

// ChunkPool is a collection of thread-safe pools for managing off-heap memory
// chunks of a pre-defined set of fixed sizes.
type ChunkPool struct {
	mu       sync.Mutex
	free64K  []*[ChunkSize64K]byte
	free512K []*[ChunkSize512K]byte
	free2M   []*[ChunkSize2M]byte

	// freeThresholds represents the number of free chunks for each size the pool
	// can hold before starting to release memory.
	freeThresholds [len(chunkSizes)]int
}

// NewChunkPool creates a new, empty collecion of chunk pools.
func NewChunkPool(config ChunkPoolConfig) *ChunkPool {
	return &ChunkPool{freeThresholds: config.FreeThresholds}
}

// Sizes returns a slice of supported chunk sizes.
func (p *ChunkPool) Sizes() []int {
	return chunkSizes[:]
}

func (p *ChunkPool) IsSupported(chunkSize int) bool {
	return slices.Contains(p.Sizes(), chunkSize)
}

// Get retrieves a chunk from a pool of the specified size.
// It will panic if an unsupported size is requested.
func (p *ChunkPool) Get(chunkSize int) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch chunkSize {
	case ChunkSize64K:
		if len(p.free64K) == 0 {
			p.alloc(chunkSize, chunksPerAlloc)
		}
		n := len(p.free64K) - 1
		ptr := p.free64K[n]
		p.free64K = p.free64K[:n]
		return ptr[:]
	case ChunkSize512K:
		if len(p.free512K) == 0 {
			p.alloc(chunkSize, chunksPerAlloc)
		}
		n := len(p.free512K) - 1
		ptr := p.free512K[n]
		p.free512K = p.free512K[:n]
		return ptr[:]
	case ChunkSize2M:
		if len(p.free2M) == 0 {
			p.alloc(chunkSize, chunksPerAlloc)
		}
		n := len(p.free2M) - 1
		ptr := p.free2M[n]
		p.free2M = p.free2M[:n]
		return ptr[:]
	default:
		panic(fmt.Sprintf("unsupported chunk size requested: %d", chunkSize))
	}
}

// Put returns a byte slice to the pool.
// It does nothing if the chunk size is not a supported size.
func (p *ChunkPool) Put(c []byte) {
	if c == nil {
		return
	}

	size := cap(c)
	c = c[:size] // Ensure the chunk is reset to its full capacity before returning.

	switch size {
	case ChunkSize64K:
		ptr := (*[ChunkSize64K]byte)(unsafe.Pointer(&c[0]))
		var chunksToUnmap []*[ChunkSize64K]byte

		p.mu.Lock()
		p.free64K = append(p.free64K, ptr)
		p.free64K, chunksToUnmap = releaseChunks(p.free64K, p.freeThresholds[0])
		p.mu.Unlock()

		// Perform unmap outside of the lock to avoid blocking other operations.
		for _, chunkPtr := range chunksToUnmap {
			p.unmap(chunkPtr[:])
		}

	case ChunkSize512K:
		ptr := (*[ChunkSize512K]byte)(unsafe.Pointer(&c[0]))
		var chunksToUnmap []*[ChunkSize512K]byte

		p.mu.Lock()
		p.free512K = append(p.free512K, ptr)
		p.free512K, chunksToUnmap = releaseChunks(p.free512K, p.freeThresholds[1])
		p.mu.Unlock()

		for _, chunkPtr := range chunksToUnmap {
			p.unmap(chunkPtr[:])
		}

	case ChunkSize2M:
		ptr := (*[ChunkSize2M]byte)(unsafe.Pointer(&c[0]))
		var chunksToUnmap []*[ChunkSize2M]byte

		p.mu.Lock()
		p.free2M = append(p.free2M, ptr)
		p.free2M, chunksToUnmap = releaseChunks(p.free2M, p.freeThresholds[2])
		p.mu.Unlock()

		for _, chunkPtr := range chunksToUnmap {
			p.unmap(chunkPtr[:])
		}
	}
}

// Allocate ensures that at least numChunks are available in the pool for the
// specified size. This is useful for pre-warming a pool to a specific capacity.
// It will panic if an unsupported size is requested.
func (p *ChunkPool) Allocate(chunkSize int, numChunks int) {
	if numChunks <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	var n int
	switch chunkSize {
	case ChunkSize64K:
		n = numChunks - len(p.free64K)
	case ChunkSize512K:
		n = numChunks - len(p.free512K)
	case ChunkSize2M:
		n = numChunks - len(p.free2M)
	default:
		panic(fmt.Sprintf("unsupported chunk size for pre-allocation: %d", chunkSize))
	}
	if n > 0 {
		p.alloc(chunkSize, n)
	}
}

// unmap releases the memory of a chunk back to the operating system.
func (p *ChunkPool) unmap(c []byte) {
	if err := unix.Munmap(c); err != nil {
		slog.Error("failed to unmap chunk", "error", err)
	}
}

// alloc allocates the specified number of free chunks and size.
// It assumes the caller holds the mutex.
func (p *ChunkPool) alloc(chunkSize int, numChunks int) {
	totalAllocSize := chunkSize * numChunks

	// Use unix.Mmap to allocate virtual memory that is not part the Go heap.
	// This effectively reduces how often the GOGC has to run.
	data, err := unix.Mmap(-1, 0, totalAllocSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE,
	)
	if err != nil {
		panic(fmt.Errorf("cannot allocate %d bytes via mmap for chunk size %d: %w", totalAllocSize, chunkSize, err))
	}

	// Slice the mmap'd region into chunks and append to the correct free list.
	for len(data) > 0 {
		chunkSlice := data[:chunkSize:chunkSize]
		data = data[chunkSize:]

		switch chunkSize {
		case ChunkSize64K:
			ptr := (*[ChunkSize64K]byte)(unsafe.Pointer(&chunkSlice[0]))
			p.free64K = append(p.free64K, ptr)
		case ChunkSize512K:
			ptr := (*[ChunkSize512K]byte)(unsafe.Pointer(&chunkSlice[0]))
			p.free512K = append(p.free512K, ptr)
		case ChunkSize2M:
			ptr := (*[ChunkSize2M]byte)(unsafe.Pointer(&chunkSlice[0]))
			p.free2M = append(p.free2M, ptr)
		}
	}
}

// numFree returns the number of available chunks for a given chunk size.
// It is primarily intended as helper method in tests.
func (mp *ChunkPool) numFree(size int) int {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	switch size {
	case ChunkSize64K:
		return len(mp.free64K)
	case ChunkSize512K:
		return len(mp.free512K)
	case ChunkSize2M:
		return len(mp.free2M)
	default:
		return 0
	}
}

// releaseChunks is a generic helper that trims the free list if it exceeds the given threshold.
// It returns the updated list and a list of any chunks that were removed and should be unmapped.
func releaseChunks[P any](freeList []P, threshold int) (newList []P, toUnmap []P) {
	if threshold > 0 && len(freeList) > threshold {
		// Release half of the free chunks to prevent thrashing around the threshold.
		freeCount := len(freeList) / 2
		toUnmap = freeList[:freeCount]
		newList = freeList[freeCount:]
		return newList, toUnmap
	}
	return freeList, nil
}
