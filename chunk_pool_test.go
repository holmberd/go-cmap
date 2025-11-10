package cmap

import "testing"

var TestChunkPoolConfig = ChunkPoolConfig{
	FreeThresholds: [len(chunkSizes)]int{10, 10, 10},
}

func TestChunkPools(t *testing.T) {
	t.Run("Get and Put single chunk for each chunk size", func(t *testing.T) {
		pool := NewChunkPool(TestChunkPoolConfig)
		for _, size := range pool.Sizes() {
			if numFree := pool.numFree(size); numFree != 0 {
				t.Fatalf("expected new pool for size %d to be empty, got %d chunks", size, numFree)
			}
		}

		for _, size := range pool.Sizes() {
			chunk := pool.Get(size)
			if chunk == nil {
				t.Fatalf("expected to get a valid chunk for size %d, got nil", size)
			}
			if len(chunk) != size || cap(chunk) != size {
				t.Errorf("expected for size %d: len/cap %d, got len=%d, cap=%d", size, size, len(chunk), cap(chunk))
			}

			expectedFree := chunksPerAlloc - 1
			if numFree := pool.numFree(size); numFree != expectedFree {
				t.Errorf("expected for size %d: free chunks %d after Get, got %d", size, expectedFree, numFree)
			}

			pool.Put(chunk)
		}

		for _, size := range pool.Sizes() {
			if numFree := pool.numFree(size); numFree != chunksPerAlloc {
				t.Fatalf("expected for size %d: free chunks %d after Put, got %d", size, chunksPerAlloc, numFree)
			}
		}
	})

	t.Run("Put nil does not panic or add to pool", func(t *testing.T) {
		pools := NewChunkPool(TestChunkPoolConfig)
		pools.Put(nil) // This should be a no-op and should not cause a panic.
		for _, size := range pools.Sizes() {
			if numFree := pools.numFree(size); numFree != 0 {
				t.Fatalf("expected new pool for size %d to be empty, got %d chunks", size, numFree)
			}
		}
	})

	t.Run("Put unsupported size does not panic or add to pool", func(t *testing.T) {
		pool := NewChunkPool(TestChunkPoolConfig)
		chunk := make([]byte, pool.Sizes()[len(pool.Sizes())-1]+1)
		pool.Put(chunk)
		for _, size := range pool.Sizes() {
			if numFree := pool.numFree(size); numFree != 0 {
				t.Fatalf("expected new pool for size %d to be empty, got %d chunks", size, numFree)
			}
		}
	})
}
