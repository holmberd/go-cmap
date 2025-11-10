// Package cmap implements an in-memory concurrent hash map.
// It supports efficient insertion, deletion, and retrival of entries by key.
package cmap

import (
	"github.com/cespare/xxhash/v2"
	"github.com/holmberd/go-cmap/internal/buffer"
)

var (
	defaultChunkPool   = NewChunkPool(DefaultChunkPoolConfig())
	ErrBufferCorrupted = buffer.ErrBufferCorrupted
	ErrKeyTooLarge     = buffer.ErrKeyTooLarge
	ErrValueTooLarge   = buffer.ErrValueTooLarge
	ErrKeyEmpty        = buffer.ErrKeyEmpty
)

const (
	bucketCount = 512 // Must be a power of two for unbiased modulo.
)

func bucketIndex(n uint64) uint64 {
	// Faster modulo via bitwise AND; requires bucketCount to be a power of two.
	return n & (bucketCount - 1)
}

// CMap represents a concurrent-safe map.
type CMap[P buffer.ChunkPooler] struct {
	buckets [bucketCount]bucket[P]
}

func new[P buffer.ChunkPooler](pool P, config buffer.Config) (*CMap[P], error) {
	if err := config.Validate(pool); err != nil {
		return nil, err
	}
	c := &CMap[P]{}
	for i := range c.buckets[:] {
		c.buckets[i].Init(pool, config)
	}
	return c, nil
}

// New creates a new concurrent map.
func New() (*CMap[*ChunkPool], error) {
	return new(defaultChunkPool, buffer.DefaultConfig(defaultChunkPool))
}

// Custom creates a new concurrent map with custom chunk pool and config.
func Custom[P buffer.ChunkPooler](pool P, config Config) (*CMap[P], error) {
	bConfig := buffer.DefaultConfig(pool)
	bConfig.P = config.P
	bConfig.CompactDeadRatio = config.CompactDeadRatio
	return new(pool, bConfig)
}

// Insert adds or updates an entry.
func (c *CMap[P]) Insert(key, value []byte) (updated bool, err error) {
	h := xxhash.Sum64(key)
	return c.buckets[bucketIndex(h)].Insert(key, value, h)
}

// Get retrieves an entry by its key.
// The ok result indicates whether the key was found in the map.
func (c *CMap[P]) Get(key []byte) (value []byte, ok bool, err error) {
	h := xxhash.Sum64(key)
	k, v, found, err := c.buckets[bucketIndex(h)].Get(h)
	if string(k) != string(key) {
		return nil, false, nil
	}
	return v, found, err
}

// Has returns whether the key exist in the map.
func (c *CMap[P]) Has(key []byte) bool {
	h := xxhash.Sum64(key)
	return c.buckets[bucketIndex(h)].Has(h)
}

// Delete removes an entry from the map by its key.
func (c *CMap[P]) Delete(key []byte) (ok bool, err error) {
	h := xxhash.Sum64(key)
	return c.buckets[bucketIndex(h)].Delete(h)
}

// Clear clears the map.
func (c *CMap[P]) Clear() {
	c.buckets = [bucketCount]bucket[P]{}
}

// Len returns the number of entries in the map.
func (c *CMap[P]) Len() int {
	n := 0
	for i := range c.buckets {
		n += c.buckets[i].Len()
	}
	return n
}

func (c *CMap[P]) UpdateStats(s *Stats) {
	for i := range c.buckets {
		c.buckets[i].UpdateStats(s)
	}
}
