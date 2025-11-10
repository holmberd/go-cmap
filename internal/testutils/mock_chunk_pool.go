package testutils

import (
	"slices"
	"sync/atomic"
)

var (
	MockChunkSizes = []int{128, 512, 2048}
)

type MockChunkPool struct {
	getCalls atomic.Int64
	putCalls atomic.Int64
}

// Sizes returns supported chunk sizes.
func (p *MockChunkPool) Sizes() []int {
	return MockChunkSizes
}

func (p *MockChunkPool) IsSupported(chunkSize int) bool {
	return slices.Contains(p.Sizes(), chunkSize)
}

func (p *MockChunkPool) Get(chunkSize int) []byte {
	p.getCalls.Add(1)
	return make([]byte, chunkSize)
}

func (p *MockChunkPool) Put(c []byte) {
	p.putCalls.Add(1)
}

func (p *MockChunkPool) Allocate(chunkSize int, numChunks int) {}

func (p *MockChunkPool) GetCalls() int64 {
	return p.getCalls.Load()
}

func (p *MockChunkPool) PutCalls() int64 {
	return p.putCalls.Load()
}

func (p *MockChunkPool) ChunksInUse() int64 {
	return p.GetCalls() - p.PutCalls()
}

func (p *MockChunkPool) Reset() {
	p.getCalls.Store(0)
	p.putCalls.Store(0)
}
