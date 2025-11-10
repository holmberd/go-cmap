package buffer

import "sync"

// readerPool represents a pool of reusable reader objects.
// A Pool is safe for concurrent use by multiple goroutines.
type readerPool[P ChunkPooler] struct {
	pool sync.Pool
}

func newReaderPool[P ChunkPooler](b *Buffer[P]) *readerPool[P] {
	return &readerPool[P]{
		pool: sync.Pool{
			New: func() any {
				return NewReader(b)
			},
		},
	}
}

// Get retrieves a bucket from the pool or creates a new one.
func (p *readerPool[P]) Get() *Reader[P] {
	return p.pool.Get().(*Reader[P])
}

// Put releases any resources associated with a bucket and returns it to the pool for reuse.
func (p *readerPool[P]) Put(r *Reader[P]) {
	p.pool.Put(r.Reset())
}
