package buffer

// chunkPooler defines the contract for a memory pool that manages fixed-size chunks.
type ChunkPooler interface {
	Sizes() []int                          // Returns supported chunk sizes.
	IsSupported(chunkSize int) bool        // Checks if a chunk size is supported.
	Get(chunkSize int) []byte              // Get retrieves a chunk from a pool of the specified size.
	Put(c []byte)                          // Put returns a byte slice to the pool.
	Allocate(chunkSize int, numChunks int) // Allocates chunks in the pool (pre-warming).
}
