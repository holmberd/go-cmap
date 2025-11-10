package cmap

type Config struct {
	// P is a performance parameter, a value from 0.0 to 1.0 that tunes the trade-off
	// between CPU efficiency (fewer pointers for the GOGC to scan) and memory
	// efficiency (less wasted space in chunks).
	//
	// 	- A value of 1.0 prioritizes minimizing the number of chunks (larger chunks) to
	// 		reduce GC pressure, at the cost of higher potential memory waste.
	//
	// 	- A value of 0.0 prioritizes minimizing memory waste by using smaller chunks,
	// 		at the cost of higher potential GC pressure.
	//
	// 	- A value of 0.5 aims to balance memory waste against GC pressure by favouring
	// 		small chunks for small buffers and larger chunks for large buffers.
	P float64

	CompactDeadRatio float64 // Ratio of dead to live space for when to trigger compaction.
}

func DefaultChunkPoolConfig() ChunkPoolConfig {
	return ChunkPoolConfig{
		FreeThresholds: [len(chunkSizes)]int{
			1024, // 64MB
			128,  // 64MB
			64,   // 128MB
		},
	}
}
