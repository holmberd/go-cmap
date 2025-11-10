package buffer

import (
	"errors"
	"fmt"
)

type Config struct {
	// P is a performance preference parameter, a value from 0.0 to 1.0 that tunes
	// the trade-off between CPU efficiency (fewer pointers for the GOGC to scan) and
	// memory efficiency (less wasted space in chunks).
	//
	// 	- A value of 1.0 prioritizes minimizing the number of chunks to reduce GC
	// 		pressure, at the cost of higher potential memory waste.
	//
	// 	- A value of 0.0 prioritizes minimizing memory waste by using smaller chunks,
	// 		at the cost of higher potential GC pressure.
	//
	// 	- A value of 0.5 aims to balance memory waste against GC pressure by favouring
	// 		small chunks for small buffers and larger chunks for large buffers.
	P float64

	ChunkSize int // Initial buffer chunk size.

	// Bytes to process in a single compaction step. A value <= 0 disables compaction
	// TODO: test disable.
	CompactBytes int

	// Compaction is triggered only when both of the following conditions are met:
	//
	// 1. The absolute amount of dead bytes exceeds the dynamic minimum threshold calculated from:
	//    (CurrentChunkSize * CompactDeadChunkRatio)
	//
	// 2. The relative amount of dead bytes to total bytes exceeds CompactDeadRatio.
	//
	// This two-level check prevents thrashing:
	// - CompactDeadRatio prevents compaction on large buffers where the relative waste is low.
	// - CompactDeadChunkRatio prevents compaction on small buffer even if the ratio is high.

	CompactDeadRatio      float64 // Ratio of dead bytes to total to trigger compaction.
	CompactDeadChunkRatio float64 // Minimum amount of dead bytes to chunk size ratio to trigger compaction.

	// ChunkDowngradeRatio is the ratio of a buffer's live bytes to a lower
	// chunk resize threshold tier that triggers a chunk size downgrade (shrink).
	//
	// A downgrade is triggered when:
	//   LiveBytes < (resizeThreshold * ChunkDowngradeRatio)
	//
	// This parameter works with the upgrade logic to create a hysteresis
	// band, preventing the buffer from rapidly thrashing (growing and
	// shrinking) under fluctuating load.
	ChunkDowngradeRatio float64
}

func (c Config) Validate(pool ChunkPooler) error {
	var errs []error
	if c.P < 0.0 || c.P > 1.0 {
		errs = append(errs, errors.New("invalid config: P must be between 0.0 and 1.0"))
	}
	if !pool.IsSupported(c.ChunkSize) {
		errs = append(
			errs,
			fmt.Errorf("invalid config: invalid chunk size %d must be one of %v", c.ChunkSize, pool.Sizes()),
		)
	}
	return errors.Join(errs...)
}

func DefaultConfig(pool ChunkPooler) Config {
	minChunkSize := pool.Sizes()[0]
	return Config{
		P:                     0.75,         // Favour reducing chunk pointers over memory waste.
		ChunkSize:             minChunkSize, // Initial chunk size is the smallest supported.
		CompactBytes:          64 * KiB,     // Compact 64KB bytes in a single compaction step.
		CompactDeadRatio:      0.3,          // Trigger compaction on dead/total > 30%.
		CompactDeadChunkRatio: 0.5,          // Trigger compaction on dead/chunk > 50%.
		ChunkDowngradeRatio:   0.5,          // Trigger downgrade on < 50% of lower resize threshold tier.
	}
}
