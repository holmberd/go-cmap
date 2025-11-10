package cmap

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/holmberd/go-cmap/internal/buffer"
)

const keysPerMigration = 10

// Stats represents bucket stats.
type Stats struct {
	Compactions uint64
}

func (s *Stats) Reset() {
	s.Compactions = 0
}

type bucketState int

const (
	stateIdle          bucketState = iota // Normal operation.
	stateCompacting                       // Compaction has started.
	stateMigratingKeys                    // Buffer is compacted and merged, migrating keys.
	stateDisabled                         // Compaction failed; buffer operations are disabled.
)

func (s bucketState) String() string {
	switch s {
	case stateIdle:
		return "idle"
	case stateCompacting:
		return "compacting"
	case stateMigratingKeys:
		return "migratingKeys"
	case stateDisabled:
		return "disabled"
	default:
		return fmt.Sprintf("bucketState(%d)", s)
	}
}

type bucket[P buffer.ChunkPooler] struct {
	sync.RWMutex
	logger    *slog.Logger
	chunkPool P

	// keys contains a map of key → offset maps.
	// keys[0] is the source key map, corresponding to the buffer being compacted.
	// keys[1] is the staging key map, receiving all new keys during compaction.
	keys [2]map[uint64]uint64

	// buf contains the encoded entries.
	// buf[0] is the source buffer, which is compacted in-place.
	// buf[1] is the staging buffer, receiving all new writes during compaction (before key-migration).
	buf [2]*buffer.Buffer[P]

	config     buffer.Config
	state      bucketState
	baseOffset uint64
	stats      Stats
}

func newBucket[P buffer.ChunkPooler](chunkPool P, config buffer.Config) *bucket[P] {
	b := &bucket[P]{}
	b.Init(chunkPool, config)
	return b
}

func (b *bucket[P]) isCompacting() bool {
	return b.state != stateIdle && b.state != stateDisabled
}

func (b *bucket[P]) isDisabled() bool {
	return b.state == stateDisabled
}

func (b *bucket[P]) UpdateStats(s *Stats) {
	s.Compactions += atomic.LoadUint64(&b.stats.Compactions)
}

func (b *bucket[P]) Init(chunkPool P, config buffer.Config) {
	b.chunkPool = chunkPool
	b.config = config
	b.keys[0] = make(map[uint64]uint64)
	b.buf[0] = buffer.New(chunkPool, b.logger, config)
	// Initializing keys[1] and buf[1] is deferred until compaction start,
	// since it requires the potentially next chunkSize, and it saves memory.
}

// Insert adds or updates an entry. It returns true if the entry was updated.
func (b *bucket[P]) Insert(key []byte, value []byte, hash uint64) (updated bool, err error) {
	b.Lock()
	defer b.Unlock()

	if b.isDisabled() {
		return false, buffer.ErrBufferCorrupted
	}
	if b.isCompacting() {
		// We run compact before write, since compact could free memory for the new write,
		// preventing a potentially blocking memory allocation.
		if err := b.compactStep(); err != nil {
			return updated, err
		}
	}

	var offset uint64
	if offset, updated = b.keys[0][hash]; updated {
		if err = b.buf[0].Delete(offset); err != nil {
			return false, err
		}
		delete(b.keys[0], hash)
	} else if offset, updated = b.keys[1][hash]; updated {
		i := 0
		if b.state != stateMigratingKeys {
			i = 1
		}
		if err := b.buf[i].Delete(offset - (b.baseOffset * uint64(i))); err != nil {
			return false, err
		}
		delete(b.keys[1], hash)
	}

	var buf *buffer.Buffer[P]
	var keys map[uint64]uint64
	var offsetAdjustment uint64
	switch b.state {
	case stateIdle:
		// Normal operation: write to the source buffer and map.
		buf = b.buf[0]
		keys = b.keys[0]
	case stateCompacting:
		// Buffer compaction: new writes are isolated to staging.
		buf = b.buf[1]
		keys = b.keys[1]

		// Adjust the key offset with its final offset in the new buffer after merge.
		// This simplifies migrating keys from the source to staging.
		offsetAdjustment = b.baseOffset
	case stateMigratingKeys:
		// Key migration: keys are migrated from keys[0] → keys[1].
		buf = b.buf[0]   // All data is in buf[0] after merge.
		keys = b.keys[1] // New keys go to keys[1].
	default:
		panic(fmt.Errorf("invalid bucket state: %v", b.state))
	}
	offset, _, err = buf.Write(key, value)
	if err != nil {
		return false, err
	}
	keys[hash] = offset + offsetAdjustment

	if !b.isCompacting() {
		if _, err := b.startCompact(); err != nil {
			return false, err
		}
	}
	return updated, nil
}

// Get returns the entry's value, or nil if not found.
// The returned entry is safe for modifications.
func (b *bucket[P]) Get(hash uint64) (key, value []byte, ok bool, err error) {
	b.RLock()
	defer b.RUnlock()

	if b.isDisabled() {
		return nil, nil, false, buffer.ErrBufferCorrupted
	}
	var offset uint64
	var found bool
	if offset, found = b.keys[0][hash]; found {
		key, value, err = b.buf[0].Read(offset)
		if err != nil {
			return nil, nil, false, err
		}
		return key, value, found, nil
	}
	if offset, found = b.keys[1][hash]; found {
		i := 0
		if b.state != stateMigratingKeys {
			i = 1
		}
		key, value, err = b.buf[i].Read(offset - (b.baseOffset * uint64(i)))
		if err != nil {
			return nil, nil, false, err
		}
		return key, value, found, nil
	}
	return nil, nil, false, nil
}

// Delete deletes an entry from the bucket and returns true if the entry existed.
func (b *bucket[P]) Delete(hash uint64) (existed bool, err error) {
	b.Lock()
	defer b.Unlock()

	if b.isDisabled() {
		return false, buffer.ErrBufferCorrupted
	}
	var offset uint64
	if offset, existed = b.keys[0][hash]; existed {
		if err := b.buf[0].Delete(offset); err != nil {
			return false, err
		}
		delete(b.keys[0], hash)
	} else if offset, existed = b.keys[1][hash]; existed {
		i := 0
		if b.state != stateMigratingKeys {
			i = 1
		}
		if err := b.buf[i].Delete(offset - (b.baseOffset * uint64(i))); err != nil {
			return false, err
		}
		delete(b.keys[1], hash)
	}

	if existed && !b.isCompacting() {
		b.startCompact()
	} else {
		if err := b.compactStep(); err != nil {
			return false, err
		}
	}
	return existed, nil
}

// Has returns a boolean indiciating if an entry exists with the key.
func (b *bucket[P]) Has(hash uint64) bool {
	b.RLock()
	defer b.RUnlock()

	for _, keyMap := range b.keys {
		if _, exists := keyMap[hash]; exists {
			return true
		}
	}
	return false
}

// Len returns number of entries in the bucket.
func (b *bucket[P]) Len() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.keys[0]) + len(b.keys[1])
}

// Reset clears the bucket (does not zero memory).
func (b *bucket[P]) Reset() *bucket[P] {
	b.Lock()
	defer b.Unlock()
	return b.ResetNoLock()
}

// ResetNoLock clears the bucket without acquiring a lock.
func (b *bucket[P]) ResetNoLock() *bucket[P] {
	if b.isCompacting() {
		panic(errors.New("illegal bucket reset during compaction"))
	}
	b.keys[0] = make(map[uint64]uint64)
	// Create new buffer to ensure its initialized with the initial config.
	b.buf[0] = buffer.New(b.chunkPool, b.logger, b.config)
	b.keys[1] = nil
	b.buf[1] = nil
	b.state = stateIdle
	b.baseOffset = 0
	return b
}

// startCompact starts the compactor for the underlying source buffer.
func (b *bucket[P]) startCompact() (bool, error) {
	if int(b.state) > int(stateIdle) {
		return false, nil
	}
	started, writeOffset, chunkSize, err := b.buf[0].StartCompactor(false)
	if err != nil {
		b.state = stateDisabled
		return false, err
	}
	if !started {
		return false, nil
	}
	atomic.AddUint64(&b.stats.Compactions, 1)

	b.baseOffset = buffer.GetAlignedOffset(chunkSize, writeOffset)
	cfg := b.config // Use initial config as template.
	cfg.ChunkSize = chunkSize
	if err := cfg.Validate(b.chunkPool); err != nil {
		// Unrecoverable programmer error.
		panic(fmt.Errorf("internal error: %w", err))
	}

	// TODO: pool buffers, i.e. reset and init them?
	// Depends if we create new instances often enough to warrant it.
	b.buf[1] = buffer.New(b.chunkPool, b.logger, cfg)
	b.keys[1] = make(map[uint64]uint64)
	b.state = stateCompacting
	return true, nil
}

// StopCompact reset the bucket to its normal state.
func (b *bucket[P]) stopCompact() {
	if b.state == stateIdle {
		return
	}
	b.state = stateIdle
	b.baseOffset = 0
	b.buf[1] = nil
	b.keys[1] = nil
}

// mergeBuffers merges the underlying source and staging buffer.
// The staging buffer is left in a valid but empty state after the operation.
func (b *bucket[P]) mergeBuffers() {
	if len(b.keys[1]) == 0 {
		b.stopCompact()
		return // No-op; write buffer is empty.
	}

	baseOffset, _ := b.buf[0].Move(b.buf[1])
	if baseOffset != b.baseOffset {
		panic(fmt.Errorf(
			"invariant violation: calculated compacted buffer size %d mismatch with actual size %d",
			b.baseOffset,
			baseOffset,
		))
	}

	// All data is in buf[0], but keys are fragmented across maps keys[0] and keys[1].
	// Start migration keys from the old to the new map.
	b.state = stateMigratingKeys
}

// migrateKeys moves up to n keys from the source map to the new map.
//
// NOTE: The backing array of a map will never shrink even if an entry
// is deleted, so moving the keys to a new map will allow GOGC to
// reclaim the memory in the old array and prevent memory leaks.
func (b *bucket[P]) migrateKeys(n int) {
	count := min(n, len(b.keys[0]))
	for k, v := range b.keys[0] {
		if count <= 0 {
			break
		}
		if _, ok := b.keys[1][k]; !ok {
			// Invariant: A key should exist in one or the other index, never in both.
			// That means if it exist in the new map it has been updated and we shouldn't overwrite it.
			b.keys[1][k] = v
		}
		delete(b.keys[0], k)
		count--
	}

	// If the old map has been drained, transition back to the normal state.
	if len(b.keys[0]) == 0 {
		b.keys[0] = b.keys[1]
		b.keys[1] = make(map[uint64]uint64)
		b.stopCompact()
	}
}

// compactStep runs one small, bounded step of background compaction work.
func (b *bucket[P]) compactStep() error {
	switch b.state {
	case stateIdle:
		return nil
	case stateCompacting:
		done, newOffsets, err := b.buf[0].Compact()
		if err != nil {
			b.state = stateDisabled
			return err
		}

		// Update the offset for any relocated keys during compaction.
		for _, off := range newOffsets {
			b.keys[0][off[0]] = off[1]
		}
		if done {
			b.mergeBuffers()
		}
	case stateMigratingKeys:
		b.migrateKeys(keysPerMigration)
	default:
		panic(fmt.Errorf("internal error: invalid bucket state: %v", b.state))
	}

	return nil
}
