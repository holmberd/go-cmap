// Package buffer implements writing, reading and deleting entries to and from an in-memory buffer.
package buffer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

const (
	KiB          = 1024
	MiB          = KiB * KiB
	MaxKeySize   = math.MaxUint16 // Maximum size allowed for a key, in bytes.
	MaxValueSize = 25 * MiB       // Maximum size allowed for a value, in bytes.
	keyLenSize   = 2              // KeyLen header size, in bytes (uint16).
)

var (
	ErrOffsetOutOfBounds = errors.New("offset is out of bounds")
	ErrBufferCorrupted   = errors.New("buffer is corrupted")
	ErrKeyTooLarge       = fmt.Errorf("key is too large (max %d bytes)", MaxKeySize)
	ErrValueTooLarge     = fmt.Errorf("value is too large (max %d bytes)", MaxValueSize)
	ErrKeyEmpty          = errors.New("key cannot be empty")
)

// ChunkPooler defines the contract for a memory pool that manages fixed-size chunks.
type ChunkPooler interface {
	Sizes() []int                          // Returns supported chunk sizes.
	IsSupported(chunkSize int) bool        // Checks if a chunk size is supported.
	Get(chunkSize int) []byte              // Get retrieves a chunk from a pool of the specified size.
	Put(c []byte)                          // Put returns a byte slice to the pool.
	Allocate(chunkSize int, numChunks int) // Allocates chunks in the pool (pre-warming).
}

// Stats represents buffer stats.
type stats struct {
	LiveBytes   uint64 // Total size of all live entries.
	DeadBytes   uint64 // Total size of all dead entries.
	PaddedBytes uint64 // DeadBytes that are alignment padding.
}

// Reset resets stats for re-use.
func (s *stats) reset() {
	s.LiveBytes = 0
	s.DeadBytes = 0
	s.PaddedBytes = 0
}

func (s *stats) equal(other stats) bool {
	return s.LiveBytes == other.LiveBytes &&
		s.DeadBytes == other.DeadBytes &&
		s.PaddedBytes == other.PaddedBytes
}

type bufferState int

const (
	stateIdle bufferState = iota
	stateCompacting

	// stateCorrupted indicates internal corruption.
	// All operations are disabled.
	stateCorrupted
)

type compactionState int

const (
	cpStateFind   compactionState = iota // Searching for the next entry to compact.
	cpStateCopy                          // Copying a live entry.
	cpStateCommit                        // Committing a copied entry.
)

// compactEntry represent the current entry being compacted.
type compactEntry struct {
	KeyHash        uint64
	Offset         uint64 // Offset in buf[0].
	NewOffset      uint64 // New offset in buf[1].
	BytesRemaining int
	TotalBytes     int
	IsDeleted      bool // Set if deleted in src buffer after compaction started.
}

// Buffer represents an append-only memory store for key-value entries.
// It supports writing, deleting and reading entries by their absolute offset.
//
// The buffer is designed to minimize GC overhead by managing data in a pool of
// fixed-size memory chunks. Compaction can be used to prevent memory waste due to
// fragmentation caused by repeated updates and deletions.
type Buffer[P ChunkPooler] struct {
	logger                *slog.Logger   // Default logger.
	chunkPool             P              // Pools of memory chunks.
	readerPool            *readerPool[P] // Pool of buffer readers.
	cpReader              *Reader[P]     // Reusable compaction buffer reader.
	resizeChunkThresholds []uint64       // Thresholds for resizing chunks.

	// buf contains uniform-sized chunks of bytes for storing encoded entries.
	// 	- buf[0] is the source buffer.
	// 	- buf[1] is the temporary destination buffer for compaction.
	buf [2][][]byte

	// Current write position (head) within the last chunk.
	// Since chunks are lazily allocated at the next Write call
	// it will be equal to len(chunk) when the last chunk is full.
	writePos [2]int

	chunkSize             [2]int                      // Memory chunk size in bytes.
	stats                 [2]stats                    // Buffer stats.
	state                 bufferState                 // State of the buffer.
	chunkDowngradeRatio   float64                     // Ratio of lower chunk size tier to trigger downgrade.
	compactDeadRatio      float64                     // Ratio of dead space to total to trigger compaction.
	compactDeadChunkRatio float64                     // Ratio of dead bytes to chunk size to trigger compaction.
	compactBytes          int                         // Number of bytes to copy per compaction step.
	cpState               compactionState             // Current state of a running compaction.
	cpNextEntryOffset     uint64                      // Offset for the next entry to be compacted.
	cpLastChunkIdx        int                         // Index of the last compacted chunk.
	cpLastEntryOffset     int64                       // Offset of the last compacted entry (last-commit-point).
	cpEntry               compactEntry                // Current live entry being compacted.
	headerBuf             [binary.MaxVarintLen64]byte // Reusable Uvarint header buffer.
	keyBuf                [MaxKeySize]byte            // Reusable key buffer.
}

// New creates a new, empty Buffer.
func New[P ChunkPooler](chunkPool P, logger *slog.Logger, config Config) *Buffer[P] {
	if err := config.Validate(chunkPool); err != nil {
		panic(err)
	}
	if !sort.IntsAreSorted(chunkPool.Sizes()) {
		panic(errors.New("chunk sizes must be sorted in ascending order"))
	}

	b := &Buffer[P]{
		logger:                logger,
		buf:                   [2][][]byte{},
		writePos:              [2]int{},
		chunkPool:             chunkPool,
		chunkSize:             [2]int{config.ChunkSize, config.ChunkSize},
		resizeChunkThresholds: calcChunkResizeThresholds(config.P, chunkPool.Sizes()),
		chunkDowngradeRatio:   config.ChunkDowngradeRatio,
		compactDeadRatio:      config.CompactDeadRatio,
		compactDeadChunkRatio: config.CompactDeadChunkRatio,
		compactBytes:          config.CompactBytes,
		cpLastChunkIdx:        -1, // Start at -1, as no chunks are initially compacted.
		cpLastEntryOffset:     -1, // Start at -1, as no offset is initially compacted.
	}
	b.cpReader = NewReader(b)
	b.readerPool = newReaderPool(b)
	return b
}

func (b *Buffer[P]) isCompacting() bool {
	return b.state == stateCompacting
}

func (b *Buffer[P]) isCorrupted() bool {
	return b.state == stateCorrupted
}

// setCorrupted sets the buffer state to corrupted and logs the provided error.
// It returns ErrBufferCorrupted.
func (b *Buffer[P]) setCorrupted(err error) error {
	b.state = stateCorrupted
	b.logger.Error(
		"Unrecoverable buffer corruption detected. All buffer operations are disabled",
		"error", err,
	)
	return ErrBufferCorrupted
}

// Offset returns the global write offset.
func (b *Buffer[P]) Offset() uint64 {
	return calcOffset(b.chunkSize[0], len(b.buf[0])-1, b.writePos[0])
}

// Print outputs a visual representation of the buffer for debugging purposes.
// It prints each chunk in each buffer as a row of space-separated hexadecimal values.
func (b *Buffer[P]) Print(w io.Writer) {
	if b == nil {
		return
	}
	for i, buffer := range b.buf {
		fmt.Fprintf(w, "--- Buffer %d ---\n", i)
		if len(buffer) == 0 {
			fmt.Fprintf(w, "(empty)\n\n")
			continue
		}

		// Calculate the padding width needed to align all chunk indexes.
		// The width is the number of digits in the highest chunk index.
		paddingWidth := 1
		if len(buffer) > 1 {
			paddingWidth = len(strconv.Itoa(len(buffer) - 1))
		}

		for j, chunk := range buffer {
			if chunk == nil {
				fmt.Fprintf(w, "%*d: [nil]\n", paddingWidth, j)
				continue
			}
			fmt.Fprintf(w, "%*d: [% x]\n", paddingWidth, j, chunk)
		}
		fmt.Println()
	}
}

func (b *Buffer[P]) Reset() {
	for i := range b.buf {
		b.truncate(i, 0)      // Truncate ensures chunks are released back to the pool.
		b.buf[i] = [][]byte{} // Unreference slice headers.
	}
	for i := range b.writePos {
		b.writePos[i] = 0
	}
	for i := range b.stats {
		b.stats[i].reset()
	}
	b.state = stateIdle
	b.cpNextEntryOffset = 0
	b.cpLastChunkIdx = -1
	b.cpLastEntryOffset = -1
	b.cpEntry = compactEntry{}
	b.cpReader.Reset()
}

// GetStats returns the buffer's aggregated stats.
// Not safe for concurrent use.
func (b *Buffer[P]) GetStats() stats {
	s := stats{
		LiveBytes:   b.stats[0].LiveBytes,
		DeadBytes:   b.stats[0].DeadBytes,
		PaddedBytes: b.stats[0].PaddedBytes, // Only present in the source buffer.
	}
	if b.isCompacting() {
		// During compaction stats are the total sum of the stats from b.buf[0] and b.buf[1];
		// since live entries are copied from b.buf[0] to b.buf[1], and entries can be marked
		// as deleted in both buffers.
		s.LiveBytes = b.stats[0].LiveBytes + b.stats[1].LiveBytes
		s.DeadBytes = b.stats[0].DeadBytes + b.stats[1].DeadBytes
	}
	return s
}

// Write appends the entry to the buffer, growing the buffer as needed.
// It returns the offset for the written entry and the n number of bytes written.
func (b *Buffer[P]) Write(key, value []byte) (offset uint64, n int, err error) {
	if b.isCompacting() {
		panic(errors.New("illegal write to buffer during compaction"))
	}
	if b.isCorrupted() {
		return 0, 0, ErrBufferCorrupted
	}
	if len(value) > MaxValueSize {
		return 0, 0, ErrValueTooLarge
	}

	offset = calcOffset(b.chunkSize[0], len(b.buf[0])-1, b.writePos[0])

	// Encode the entry payload and its header.
	payload, err := encodeEntry(key, value)
	if err != nil {
		return 0, 0, err
	}
	hl := binary.PutUvarint(b.headerBuf[:], encodeHeader(len(payload), 0))
	header := b.headerBuf[:hl]

	// Write header and payload.
	// TODO: Consider removing function call overhead.
	b.write(header, 0)
	b.write(payload, 0)
	n = hl + len(payload)
	b.stats[0].LiveBytes += uint64(n)
	return offset, n, nil
}

// write appends the contents of p to the buffer, growing the buffer as needed,
// and does not modify stats. The return value n is the length of p; err is always nil.
func (b *Buffer[P]) write(p []byte, bufIdx int) (n int, err error) {
	if len(p) == 0 {
		return len(p), err // No-op; empty bytes.
	}
	chunkSize := b.chunkSize[bufIdx]

	// Initialize buffer if it's empty.
	if len(b.buf[bufIdx]) == 0 {
		b.buf[bufIdx] = append(b.buf[bufIdx], b.chunkPool.Get(chunkSize))
		b.writePos[bufIdx] = 0
	}

	chunkIdx := len(b.buf[bufIdx]) - 1
	chunkOffset := b.writePos[bufIdx]
	availableBytes := chunkSize - chunkOffset
	remainingBytes := p

	// Fill any available space in the current chunk.
	bytesToWrite := min(len(remainingBytes), availableBytes)
	copy(b.buf[bufIdx][chunkIdx][chunkOffset:], remainingBytes[:bytesToWrite])
	chunkOffset += bytesToWrite
	remainingBytes = remainingBytes[bytesToWrite:]

	if len(remainingBytes) <= 0 {
		b.writePos[bufIdx] = chunkOffset
		return len(p), err // p fits within the last chunk (fast path for small writes).
	}

	// Calculate the number of new chunks required for writing the remainder of p.
	// This is a ceiling division: (a + b - 1) / b
	numChunks := (len(remainingBytes) + chunkSize - 1) / chunkSize

	// Grow the buffer to fit any new chunks.
	if n := numChunks - (cap(b.buf[bufIdx]) - len(b.buf[bufIdx])); n > 0 {
		b.buf[bufIdx] = append(b.buf[bufIdx][:cap(b.buf[bufIdx])], make([][]byte, n)...)[:len(b.buf[bufIdx])]
	}
	b.buf[bufIdx] = b.buf[bufIdx][:len(b.buf[bufIdx])+numChunks]

	b.chunkPool.Allocate(chunkSize, numChunks) // Pre-warm the pool.

	// Write the remaining bytes to the pre-allocated chunks.
	for range numChunks {
		chunkIdx++
		b.buf[bufIdx][chunkIdx] = b.chunkPool.Get(chunkSize)
		chunkOffset = 0
		bytesToWrite = min(len(remainingBytes), chunkSize)
		copy(b.buf[bufIdx][chunkIdx][chunkOffset:], remainingBytes[:bytesToWrite])
		chunkOffset += bytesToWrite
		remainingBytes = remainingBytes[bytesToWrite:]
	}

	b.writePos[bufIdx] = chunkOffset
	return len(p), err
}

// Read reads an entry at the given offset and returns its value.
//
// The error is [io.EOF] only if no bytes were read. If an EOF happens
// after reading some but not all the bytes, it returns [io.ErrUnexpectedEOF].
// The error is ErrOffsetOutOfBounds if the offset is out of bounds.
func (b *Buffer[P]) Read(offset uint64) (key, value []byte, err error) {
	if b.isCorrupted() {
		return nil, nil, ErrBufferCorrupted
	}
	bi := 0
	if b.state > stateIdle && int64(offset) <= b.cpLastEntryOffset {
		bi = 1
	}
	chunkIdx, pos := calcPosition(b.chunkSize[bi], offset)
	if chunkIdx >= len(b.buf[bi]) || (chunkIdx == len(b.buf[bi])-1 && pos >= b.writePos[bi]) {
		return nil, nil, ErrOffsetOutOfBounds
	}
	key, v, _, _, err := b.read(bi, chunkIdx, pos)
	return key, v, err
}

// read reads an entry at the given index and position.
// It returns the entry, n number of bytes read, and if the entry is deleted.
// The error is [io.EOF] only if no bytes were read. If an EOF happens after reading
// some but not all the bytes, it returns [io.ErrUnexpectedEOF].
func (b *Buffer[P]) read(
	bufIdx int,
	chunkIdx int,
	pos int,
) (key, value []byte, n int, deleted bool, err error) {
	if bufIdx >= len(b.buf) {
		panic(fmt.Errorf("internal error: buffer index %d out of range", bufIdx))
	}
	// Initialize a new reader that is safe for concurrent use.
	reader := b.readerPool.Get()
	defer b.readerPool.Put(reader)
	reader.init(bufIdx, chunkIdx, pos)

	// Read the entry header.
	h, hl, err := reader.ReadHeader()
	if err != nil {
		return nil, nil, 0, false, err
	}
	pl, flags := decodeHeader(h)
	if (flags&flagDeleted) != 0 || (flags&flagTombstone) != 0 {
		return nil, nil, 0, true, nil // No-op; entry is deleted.
	}

	// Copy the entry payload.
	entry := make([]byte, pl)
	bytesRead, err := reader.Read(entry)
	if err != nil {
		if err == io.EOF && bytesRead > 0 && bytesRead < pl {
			return nil, nil, 0, false, io.ErrUnexpectedEOF
		}
		return nil, nil, 0, false, err
	}
	key, value, err = decodeEntry(entry)
	if err != nil {
		return nil, nil, 0, false, err
	}
	return key, value, hl + pl, false, nil
}

// Delete deletes an entry at the given offset.
// The error is ErrOffsetOutOfBounds if the offset is out of bounds.
func (b *Buffer[P]) Delete(offset uint64) error {
	if b.isCorrupted() {
		return ErrBufferCorrupted
	}

	bi := 0
	if b.isCompacting() && int64(offset) <= b.cpLastEntryOffset {
		bi = 1
	}
	chunkIdx, pos := calcPosition(b.chunkSize[bi], offset)
	if chunkIdx >= len(b.buf[bi]) || (chunkIdx == len(b.buf[bi])-1 && pos >= b.writePos[bi]) {
		return ErrOffsetOutOfBounds
	}
	n, err := b.delete(bi, chunkIdx, pos)
	if err != nil {
		return err
	}
	b.stats[bi].LiveBytes -= uint64(n)
	b.stats[bi].DeadBytes += uint64(n)
	return nil
}

// delete marks an entry as deleted at the given offset, and does not modify stats.
// It returns the n number of bytes deleted and any error.
func (b *Buffer[P]) delete(bufIdx int, chunkIdx int, pos int) (n int, err error) {
	reader := b.readerPool.Get()
	defer b.readerPool.Put(reader)
	reader.init(bufIdx, chunkIdx, pos)

	h, _, err := reader.ReadHeader()
	if err != nil {
		return 0, err
	}

	pl, flags := decodeHeader(h)
	if b.isCompacting() && bufIdx == 0 {
		// If we are compacting, set tombstone flag. The flag will be changed to deleted when
		// the entry is compacted.
		//
		// NOTE: The compactor copies tombstones entries to the destination buffer to preserve
		// the final predicted size (initial LiveBytes) of the compacted buffer. This is a
		// requirement to offset any new write offsets in a write buffer during compaction.
		h = encodeHeader(pl, flags|flagTombstone)
	} else {
		h = encodeHeader(pl, flags|flagDeleted)
	}

	hl := binary.PutUvarint(b.headerBuf[:], h)
	copy(b.buf[bufIdx][chunkIdx][pos:], b.headerBuf[:hl]) // Write header.
	return hl + pl, nil
}

// StartCompactor attempts to starts the compaction process. See [Compact].
//
// The compactor will run for one of tree reasons:
//  1. To reclaim space if the ratio of dead-to-live bytes is too high.
//  2. To resize chunks if the buffer has grown large enough to benefit from a larger chunk size.
//  3. When force is true, mainly useful in tests to force compaction.
//
// If started, it returns true, and the expected write offset and chunk size for the compacted buffer.
// The compacted buffer's final write offset is equal to the size of its live entries in the source
// buffer at the moment compaction began (invariant).
func (b *Buffer[P]) StartCompactor(force bool) (
	started bool,
	offset uint64,
	chunkSize int,
	err error,
) {
	if b.isCompacting() {
		return false, 0, 0, nil // No-op; compactor has already started.
	}
	if b.isCorrupted() {
		return false, 0, 0, ErrBufferCorrupted
	}
	if !force && b.compactBytes <= 0 {
		return false, 0, 0, nil // No-op; compactor is disabled.
	}
	if b.Offset() != b.stats[0].LiveBytes+b.stats[0].DeadBytes {
		// Sanity check that offset and stats are aligned.
		return false, 0, 0, b.setCorrupted(errors.New("invariant violation: offset and stats mismatch"))
	}

	// Check if the buffer has grown large/small enough to warrant a chunk size upgrade/downgrade.
	newChunkSize := determineChunkSize(
		b.chunkPool.Sizes(),
		b.chunkSize[0],
		b.stats[0].LiveBytes,
		b.resizeChunkThresholds,
		b.chunkDowngradeRatio,
	)
	if force {
		b.chunkSize[1] = newChunkSize
		goto done
	}
	if len(b.buf[0]) == 0 {
		return false, 0, 0, nil // No-op; buffer is empty.
	}
	if newChunkSize != b.chunkSize[1] {
		b.chunkSize[1] = newChunkSize
		goto done
	}
	if b.stats[0].DeadBytes < uint64(b.compactDeadChunkRatio*float64(b.chunkSize[0])) {
		return false, 0, 0, nil // No-op; not enough dead bytes.
	}
	if b.stats[0].LiveBytes > 0 &&
		float64(b.stats[0].DeadBytes-b.stats[0].PaddedBytes) <
			b.compactDeadRatio*float64(b.stats[0].LiveBytes+b.stats[0].DeadBytes) {
		// NOTE: Padded alignment bytes are ignored to prevent edge-case feedback loops when oscillating
		// around a dead ratio boundary.
		return false, 0, 0, nil // No-op: not enough dead bytes.
	}

done:
	b.cpNextEntryOffset = 0
	b.cpLastChunkIdx = -1
	b.cpLastEntryOffset = -1
	b.cpEntry = compactEntry{}
	b.cpReader.Reset()
	b.state = stateCompacting
	return true, b.stats[0].LiveBytes, b.chunkSize[1], nil
}

// Compact performs a single incremental step of compaction: reclaiming space
// from deleted entries.
//
// It returns true when the buffer has been successfully processed, a slice
// of [keyHash, newOffset] pairs for entries that were relocated during this step,
// and any error.
//
// Compact is not safe for concurrent use and must be protected by an
// external write-lock.
//
// To prevent unbounded execution time on long runs of dead data, the function
// will scan a limited number of deleted entries before yielding. As a result,
// it is not guaranteed that any bytes will be reclaimed in a single step.
func (b *Buffer[P]) Compact() (done bool, newOffsets [][2]uint64, err error) {
	if b.isCorrupted() {
		return false, nil, ErrBufferCorrupted
	}
	if !b.isCompacting() {
		panic(errors.New("illegal call to compact: compaction is not running"))
	}
	defer func() {
		if r := recover(); r != nil {
			err = b.setCorrupted(fmt.Errorf("panic during compaction: %v", r))
		}
	}()

	// Compaction process:
	// - Find any live entries.
	// - Copy data from the source to the new destination buffer, allocating new chunks.
	// - Releases compacted chunks back to the pool.
	// - Update statistics and buffer state.
	deadEntryVisits := 10 // Max number of deleted entries to visit.
	bytesWritten := 0
	if len(b.buf[0]) == 0 {
		// NOTE: only checking if LiveBytes == 0 wouldn't be valid. Since dead entries might
		// be tombstoned entries that need to be copied and marked as deleted.
		goto done // No-op for empty buffer.
	}
	for bytesWritten < b.compactBytes {
		switch b.cpState {

		// Find the next live entry to copy from the src to the dst buffer (buf[0] -> buf[1]).
		case cpStateFind:
			for deadEntryVisits > 0 {
				if _, err := b.cpReader.Seek(int64(b.cpNextEntryOffset), io.SeekStart); err != nil {
					return false, nil, b.setCorrupted(
						fmt.Errorf("internal error: failed to seek reader to offset %d: %w", b.cpNextEntryOffset, err),
					)
				}
				chunkIdx, readPos := b.cpReader.chunkIdx, b.cpReader.pos
				h, hl, err := b.cpReader.ReadHeader()
				if err != nil {
					if err == io.EOF {
						goto done
					}
					return false, nil, b.setCorrupted(
						fmt.Errorf("internal error: failed to read header at offset %d: %w", b.cpNextEntryOffset, err),
					)
				}
				pl, flags := decodeHeader(h)
				totalLen := hl + pl

				// Sanity check that the entry doesn't exceed the length of the buffer.
				if b.cpNextEntryOffset+uint64(totalLen) > b.Offset() {
					return false, newOffsets, b.setCorrupted(
						fmt.Errorf(
							"internal error: "+
								"invalid entry found during compaction: entry length %d is out of range of buffer with length %d",
							b.cpNextEntryOffset+uint64(totalLen),
							b.Offset(),
						),
					)
				}

				if (flags & flagDeleted) != 0 {
					// Deleted flag is set, check next entry.
					deadEntryVisits--
					b.releaseCompactedChunks()
					b.cpLastEntryOffset = int64(b.cpNextEntryOffset)
					b.cpNextEntryOffset += uint64(totalLen)
					continue
				}

				b.cpEntry.IsDeleted = false
				if (flags & flagTombstone) != 0 {
					// Tombstone flag is set, change entry to deleted before copy.
					// invariant: Compacted buffer should not contain tombstone entries.
					flags |= flagDeleted          // Set deleted flag.
					flags &= ^byte(flagTombstone) // Clear tombstone flag.
					hLen := binary.PutUvarint(b.headerBuf[:], encodeHeader(pl, flags))
					copy(b.buf[0][chunkIdx][readPos:], b.headerBuf[:hLen])
					b.cpEntry.IsDeleted = true
				}

				// Read the key from the entry payload.
				keyLenBuf := b.keyBuf[:2]
				bytesRead, err := b.cpReader.Read(keyLenBuf)
				if err != nil || bytesRead < len(keyLenBuf) {
					return false, newOffsets, b.setCorrupted(
						fmt.Errorf("internal error: failed to read entry key length at offset %d: %w", b.cpNextEntryOffset, err),
					)
				}
				keyLen := int(binary.LittleEndian.Uint16(keyLenBuf))
				bytesRead, err = b.cpReader.Read(b.keyBuf[:keyLen])
				if err != nil || bytesRead < keyLen {
					return false, newOffsets, b.setCorrupted(
						fmt.Errorf("internal error: failed to read entry key at offset %d: %w", b.cpNextEntryOffset, err),
					)
				}

				// Rewind the reader's position to the start of the entry before copying.
				// TODO: Add reader.Peek()
				b.cpReader.chunkIdx = chunkIdx
				b.cpReader.pos = readPos

				b.cpEntry.KeyHash = xxhash.Sum64(b.keyBuf[:keyLen])
				b.cpEntry.Offset = b.cpNextEntryOffset                                           // Offset in buf[0].
				b.cpEntry.NewOffset = calcOffset(b.chunkSize[1], len(b.buf[1])-1, b.writePos[1]) // Offset in buf[1].
				b.cpEntry.BytesRemaining, b.cpEntry.TotalBytes = totalLen, totalLen
				b.cpNextEntryOffset += uint64(totalLen)
				b.cpState = cpStateCopy // Transition to the next state.
				break
			}

			if deadEntryVisits <= 0 {
				return false, newOffsets, nil
			}

		// Copy the entry from the src to the dst buffer in batches until we reach the write quota,
		// or there are no bytes remaining to copy.
		case cpStateCopy:
			if b.cpReader.chunkIdx >= len(b.buf[0]) {
				return false, newOffsets, b.setCorrupted(
					fmt.Errorf(
						"internal error: reader out of range of buffer: chunk index out of range [%d] with length %d",
						b.cpReader.chunkIdx,
						len(b.buf[0]),
					),
				)
			}
			bytesToRead := min(
				len(b.buf[0][b.cpReader.chunkIdx])-b.cpReader.pos, // Bytes left to read from chunk.
				b.cpEntry.BytesRemaining,                          // Bytes left to read from entry.
				(b.compactBytes - bytesWritten),                   // Bytes left to read from quota.
			)

			if bytesToRead > 0 {
				// Copy the entries bytes to the dst buffer and manually adjust the reader's offset.
				// Since manual adjustment avoids an unnecessary intermediate read copy.
				b.write(b.buf[0][b.cpReader.chunkIdx][b.cpReader.pos:b.cpReader.pos+bytesToRead], 1)
				b.cpReader.pos += bytesToRead
				bytesWritten += bytesToRead
				b.cpEntry.BytesRemaining -= bytesToRead
			}

			// If we've completed this chunk, move to the next.
			if b.cpReader.pos >= len(b.buf[0][b.cpReader.chunkIdx]) {
				b.cpReader.chunkIdx++
				b.cpReader.pos = 0
			}

			if b.cpEntry.BytesRemaining <= 0 {
				// NOTE: If there is no write quota left at this stage,
				// the entry will get committed in the next call.
				b.cpState = cpStateCommit // Transition to the next state.
			}

		// Attempt to commit the copied entry by recording its new offset in the dst buffer.
		case cpStateCommit:
			if !b.cpEntry.IsDeleted {
				// Handle concurrent delete: If the entry in the src buffer was deleted while it was being copied,
				// we need to mark the copied entry in the dst buffer as deleted.
				if _, err := b.cpReader.Seek(int64(b.cpEntry.Offset), io.SeekStart); err != nil {
					return false, newOffsets, b.setCorrupted(
						fmt.Errorf("internal error: failed to seek reader to offset %d: %w", b.cpEntry.Offset, err),
					)
				}

				h, _, err := b.cpReader.ReadHeader()
				if err != nil {
					return false, newOffsets, b.setCorrupted(
						fmt.Errorf("internal error: failed to read header at offset %d: %w", b.cpEntry.Offset, err),
					)
				}

				pl, flags := decodeHeader(h)
				if (flags & flagTombstone) != 0 {
					flags |= flagDeleted          // Set deleted flag.
					flags &= ^byte(flagTombstone) // Clear tombstone flag.
					hl := binary.PutUvarint(b.headerBuf[:], encodeHeader(pl, flags))
					chunkIdx, pos := calcPosition(b.chunkSize[1], b.cpEntry.NewOffset)
					copy(b.buf[1][chunkIdx][pos:], b.headerBuf[:hl])
					b.cpEntry.IsDeleted = true
				}
			}
			b.releaseCompactedChunks()

			// Update the reference to the last compacted entry offset in the src buffer.
			b.cpLastEntryOffset = int64(b.cpEntry.Offset)

			if !b.cpEntry.IsDeleted {
				// Record the entry's new offset in the dst buffer to the caller.
				newOffsets = append(newOffsets, [2]uint64{b.cpEntry.KeyHash, uint64(b.cpEntry.NewOffset)})

				b.stats[1].LiveBytes += uint64(b.cpEntry.TotalBytes)
				b.stats[0].LiveBytes -= uint64(b.cpEntry.TotalBytes)
			} else {
				// Copied deleted entries new offsets are not recorded to the caller.
				b.stats[1].DeadBytes += uint64(b.cpEntry.TotalBytes)
				b.stats[0].DeadBytes -= uint64(b.cpEntry.TotalBytes)
			}
			b.cpEntry = compactEntry{}
			b.cpState = cpStateFind // Transition back to the beginning.
		}
	}
	return false, newOffsets, nil

done:
	// Finalize compaction.
	b.buf[0] = b.buf[1]
	b.buf[1] = [][]byte{}
	b.chunkSize[0] = b.chunkSize[1]
	b.writePos[0] = b.writePos[1]
	b.writePos[1] = 0

	// Swapping stats resets any paddedBytes,
	// since a destination buffer is never padded.
	b.stats[0] = b.stats[1]
	b.stats[1].reset()

	// Reset state.
	b.cpReader.Reset()
	b.cpNextEntryOffset = 0
	b.cpLastChunkIdx = -1
	b.cpLastEntryOffset = -1
	b.state = stateIdle
	return true, newOffsets, nil
}

// releaseCompactedChunks releases any chunks freed during compaction back to the pool.
// This operation is destructive to the source buffer.
func (b *Buffer[P]) releaseCompactedChunks() {
	// Calculate the chunk index where the next entry starts.
	// We can release any chunk with an index less than this.
	nextEntryChunkIdx, _ := calcPosition(b.chunkSize[0], b.cpNextEntryOffset)

	// Release any fully compacted chunks.
	startIdx := b.cpLastChunkIdx + 1
	for i := startIdx; i < nextEntryChunkIdx; i++ {
		if b.buf[0][i] != nil {
			b.chunkPool.Put(b.buf[0][i])
			b.buf[0][i] = nil
		}
	}

	// If we released any chunks, the new last compacted chunk is the last one in our loop.
	if nextEntryChunkIdx > startIdx {
		b.cpLastChunkIdx = nextEntryChunkIdx - 1
	}
}

// Move transfers ownership of data from a source buffer to the destination buffer.
// The data is moved to the end of the destination buffer and the the source buffer
// is left in a valid but empty state.
// It returns the start offset where the new data begins and the new write offset.
//
// As a boundary alignment optimization, this function pads the last chunk, if it has
// remaining space and is not aligned with a chunk boundary, with a deleted entry.
// The returned startOffset is the offset after any padding.
func (b *Buffer[P]) Move(srcBuf *Buffer[P]) (startOffset uint64, offset uint64) {
	if b.isCompacting() || srcBuf.isCompacting() {
		panic(errors.New("illegal call to move: source or destination buffer is compacting"))
	}
	if b.isCorrupted() || srcBuf.isCorrupted() {
		panic(errors.New("illegal call to move: source or destination buffer is corrupted"))
	}
	if b.chunkSize[0] != srcBuf.chunkSize[0] {
		panic(errors.New("illegal call to move: buffer chunk size mismatch"))
	}

	defer func() {
		// Complete data ownership transfer to prevent Use-After-Free.
		srcBuf.resetWithoutRelease()
	}()

	chunkSize := b.chunkSize[0]
	src := &srcBuf.buf[0]
	srcOffset := srcBuf.Offset()

	if len(*src) == 0 {
		off := b.Offset()
		return off, off // No-op; buf is empty.
	}
	if len(b.buf[0]) == 0 {
		b.buf[0] = *src
		b.writePos[0] = srcBuf.writePos[0]
		b.stats[0].LiveBytes = srcBuf.stats[0].LiveBytes
		b.stats[0].DeadBytes = srcBuf.stats[0].DeadBytes
		return 0, srcOffset
	}

	// Append the chunks at the start of the next chunk in the dst buffer.
	//
	// If the last chunk in the dst buffer has remaining space, it will be padded.
	// This ensure that the buffer is aligned with the chunk boundary, and avoids an
	// otherwise slow, byte-by-byte copy of data from src to dst.
	remainingSpace := int(chunkSize - b.writePos[0])
	startOffset = calcOffset(chunkSize, len(b.buf[0]), 0)

	if remainingSpace > 0 {
		headerLen, payloadLen, err := findUvarintHeaderLen(remainingSpace)
		if err != nil {
			// This should only occur if a uvarint header encoding a zero length payload
			// can no longer fit in 1 byte.
			panic(fmt.Errorf("invariant violation: %v", err))
		}

		// Write a fixed length header, which pads the header, for the case where the natural
		// Uvarint encoded header and the payloadLen would otherwise not fit in the remaining space.
		hl := putUvarintFixed(b.headerBuf[:], encodeHeader(payloadLen, flagDeleted), headerLen)
		if hl != headerLen || headerLen+payloadLen != remainingSpace {
			// Sanity check that our calculation was correct.
			panic(fmt.Errorf("internal error: chunk padding calculation mismatch"))
		}

		copy(b.buf[0][len(b.buf[0])-1][b.writePos[0]:], b.headerBuf[:hl]) // Write the header.
		b.stats[0].DeadBytes += uint64(remainingSpace)
		b.stats[0].PaddedBytes += uint64(remainingSpace)
	}

	b.buf[0] = append(b.buf[0], *src...)
	b.writePos[0] = srcBuf.writePos[0]
	b.stats[0].LiveBytes += srcBuf.stats[0].LiveBytes
	b.stats[0].DeadBytes += srcBuf.stats[0].DeadBytes
	return startOffset, calcOffset(chunkSize, len(b.buf[0])-1, b.writePos[0])
}

// resetWithoutRelease resets the buffers state without returning any memory chunks to the pool.
func (b *Buffer[P]) resetWithoutRelease() {
	for i := range b.buf {
		b.buf[i] = [][]byte{}
	}
	for i := range b.writePos {
		b.writePos[i] = 0
	}
	for i := range b.stats {
		b.stats[i].reset()
	}
	b.state = stateIdle
	b.cpNextEntryOffset = 0
	b.cpLastChunkIdx = -1
	b.cpLastEntryOffset = -1
	b.cpEntry = compactEntry{}
	b.cpReader.Reset()
}

// findUvarintHeaderLen finds the header length that can encode
// a payload length such that headerLen + payloadLen = cap.
// It returns the header length, payload length, and any error.
//
// NOTE: The returned header length can be larger than required for
// encoding the payload length. Use [buffer.putUvarintFixed] to pad
// the header when encoding it.
func findUvarintHeaderLen(cap int) (headerLen int, payloadLen int, err error) {
	if cap < 1 {
		return 0, 0, errors.New("cannot fit smallest possible header")
	}

	// Perform a greedy search for a header length (n) that can encode the payload.
	// Initial guess is the smallest possible n header length (1 byte).
	for n := 1; n <= binary.MaxVarintLen64; {
		p := cap - n
		if p < 0 {
			return 0, 0, errors.New("cannot fit smallest possible header")
		}
		actual := uvarintLen(encodeHeader(p, 0))
		if n == actual {
			return n, p, nil // Found optimal n.
		}
		if n > actual {
			// Reached boundary where there's no header length (n) that satisfies the equation:
			// S(t, n) = Uvarint(t - n) + (t - n)
			//
			// E.g: S(33, n) = 33:
			// S(33, 1) = 2 + 32 = 34; > 33
			// S(33, 2) = 1 + 31 = 32; < 33
			return n, p, nil // First n that can encode the payload.
		}
		n = actual // n didn't fit, update guess.
	}

	// In practice this should be unreachable for MaxVarintLen64.
	return 0, 0, errors.New("cannot fit largest possible header")
}

// putUvarintFixed is similar to [binary.PutUvarint] but writes a fixed number of n bytes,
// regardless of the value's natural Uvarint encoding length.
// It will panic if n is too small or too large to encode the value.
func putUvarintFixed(buf []byte, x uint64, n int) int {
	if n < uvarintLen(x) || n > binary.MaxVarintLen64 {
		panic(fmt.Errorf("invalid fixed uvarint size %d", n))
	}

	// Encodes the value x up to the n-1 byte with a continuation bit (MSB=1).
	i := 0
	for {
		// Stop encoding the data bits when either:
		// - We've reached the second-to-last padding byte (n-1).
		// - The remaining value of x can fit into the next 7 data bits.
		if i == n-1 || x < 0x80 {
			break
		}
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}

	if i == n-1 {
		buf[i] = byte(x)
	} else {
		buf[i] = byte(x) | 0x80

		// Zero-fill the padding bytes with MBS=1 set.
		for j := i + 1; j < n-1; j++ {
			buf[j] = 0x80
		}
		buf[n-1] = 0x00 // Stop bit (MSB=0).
	}
	return n
}

// uvarintLen returns the length of a uint64 integer when Uvarint-encoded.
func uvarintLen(x uint64) int {
	switch {
	case x < 1<<7:
		return 1
	case x < 1<<14:
		return 2
	case x < 1<<21:
		return 3
	case x < 1<<28:
		return 4
	case x < 1<<35:
		return 5
	case x < 1<<42:
		return 6
	case x < 1<<49:
		return 7
	case x < 1<<56:
		return 8
	case x < 1<<63:
		return 9
	default:
		return binary.MaxVarintLen64 // 10 bytes.
	}
}

// Truncate discards all bytes from a buffer after the specified offset.
// It continues to use the same allocated storage and does not modify stats.
func (b *Buffer[P]) truncate(bufIdx int, toOffset uint64) {
	if len(b.buf[bufIdx]) == 0 {
		return // No-op; buffer is empty.
	}
	chunkSize := b.chunkSize[bufIdx]
	chunkIdx, pos := calcPosition(chunkSize, toOffset)

	// Normalize the new position to reflect that when the last chunk is full writePos == len(chunk).
	if toOffset > 0 && pos == 0 {
		chunkIdx--
		pos = chunkSize
	}

	lastChunkIdx := len(b.buf[bufIdx]) - 1
	if chunkIdx > lastChunkIdx || (chunkIdx == lastChunkIdx && pos >= b.writePos[bufIdx]) {
		return // No-op; offset is outside of bounds.
	}

	// Put any truncated chunks back into the pool.
	if len(b.buf[bufIdx])-1 > chunkIdx {
		for i := lastChunkIdx; i >= chunkIdx; i-- {
			b.chunkPool.Put(b.buf[bufIdx][i])
		}
		b.buf[bufIdx] = b.buf[bufIdx][:chunkIdx]
	}
	b.writePos[bufIdx] = pos
}

// GetAlignedOffset returns the start offset of the next chunk
// boundary from a given offset. If the provided offset is already
// aligned on a chunk boundary, it returns the offset itself.
func GetAlignedOffset(chunkSize int, offset uint64) uint64 {
	if offset == 0 {
		return 0
	}
	chunkIdx, pos := calcPosition(chunkSize, offset)
	if pos == 0 {
		return offset
	}
	return calcOffset(chunkSize, chunkIdx+1, 0)
}

// calcPosition calculates a position from an offset.
func calcPosition(chunkSize int, offset uint64) (chunkIdx int, pos int) {
	return int(offset) / chunkSize, int(offset) % chunkSize
}

// calcOffset calculates an offset from a position.
func calcOffset(chunkSize int, chunkIdx int, pos int) uint64 {
	if chunkIdx < 0 {
		// Since we don't store the write header chunkIdx we need to
		// correct for the case where len(buf)-1 < 0.
		chunkIdx = 0
	}
	return uint64(chunkIdx*chunkSize + pos)
}

// encodeEntry encodes the key, and value into a byte slice.
//
// Layout:
//
//	| Offset | Length | Field      | Type     | Encoding
//	|--------|--------|------------|----------|-----------
//	| 0      | 2      | KeyLen (N) | uint16   | LE
//	| 2      | N      | Key        | []byte   |
//	| N      | M      | Value      | []byte   |
func encodeEntry(key, value []byte) ([]byte, error) {
	keyLen := len(key)
	if keyLen == 0 {
		return nil, ErrKeyEmpty
	}
	if keyLen > MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	totalKeyLen := keyLenSize + keyLen
	entry := make([]byte, totalKeyLen+len(value))
	binary.LittleEndian.PutUint16(entry[0:keyLenSize], uint16(keyLen))
	copy(entry[keyLenSize:totalKeyLen], key)
	copy(entry[totalKeyLen:], value)
	return entry, nil
}

// decodeEntry decodes the entry and returns its key-value pair.
// NOTE: The returned value slice is not safe for modifications without clone.
func decodeEntry(entry []byte) (key, value []byte, err error) {
	if len(entry) < keyLenSize {
		return nil, nil, errors.New("invalid entry: entry is too small to contain a key")
	}
	keyLen := int(binary.LittleEndian.Uint16(entry[0:keyLenSize]))
	return entry[keyLenSize : keyLenSize+keyLen],
		entry[keyLenSize+keyLen:],
		nil
}

const (
	flagDeleted   byte = 1 << 0 // 1
	flagTombstone byte = 1 << 1 // 2
	flagTest      byte = 1 << 2 // 4
)

// NOTE: Max 7 flags bits to ensure the minimum header size is 1 byte.
// This is required for padding a chunk with 1 byte remaining space
// without its encoded header crossing a chunk boundary.
const numFlagBits = 2

// encodeHeader encodes the payload length and flags into a uint64.
func encodeHeader(payloadLen int, flags byte) uint64 {
	return uint64(payloadLen<<numFlagBits) | uint64(flags&byte((1<<numFlagBits)-1))
}

// decodeHeader extracts the payload length and flags from an encoded uint64.
func decodeHeader(h uint64) (payloadLen int, flags byte) {
	flags = byte(h) & byte((1<<numFlagBits)-1)
	payloadLen = int(h >> numFlagBits)
	return payloadLen, flags
}

// calcChunkResizeThresholds calculates and returns the buffer size thresholds
// for upgrading between different chunk size tiers, based on the parameter p.
//
// The performance trade-off is controlled by the parameter p, a value between 0.0
// and 1.0:
//   - p = 1.0: 100% priority on reducing the number of chunks (less GC pressure).
//   - p = 0.0: 100% priority on reducing wasted space (for memory efficiency).
//   - p = 0.5: A 50/50 balance.
//
// The returned slice contains the calculated thresholds in bytes. The threshold at
// index (i) is the buffer size to trigger an upgrade from chunkSizes[i] to chunkSizes[i+1].
//
// The function will panic if the provided p is outside the [0.0, 1.0] range.
func calcChunkResizeThresholds(p float64, chunkSizes []int) []uint64 {
	if p < 0.0 || p > 1.0 {
		panic(errors.New("p must be between 0.0 and 1.0"))
	}

	// TODO: Handle custom case with a single chunkSize.
	if len(chunkSizes) < 2 {
		panic(errors.New("at least two chunk sizes are required to calculate a threshold"))
	}

	thresholds := make([]uint64, len(chunkSizes)-1)
	for i := 0; i < len(chunkSizes)-1; i++ {
		// T = (2 * (1 - p)) * chunkSizes[i + 1]
		thresholds[i] = uint64(math.Ceil(2.0 * (1.0 - p) * float64(chunkSizes[i+1])))
	}
	return thresholds
}

// determineChunkSize returns an optimal chunk size based on the buffer's
// live size and resize thresholds, supporting both upgrades and downgrades.
func determineChunkSize(
	chunkSizes []int,
	chunkSize int,
	bufferSize uint64,
	thresholds []uint64,
	chunkDowngradeRatio float64,
) (newChunkSize int) {
	newChunkSize = chunkSize
	for {
		ci := -1 // Find the index of the current, or proposed, chunk size.
		for i, size := range chunkSizes {
			if size == newChunkSize {
				ci = i
				break
			}
		}
		if ci == -1 {
			return chunkSize // No-op: Current size is not supported.
		}
		if ci < len(chunkSizes)-1 {
			// Check for upgrade if not at largest size.
			if bufferSize > thresholds[ci] {
				newChunkSize = chunkSizes[ci+1]
				continue
			}
		}
		if ci > 0 {
			// Check for downgrade if not at the smallest size.
			if bufferSize < uint64(float64(thresholds[ci-1])*chunkDowngradeRatio) {
				newChunkSize = chunkSizes[ci-1]
				continue
			}
		}
		return newChunkSize
	}
}
