package buffer

import (
	"encoding/binary"
	"errors"
	"io"
)

// Reader is a reader for reading entries in an Buffer.
// It implements the [io.Reader], [io.ByteReader] and [io.Seeker] interface.
type Reader[P ChunkPooler] struct {
	b         *Buffer[P]                  // Entries Buffer.
	bufIdx    int                         // Which internal buffer to read from (0 or 1).
	chunkIdx  int                         // Index of the current chunk.
	pos       int                         // Read position within the current chunk.
	headerBuf [binary.MaxVarintLen64]byte // Reusable Uvarint header buffer.
}

func NewReader[P ChunkPooler](b *Buffer[P]) *Reader[P] {
	return &Reader[P]{b: b}
}

// init is used to set up or reset the initial state of a Reader instance.
func (r *Reader[P]) init(bufIdx int, chunkIdx int, pos int) {
	r.bufIdx = bufIdx
	r.chunkIdx = chunkIdx
	r.pos = pos
}

func (r *Reader[P]) Offset() uint64 {
	return calcOffset(r.b.chunkSize[r.bufIdx], r.chunkIdx, r.pos)
}

// Reset resets the reader to the start of the buffer.
func (r *Reader[P]) Reset() *Reader[P] {
	r.chunkIdx = 0
	r.pos = 0
	return r
}

func (r *Reader[P]) IsEOF() bool {
	buf := r.b.buf[r.bufIdx]
	lastChunkIdx := len(buf) - 1
	return r.chunkIdx > lastChunkIdx ||
		(r.chunkIdx == lastChunkIdx && r.pos >= r.b.writePos[r.bufIdx])
}

// ReadHeader reads and returns the header and the number of bytes read.
// The error is [io.EOF] only if no bytes were read. If an EOF happens after reading
// some but not all the bytes, it returns [io.ErrUnexpectedEOF].
func (r *Reader[P]) ReadHeader() (h uint64, n int, err error) {
	// NOTE: The Uvarint is decoded manually as a performance optimization.
	// This avoids the overhead of an interface call to binary.ReadUvarint(r) and
	// simplifies tracking the exact number of bytes read.
	// A potential further optimization would be to unroll the loop and reduce branching.
	var i int
	for i = range len(r.headerBuf) {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && i > 0 {
				// If we get EOF after reading some bytes, it's an unexpected EOF.
				return 0, i, io.ErrUnexpectedEOF
			}
			return 0, 0, err
		}
		r.headerBuf[i] = b

		if b < 0x80 {
			break // End of Uvarint; high bit is not set.
		}
	}
	h, n = binary.Uvarint(r.headerBuf[:])
	if n <= 0 {
		return 0, 0, errors.New("invalid header: cannot be zero bytes")
	}
	return h, n, nil
}

// Seek sets the offset for the next read, modifying the reader's internal state.
// It implements the [io.Seeker] interface.
//
// Seeking to an offset before the start of the buffer is an error.
// Seeking to any positive offset is allowed, but if the new offset exceeds the size of
// the underlying buffer the behavior of subsequent I/O operations is implementation-dependent.
func (r *Reader[P]) Seek(offset int64, whence int) (ret int64, err error) {
	var newOffset int64
	chunkSize := r.b.chunkSize[r.bufIdx]
	maxOffset := int64(calcOffset(chunkSize, len(r.b.buf[r.bufIdx])-1, r.b.writePos[r.bufIdx]))

	switch whence {
	case io.SeekStart:
		// Offset is relative to the beginning of the buffer.
		newOffset = offset

	case io.SeekCurrent:
		// Offset is relative to the current position.
		currentOffset := calcOffset(chunkSize, r.chunkIdx, r.pos)
		newOffset = int64(currentOffset) + offset

	case io.SeekEnd:
		// Offset is relative to the end of the buffer.
		newOffset = maxOffset + offset // Offset is expected to be negative.

	default:
		return 0, errors.New("invalid whence")
	}

	// The new offset must be non-negative.
	if newOffset < 0 {
		return 0, errors.New("invalid offset: cannot be negative")
	}
	newChunkIdx, newPos := calcPosition(chunkSize, uint64(newOffset))
	r.chunkIdx = newChunkIdx
	r.pos = newPos
	return newOffset, nil
}

// Read reads data from the buffer into p and returns the number of bytes read.
func (r *Reader[P]) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil // No-op
	}
	buf := r.b.buf[r.bufIdx]
	if len(r.b.buf[r.bufIdx]) == 0 {
		return 0, io.EOF // No-op; empty buffer.
	}

	lastChunkIdx := len(buf) - 1
	lastPos := r.b.writePos[r.bufIdx] // Write head, can be equal to len(chunk).
	bytesToRead := len(p)
	bytesRead := 0
	for bytesRead < bytesToRead {
		if r.chunkIdx > lastChunkIdx || (r.chunkIdx == lastChunkIdx && r.pos >= lastPos) {
			return bytesRead, io.EOF
		}

		chunk := buf[r.chunkIdx]
		available := len(chunk) - r.pos
		if r.chunkIdx == lastChunkIdx {
			available = lastPos - r.pos
		}
		if available == 0 && r.chunkIdx != lastChunkIdx {
			r.chunkIdx++
			r.pos = 0
			continue // Move to the next chunk.
		}

		toCopy := min(available, bytesToRead-bytesRead)
		copy(p[bytesRead:], chunk[r.pos:r.pos+toCopy])
		r.pos += toCopy
		if r.pos >= len(chunk) {
			r.chunkIdx++
			r.pos = r.pos - len(chunk)
		}
		bytesRead += toCopy
	}
	return bytesRead, nil
}

// ReadByte reads a single byte from the buffer.
func (r *Reader[P]) ReadByte() (byte, error) {
	buf := r.b.buf[r.bufIdx]
	if len(buf) == 0 {
		return 0, io.EOF // No-op; empty bufer.
	}
	lastChunkIdx := len(buf) - 1
	if r.chunkIdx > lastChunkIdx || (r.chunkIdx == lastChunkIdx && r.pos >= r.b.writePos[r.bufIdx]) {
		// Optimization: Check for termination byte instead of checking boundaries each time.
		// Would require uvarint header to never equal e.g. 0xFF.
		return 0, io.EOF
	}

	chunk := buf[r.chunkIdx]
	b := chunk[r.pos]
	r.pos++
	if r.pos >= len(chunk) {
		r.chunkIdx++
		r.pos = 0
	}
	return b, nil
}
