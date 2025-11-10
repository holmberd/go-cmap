package buffer

// White box testing of buffer functionality.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/holmberd/go-cmap/internal/testutils"
)

type testEntry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// NewTestBuffer is a helper for creating a new buffer with a default configuration for testing.
func NewTestBuffer(
	t *testing.T,
	chunkSize int,
	compactBytes int,
) (*Buffer[*testutils.MockChunkPool], *testutils.MockChunkPool) {
	t.Helper()
	pool := &testutils.MockChunkPool{}
	config := Config{
		P:                     0.75,
		ChunkSize:             chunkSize,
		CompactBytes:          compactBytes,
		CompactDeadRatio:      0,
		CompactDeadChunkRatio: 0,
		ChunkDowngradeRatio:   0.25,
	}
	if err := config.Validate(pool); err != nil {
		t.Fatal(err)
	}
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil)) // Discard logs during testing.
	b := New(pool, discardLogger, config)
	return b, pool
}

// SetupTestBufferWithCleanup is a helper function for creating a new buffer with cleanup.
func SetupTestBufferWithCleanup(
	t *testing.T,
	chunkSize int,
	compactBytes int,
) (*Buffer[*testutils.MockChunkPool], *testutils.MockChunkPool) {
	t.Helper()
	b, p := NewTestBuffer(t, chunkSize, compactBytes)
	t.Cleanup(func() {
		b.Reset()
		p.Reset()
	})
	return b, p
}

// generateBytes returns a byte slice of a size relative to the chunkSize.
// numChunks specifies the minimum number of chunks the data should span,
// e.g. 2.5 for at least two and a half chunks. The slice is filled with a
// predictable pattern for verification.
func generateBytes(t *testing.T, chunkSize int, numChunks float64) []byte {
	t.Helper()
	if chunkSize <= 0 || numChunks <= 0 {
		t.Fatal("chunkSize and numChunks must be > 0")
	}
	data := make([]byte, int(float64(chunkSize)*numChunks))
	for i := range data {
		data[i] = byte('a' + (i % 26)) // fill with repeating a-z chars.
	}
	return data
}

// generateRandomEntries is a test helper for generating entries with
// a random size, i.e. chunk length between 0.2 and 5.0.
func generateRandomEntries(
	t *testing.T,
	randSeed int64,
	chunkSize int,
	numEntries int,
	keyStart int,
) []testEntry {
	t.Helper()

	// Use the current time to get a new, random seed for each run.
	// If a test fails, hardcode this seed to reproduce the exact failure.
	r := rand.New(rand.NewSource(randSeed))
	t.Logf("Using random seed: %d\n", randSeed)

	entries := make([]testEntry, numEntries)
	for i := range entries {
		numChunks := 0.2 + r.Float64()*(5.0-0.2) // Generate a random float64 between 0.2 and 5.0.
		entries[i] = testEntry{
			Key:   []byte(strconv.Itoa(keyStart + i)),
			Value: generateBytes(t, chunkSize, numChunks),
		}
	}
	return entries
}

// assertEntries asserts that a buffer contains only the expected entries.
func assertEntries(t *testing.T, b *Buffer[*testutils.MockChunkPool], expected []testEntry, skipDeleted bool) {
	t.Helper()
	if b.isCompacting() {
		t.Fatal("illegal entries assertion: buffer is compacting")
		return
	}

	reader := NewReader(b)
	actual := []testEntry{}
	for {
		h, _, err := reader.ReadHeader()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read header: %v", err)
		}

		pl, flags := decodeHeader(h)
		if (flags & flagTombstone) != 0 {
			t.Fatal("invariant violation: tombstone entry found in source buffer")
		}
		isDeleted := (flags & flagDeleted) != 0
		if isDeleted && skipDeleted {
			_, err := reader.Seek(int64(pl), io.SeekCurrent)
			if err != nil {
				t.Fatalf("failed to seek to %d", reader.Offset()+uint64(pl))
			}
			continue
		}

		payload := make([]byte, pl)
		_, err = io.ReadFull(reader, payload)
		if err != nil {
			t.Fatalf("failed to read payload: %v", err)
		}

		key, value, err := decodeEntry(payload)
		if err != nil {
			t.Fatalf("failed to decode entry: %v", err)
		}
		actual = append(actual, testEntry{Key: key, Value: value, Deleted: isDeleted})
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("entries content mismatch:\n\nexpected: %+v\n\ngot: %+v", expected, actual)
	}
}

func TestBufferReset(t *testing.T) {
	b, p := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 0)
	for i := range 10 {
		b.Write([]byte(strconv.Itoa(i+1)), generateBytes(t, testutils.MockChunkSizes[0], 2.5))
	}

	if b.Offset() == 0 {
		t.Fatal("expected buffer to not be empty before reset")
	}
	if b.state != stateIdle {
		t.Fatalf("expected buffer state to be idle before reset, got %d", b.state)
	}
	if b.stats[0].LiveBytes == 0 {
		t.Fatal("expected buffer to have live bytes before reset")
	}
	if b.stats[0].DeadBytes != 0 {
		t.Fatal("expected buffer to not have dead bytes before reset")
	}
	if p.ChunksInUse() == 0 {
		t.Fatal("expeccted pool to have chunks in use before reset")
	}

	b.Reset()

	if b.Offset() != 0 {
		t.Errorf("expected offset to be 0 after reset, got %d", b.Offset())
	}
	for i := range b.stats {
		if b.stats[i].LiveBytes != 0 || b.stats[i].DeadBytes != 0 {
			t.Errorf("expected stats to be zeroed after reset, got %+v", b.stats[0])
		}
	}
	if len(b.buf[0]) != 0 || len(b.buf[1]) != 0 {
		t.Errorf("expected internal buffer slices to be empty after reset")
	}
	if b.writePos[0] != 0 || b.writePos[1] != 0 {
		t.Errorf("expected write positions to be 0 after reset, got %v", b.writePos)
	}
	if b.state != stateIdle {
		t.Errorf("expected buffer state to be idle after reset, got %d", b.state)
	}

	// Assert that all chunks were returned to the pool.
	if p.ChunksInUse() != 0 {
		t.Errorf("expected 0 chunks in use after reset (leak detected), got %d", p.ChunksInUse())
	}
}

func TestBufferEncodeEntry(t *testing.T) {
	testCases := []struct {
		name        string
		key         []byte
		value       []byte
		expectedErr error
	}{
		{"Valid entry", []byte("1"), []byte("xyz"), nil},
		{"Key too large", make([]byte, MaxKeySize+1), []byte("xyz"), ErrKeyTooLarge},
		{"Empty key ", []byte{}, []byte("xyz"), ErrKeyEmpty},
		{"Nil key ", []byte{}, []byte("xyz"), ErrKeyEmpty},
		{"Empty value", []byte("1"), []byte{}, nil},
		{"Nil value", []byte("1"), nil, nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := encodeEntry(tc.key, tc.value)
			if tc.expectedErr != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err)
				}
				return
			}
			k, v, err := decodeEntry(e)
			if err != nil {
				t.Fatalf("failed to decode entry: %v", err)
			}
			if !bytes.Equal(tc.key, k) {
				t.Errorf("expected key %q, got %q", tc.key, k)
			}
			if !bytes.Equal(tc.value, v) {
				t.Errorf("expected value %q, got %q", tc.value, v)
			}
		})
	}
}

func TestBufferEncodeHeader(t *testing.T) {
	testCases := []struct {
		name       string
		payloadLen int
		flags      byte
	}{
		{"No flags", 2, 0b0000},
		{"Flag bit 1 set", 2, 0b0001},
		{"Flag  bit 2 set", 2, 0b0010},
		{"All flags set", 2, 0b0011},
		{"Zero payload length", 0, 0b0001},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			flagMask := byte((1 << numFlagBits) - 1)
			expectedHeader := uint64(tc.payloadLen<<numFlagBits) | uint64(tc.flags&flagMask)

			actualHeader := encodeHeader(tc.payloadLen, tc.flags)
			if actualHeader != expectedHeader {
				t.Errorf("encodeHeader: expected %08b, got %08b",
					expectedHeader, actualHeader)
			}

			decodedLen, decodedFlags := decodeHeader(actualHeader)
			if decodedLen != tc.payloadLen {
				t.Errorf("decodeHeader: expected payload length %d, got %d", tc.payloadLen, decodedLen)
			}
			if decodedFlags != (tc.flags & flagMask) {
				t.Errorf("decodeHeader: expected flags %08b, got %08b", (tc.flags & flagMask), decodedFlags)
			}
		})
	}
}

func TestBufferWriteAndRead(t *testing.T) {
	t.Run("Writes empty entry", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := []byte("1")
		var value []byte
		offset, bytesWritten, err := b.Write(key, value)
		if err != nil {
			t.Fatal(err)
		}
		k, v, err := b.Read(offset)
		if err != nil {
			t.Fatal(err)
		}
		if bytesWritten <= 0 {
			t.Errorf("expected written bytes to be > 0, got %d", bytesWritten)
		}
		if !bytes.Equal(key, k) {
			t.Errorf("expected key %q, got %q", key, k)
		}
		if len(v) != 0 {
			t.Errorf("expected value to have length 0, got %d", len(v))
		}
	})

	t.Run("Returns error when key is too large", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := make([]byte, MaxKeySize+1)
		value := []byte("xyz")
		offset, bytesWritten, err := b.Write(key, value)
		if !errors.Is(err, ErrKeyTooLarge) {
			t.Errorf("expected error %q, got %q", ErrKeyTooLarge, err)
		}
		if bytesWritten != 0 {
			t.Fatalf("expected total bytes written to be 0, got %d", bytesWritten)
		}
		if offset != 0 {
			t.Errorf("expected offset 0, got %d", offset)
		}
	})

	t.Run("Returns error when value is too large", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := []byte("1")
		value := make([]byte, MaxValueSize+1)
		offset, bytesWritten, err := b.Write(key, value)
		if !errors.Is(err, ErrValueTooLarge) {
			t.Errorf("expected error %q, got %q", ErrValueTooLarge, err)
		}
		if bytesWritten != 0 {
			t.Fatalf("expected total bytes written to be 0, got %d", bytesWritten)
		}
		if offset != 0 {
			t.Errorf("expected offset 0, got %d", offset)
		}
	})

	t.Run("Writes entry to a single chunk", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := []byte("1")
		value := generateBytes(t, testutils.MockChunkSizes[0], 0.3)
		offset, bytesWritten, err := b.Write(key, value)
		if err != nil {
			t.Fatal(err)
		}
		if bytesWritten >= b.chunkSize[0] {
			// header(1) + keyLen(2) + key(N) + value(M)
			t.Fatalf("expected total bytes written to be < chunkSize(%d), got %d", b.chunkSize[0], bytesWritten)
		}
		if b.writePos[0] != int(offset)+bytesWritten {
			t.Fatalf("expected next write pos to be %d, got %d", int(offset)+bytesWritten, b.writePos[0])
		}

		outKey, outValue, err := b.Read(offset)
		if err != nil {
			t.Fatalf("failed to read entry: %v", err)
		}
		if !bytes.Equal(key, outKey) {
			t.Errorf("expected key %s, got %s", key, outKey)
		}
		if !bytes.Equal(value, outValue) {
			t.Errorf("expected value %q, got %q", value, outValue)
		}
	})

	t.Run("Writes entry across chunks", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := []byte("1")
		value := generateBytes(t, testutils.MockChunkSizes[0], 2.25)
		offset, bytesWritten, err := b.Write(key, value)
		if err != nil {
			t.Fatal(err)
		}
		if bytesWritten <= b.chunkSize[0] {
			t.Fatalf("expected total bytes written to be > chunkSize(%d), got %d", b.chunkSize[0], bytesWritten)
		}
		_, outValue, err := b.Read(offset)
		if err != nil {
			t.Fatalf("failed to read entry: %v", err)
		}
		if !bytes.Equal(value, outValue) {
			t.Errorf("expected value %q, got %q", value, outValue)
		}
	})

	t.Run("Writes entry that fills a single chunk", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		key := []byte("1")

		// value = chunkSize - (header(2) + keyLen(2) + key(1))
		value := make([]byte, testutils.MockChunkSizes[0]-(keyLenSize+len(key)+2))
		offset, bytesWritten, err := b.Write(key, value)
		if err != nil {
			t.Fatal(err)
		}
		if bytesWritten != b.chunkSize[0] {
			t.Errorf("expected written bytes to equal chunksize %d, got %d", b.chunkSize[0], bytesWritten)
		}
		if b.writePos[0] != b.chunkSize[0] {
			t.Errorf("expected next write pos to be %d, got %d", b.chunkSize[0], b.writePos[0])
		}

		_, outValue, err := b.Read(offset)
		if err != nil {
			t.Fatalf("failed to read entry from buffer: %v", err)
		}
		if !bytes.Equal(value, outValue) {
			t.Errorf("expected value %q, got %q", value, outValue)
		}

		// Assert that we can write when the write offset is pointing at a chunk that doesn't exist.
		value = []byte("abc")
		offset, _, err = b.Write([]byte("2"), value)
		if err != nil {
			t.Fatal(err)
		}
		_, outValue, err = b.Read(offset)
		if err != nil {
			t.Fatalf("failed to read entry from buffer: %v", err)
		}
		if !bytes.Equal(value, outValue) {
			t.Errorf("expected value %q, got %q", value, outValue)
		}
	})

	t.Run("Writes multiple entries", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		entries := generateRandomEntries(t, time.Now().UnixMicro(), testutils.MockChunkSizes[0], 100, 1)
		offsets := []uint64{}
		for _, e := range entries {
			offset, _, err := b.Write(e.Key, e.Value)
			if err != nil {
				t.Fatal(err)
			}
			offsets = append(offsets, offset)
		}

		for i, s := range offsets {
			_, v, err := b.Read(s)
			if err != nil {
				t.Fatalf("failed to read entry %d: %v", s, err)
			}
			e := entries[i]
			if !bytes.Equal(e.Value, v) {
				t.Errorf("expected value %q, got %q", e.Value, v)
			}
		}
	})

	t.Run("Panics when called during compaction", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 10)
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during write")
			}
		}()
		if started, _, _, err := b.StartCompactor(true); !started || err != nil {
			t.Fatalf("expected compactor to start: %v", err)
		}
		b.Write([]byte("1"), []byte("abc"))
	})

	t.Run("Returns error when buffer is corrupted", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		b.state = stateCorrupted
		if _, _, err := b.Write([]byte("1"), []byte("abc")); !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
	})

	t.Run("Updates stats", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		if b.stats[0].LiveBytes != 0 {
			t.Fatalf("expected live bytes to be 0, got %d", b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != 0 {
			t.Fatalf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
		}

		numEntries := 10
		var bytesWritten int
		var n int
		var err error
		for i := range numEntries {
			_, n, err = b.Write([]byte(strconv.Itoa(i+1)), make([]byte, 3)) // 12 bytes per entry (1+8+3).
			if err != nil {
				t.Fatal(err)
			}
			bytesWritten += n
		}
		if b.stats[0].LiveBytes != uint64(bytesWritten) {
			t.Errorf("expected live bytes to be %d, got %d", bytesWritten, b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != 0 {
			t.Errorf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
		}

		// Test Reset.
		b.Reset()
		if b.stats[0].LiveBytes != 0 {
			t.Fatalf("expected live bytes to be 0, got %d", b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != 0 {
			t.Fatalf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
		}
	})
}

func TestBufferRead(t *testing.T) {
	t.Run("Reads from empty buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		if _, _, err := b.Read(0); err != ErrOffsetOutOfBounds {
			t.Errorf("expected ErrOffsetOutOfBounds")
		}
	})

	t.Run("Returns error if buffer is corrupted", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		b.state = stateCorrupted
		if _, _, err := b.Read(0); !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
	})

	t.Run("Read offset is out of bounds", func(t *testing.T) {
		t.Run("Empty buffer", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
			if _, _, err := b.Read(999); err != ErrOffsetOutOfBounds {
				t.Errorf("expected ErrOffsetOutOfBounds")
			}
		})

		t.Run("After write header in last chunk", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
			_, n, err := b.Write([]byte("1"), generateBytes(t, testutils.MockChunkSizes[0], 1.2))
			if err != nil {
				t.Fatal(err)
			}
			if n < b.chunkSize[0] {
				t.Fatal("expected bytes written to be > chunkSize")
			}
			offset := calcOffset(b.chunkSize[0], len(b.buf[0])-1, b.writePos[0]+1) // +1 after write header.
			if _, _, err := b.Read(offset); err != ErrOffsetOutOfBounds {
				t.Errorf("expected ErrOffsetOutOfBounds")
			}
		})
	})

	t.Run("Reads incomplete payload", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)

		headerBuf := make([]byte, binary.MaxVarintLen64)
		// Create a valid header that claims a payload of 100 bytes.
		hl := binary.PutUvarint(headerBuf, encodeHeader(100, 0))
		b.write(headerBuf[:hl], 0)
		payload := make([]byte, 50) // Write only 50 bytes of payload.
		b.write(payload, 0)
		_, _, err := b.Read(0)

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected io.ErrUnexpectedEOF for incomplete payload, got %v", err)
		}
	})

	t.Run("Modifying read value slice doesn't mutate buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		value := []byte("abc")
		off, _, err := b.Write([]byte("1"), value)
		if err != nil {
			t.Fatal(err)
		}
		_, mv, err := b.Read(off)
		if err != nil {
			t.Fatal(err)
		}
		mv[0] = 65 // Mutate slice.

		_, v, err := b.Read(off)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value, v) {
			t.Errorf("expected value %q, got %q", value, v)
		}
	})

	t.Run("Reads entry during compaction", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		numEntries := 5
		offsets := make([]uint64, numEntries)
		var writtenBytes int
		var err error
		for i := range numEntries {
			offsets[i], writtenBytes, err = b.Write(
				[]byte(strconv.Itoa(i+1)),
				generateBytes(t, testutils.MockChunkSizes[0], 1),
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		deleteIndexes := []int{0, 1}
		for _, i := range deleteIndexes {
			if err := b.Delete(offsets[i]); err != nil {
				t.Fatalf("failed to delete entry: %v", err)
			}
		}

		b.StartCompactor(true)
		var newOffsets [][2]uint64
		for {
			_, newOffsets, err = b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
				break
			}
			if len(newOffsets) != 0 {
				break
			}
		}
		if b.cpLastEntryOffset != int64(len(deleteIndexes)*writtenBytes) {
			t.Fatalf(
				"expected last compacted entry offset to be %d, got %d",
				int64(len(deleteIndexes)*writtenBytes),
				b.cpLastEntryOffset,
			)
		}
		if b.buf[0][0] != nil {
			t.Error("expect the first chunk to be nil")
		}

		t.Run("Reads compacted entry from new buffer (buf[1])", func(t *testing.T) {
			// Test reading an offset that is <= lastCompactedEntryOffset.
			if _, _, err := b.Read(0); err != nil {
				t.Errorf("failed to read offset %d: %v", 0, err)
			}
		})

		t.Run("Reads uncompacted entry from buffer (buf[0])", func(t *testing.T) {
			// Find the first offset that is greater than the last compacted entry offset.
			var nextLiveOffset uint64
			if b.cpLastEntryOffset >= 0 {
				for _, offset := range offsets {
					if int64(offset) > b.cpLastEntryOffset {
						nextLiveOffset = offset
						break
					}
				}
			}

			// Assert reading an offset that is > lastCompactedEntryOffset.
			if _, _, err := b.Read(nextLiveOffset); err != nil {
				t.Errorf("failed to read offset %d: %v", 0, err)
			}
		})
	})
}

func TestBufferDelete(t *testing.T) {
	t.Run("Deletes from empty buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		if err := b.Delete(0); err != ErrOffsetOutOfBounds {
			t.Error("expected ErrOffsetOutOfBounds")
		}
	})

	t.Run("Returns error if buffer is corrupted", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		b.state = stateCorrupted
		if err := b.Delete(0); !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
	})

	t.Run("Delete offset is out of bounds", func(t *testing.T) {
		t.Run("Empty buffer", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
			if err := b.Delete(999); err != ErrOffsetOutOfBounds {
				t.Errorf("expected ErrOffsetOutOfBounds")
			}
		})

		t.Run("After write header in last chunk", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
			_, n, err := b.Write([]byte("1"), generateBytes(t, testutils.MockChunkSizes[0], 1.2))
			if err != nil {
				t.Fatal(err)
			}
			if n < b.chunkSize[0] {
				t.Fatal("expected bytes written to be > chunkSize")
			}
			offset := calcOffset(b.chunkSize[0], len(b.buf[0])-1, b.writePos[0]+1) // +1 after write header.
			if err := b.Delete(offset); err != ErrOffsetOutOfBounds {
				t.Errorf("expected ErrOffsetOutOfBounds")
			}
		})
	})

	t.Run("Deletes entry", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		off, _, err := b.Write([]byte("1"), []byte("abc"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := b.Read(off); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(off); err != nil {
			t.Fatal(err)
		}

		_, v, err := b.Read(off)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Error("expected value to be nil after delete")
		}
	})

	t.Run("Updates stats", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		if b.stats[0].LiveBytes != 0 {
			t.Fatalf("expected live bytes to be 0, got %d", b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != 0 {
			t.Fatalf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
		}

		// Write entries.
		var offsets []uint64
		var n int
		var off uint64
		var err error
		numEntries := 10
		for i := range numEntries {
			off, n, err = b.Write([]byte(strconv.Itoa(i+10)), make([]byte, 3))
			if err != nil {
				t.Fatal(err)
			}
			offsets = append(offsets, off)
		}

		// Delete entries.
		numDeletedEntries := 3
		for i := range numDeletedEntries {
			if err := b.Delete(offsets[i]); err != nil {
				t.Fatal(err)
			}
		}
		if b.stats[0].LiveBytes != uint64(n*numEntries-numDeletedEntries*n) {
			t.Errorf("expected live bytes to be %d, got %d", n*numEntries, b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != uint64(n*3) {
			t.Errorf("expected dead bytes to be %d, got %d", n*numDeletedEntries, b.stats[0].DeadBytes)
		}
	})

	t.Run("Deletes entry during compaction", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		data := []byte("a")
		key1, key2, key3 := []byte("1"), []byte("2"), []byte("3")

		// Write entries to buf[0].
		b.Write(key1, data)
		offset, _, err := b.Write(key2, data)
		if err != nil {
			t.Fatal(err)
		}

		// Write entry to buf[0] at 0.
		payload, err := encodeEntry(key3, data)
		if err != nil {
			t.Fatalf("failed to encode entry: %v", err)
		}
		hl := binary.PutUvarint(b.headerBuf[:], encodeHeader(len(payload), 0))
		header := b.headerBuf[:hl]
		b.write(header, 1)
		b.write(payload, 1)
		b.StartCompactor(true)

		t.Run("Deletes entry from src buffer (buf[0])", func(t *testing.T) {
			if err := b.Delete(offset); err != nil {
				t.Fatalf("failed to delete entry: %v", err)
			}
			chunkIdx, pos := calcPosition(b.chunkSize[0], offset)
			_, _, _, deleted, err := b.read(0, chunkIdx, pos)
			if err != nil {
				t.Fatalf("failed to read entry: %v", err)
			}
			if !deleted {
				t.Error("expected entry to be deleted")
			}
			reader := NewReader(b)
			h, _, err := reader.ReadHeader()
			if err != nil {
				t.Fatalf("failed to read header: %v", err)
			}
			_, flags := decodeHeader(h)
			if (flags & flagTombstone) != 0 {
				t.Error("expected tombstone flag be set")
			}
		})

		t.Run("Deletes entry from dst buffer (buf[1])", func(t *testing.T) {
			b.cpLastEntryOffset = 0
			if err := b.Delete(0); err != nil {
				t.Fatalf("failed to delete entry: %v", err)
			}
			chunkIdx, pos := calcPosition(b.chunkSize[1], 0)
			_, _, _, deleted, err := b.read(1, chunkIdx, pos)
			if err != nil {
				t.Fatalf("failed to read entry: %v", err)
			}
			if !deleted {
				t.Errorf("expected entry in buf[1] at offset %d to be deleted", 0)
			}

			_, _, _, deleted, err = b.read(0, chunkIdx, pos)
			if err != nil {
				t.Fatalf("failed to read entry: %v", err)
			}
			if deleted {
				t.Errorf("expected entry in buf[0] at offset %d to not be deleted", 0)
			}
		})
	})
}

func TestBufferGetStats(t *testing.T) {
	t.Run("Compactor is stopped", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		b.stats[0].LiveBytes = 123
		b.stats[0].LiveBytes = 456
		b.stats[1].LiveBytes = 123
		b.stats[1].LiveBytes = 456

		s := b.GetStats()
		if s.LiveBytes != b.stats[0].LiveBytes {
			t.Errorf("expected stats live bytes to be %d, got %d", b.stats[0].LiveBytes, s.LiveBytes)
		}
		if s.DeadBytes != b.stats[0].DeadBytes {
			t.Errorf("expected stats dead bytes to be %d, got %d", b.stats[0].DeadBytes, s.DeadBytes)
		}
	})

	t.Run("Compactor is running", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 10)
		if started, _, _, err := b.StartCompactor(true); !started || err != nil {
			t.Fatalf("expected compactor to start: %v", err)
		}
		b.stats[0].LiveBytes = 123
		b.stats[0].LiveBytes = 456
		b.stats[1].LiveBytes = 123
		b.stats[1].LiveBytes = 456

		s := b.GetStats()
		if s.LiveBytes != b.stats[0].LiveBytes+b.stats[1].LiveBytes {
			t.Errorf(
				"expected stats live bytes to be %d, got %d",
				b.stats[0].LiveBytes+b.stats[1].LiveBytes,
				s.LiveBytes,
			)
		}
		if s.DeadBytes != b.stats[0].DeadBytes+b.stats[1].DeadBytes {
			t.Errorf(
				"expected stats dead bytes to be %d, got %d",
				b.stats[0].DeadBytes+b.stats[1].DeadBytes,
				s.DeadBytes,
			)
		}
	})
}

func TestBufferCompact(t *testing.T) {
	t.Run("Compact should not corrupt buffer entries", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])

		expected := []testEntry{}
		for i := range 1000 {
			key := []byte(strconv.Itoa(i + 1))
			data := []byte{byte(i + 1)}
			b.Write(key, data)
			expected = append(expected, testEntry{Key: key, Value: data})
		}

		// Compact N times.
		for range 10 {
			_, writeOffset, _, err := b.StartCompactor(true) // Force compaction.
			if err != nil {
				t.Fatal(err)
			}
			for {
				done, _, err := b.Compact()
				if err != nil {
					t.Fatalf("failed to compact buffer: %v", err)
					break
				}
				if done {
					break
				}
			}

			if writeOffset != b.Offset() {
				t.Fatalf("expected write offset to be %d, got %d", b.Offset(), writeOffset)
			}
		}
		assertEntries(t, b, expected, false)
	})

	t.Run("Compacts empty buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		_, writeOffset, _, err := b.StartCompactor(true)
		if err != nil {
			t.Fatal(err)
		}

		done, newOffsets, err := b.Compact()
		if err != nil {
			t.Errorf("Compact() error = %v, want nil", err)
		}
		if !done {
			t.Error("expected compact to return done=true for an empty buffer")
		}
		if len(newOffsets) != 0 {
			t.Errorf("expected 0 entries relocated, got %d", len(newOffsets))
		}
		if writeOffset != b.Offset() {
			t.Fatalf("expected write offset to be %d, got %d", b.Offset(), writeOffset)
		}
		if b.state != stateIdle {
			t.Errorf("expected buffer state to be idle, got %d", b.state)
		}
	})

	t.Run("Returns error if buffer is corrupted", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 4)
		b.StartCompactor(true)
		b.state = stateCorrupted
		if _, _, err := b.Compact(); !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
	})

	t.Run("Resets compaction state when done", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		n := 1000
		for i := range n {
			key := []byte(strconv.Itoa(i + 1))
			val := []byte{byte(i + 1)}
			b.Write(key, val)
		}

		_, writeOffset, _, err := b.StartCompactor(true)
		if err != nil {
			t.Fatal(err)
		}
		for {
			done, _, err := b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
			}
			if done {
				break
			}
		}

		if b.stats[0].DeadBytes != 0 {
			t.Errorf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
		}
		if b.stats[1].DeadBytes != 0 || b.stats[1].LiveBytes != 0 {
			t.Error("expected stats[1] to be reset after compaction")
		}
		if b.cpReader.pos != 0 || b.cpReader.chunkIdx != 0 {
			t.Errorf("expected compact reader to be reset, got (%d, %d)", b.cpReader.chunkIdx, b.cpReader.pos)
		}
		if b.cpLastChunkIdx != -1 {
			t.Errorf("expected last chunk index to be reset, got %d", b.cpLastChunkIdx)
		}
		if b.cpLastEntryOffset != -1 {
			t.Errorf("expected last entry offset to be reset, got %d", b.cpLastEntryOffset)
		}
		if b.cpNextEntryOffset != 0 {
			t.Errorf("expected next entry offset to be reset, got %d", b.cpNextEntryOffset)
		}
		if writeOffset != b.Offset() {
			t.Errorf("expected calculated write offset to be %d, got %d", b.Offset(), writeOffset)
		}
		if b.state != stateIdle {
			t.Errorf("expected buffer state to be idle, got %d", b.state)
		}
	})

	t.Run("Panics when called without starting compaction", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 4)
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected a panic when calling Compact() in non-compacting mode")
			}
		}()
		b.Compact()
	})

	t.Run("Changes tombstone entry to deleted during copy", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		key := []byte("1")
		offset, writtenBytes, err := b.Write(key, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}

		_, writeOffset, _, err := b.StartCompactor(true)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(offset); err != nil {
			t.Fatal(err)
		}

		if b.stats[0].LiveBytes != 0 {
			t.Fatalf("expected live bytes to be %d, got %d", 0, b.stats[0].LiveBytes)
		}
		if b.stats[0].DeadBytes != uint64(writtenBytes) {
			t.Fatalf("expected dead bytes to be %d, got %d", writtenBytes, b.stats[0].DeadBytes)
		}

		relocations := [][2]uint64{}
		for {
			done, newOffsets, err := b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
				break
			}
			relocations = append(relocations, newOffsets...)
			if done {
				break
			}

		}
		if writeOffset != b.Offset() {
			t.Fatalf("expected write offset to be %d, got %d", b.Offset(), writeOffset)
		}
		if len(relocations) != 0 {
			t.Errorf("expected 0 entries offset to update, got %d", len(relocations))
		}
		s := b.GetStats()
		if s.LiveBytes != 0 {
			t.Fatalf("expected live bytes to be 0, got %d", s.LiveBytes)
		}
		if s.DeadBytes != uint64(writtenBytes) {
			t.Fatalf("expected dead bytes to be %d, got %d", writtenBytes, s.DeadBytes)
		}

		reader := NewReader(b)
		h, _, err := reader.ReadHeader()
		if err != nil {
			t.Fatalf("failed to read header: %v", err)
		}
		_, flags := decodeHeader(h)
		if (flags & flagDeleted) != 1 {
			t.Error("expected deleted entry")
		}
	})

	t.Run("Returns compacted entries new offsets", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		data := []byte("a")
		numEntries := 10
		entries := []testEntry{}
		offsets := []uint64{}
		var n int // bytes written.

		// Write 10 entries.
		var offset uint64
		var err error
		for i := range numEntries {
			key := []byte(strconv.Itoa(i + 10))
			offset, n, err = b.Write(key, data)
			if err != nil {
				t.Fatal(err)
			}
			offsets = append(offsets, offset)
			entries = append(entries, testEntry{Key: key, Value: data})
		}

		// Delete 5 entries.
		deletedIndexes := []int{0, 1, 4, 8, 9}
		for _, n := range deletedIndexes {
			if err := b.Delete(offsets[n]); err != nil {
				t.Fatal(err)
			}
		}

		// Filter out the deleted entries.
		expectedEntries := []testEntry{
			entries[2], // key: 3
			entries[3], // key: 4
			entries[5], // key: 6
			entries[6], // key: 7
			entries[7], // key: 8
		}

		s := b.GetStats()
		if s.LiveBytes != uint64(n*(numEntries-len(deletedIndexes))) {
			t.Fatalf("expected live bytes to be %d, got %d", n*(numEntries-len(deletedIndexes)), s.LiveBytes)
		}
		if s.DeadBytes != uint64(n*len(deletedIndexes)) {
			t.Fatalf("expected dead bytes to be %d, got %d", n*len(deletedIndexes), s.DeadBytes)
		}

		_, writeOffset, _, err := b.StartCompactor(true)
		if err != nil {
			t.Fatal(err)
		}
		relocations := [][2]uint64{}
		for {
			done, newOffsets, err := b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
				break
			}
			relocations = append(relocations, newOffsets...)
			if done {
				break
			}
		}
		if writeOffset != b.Offset() {
			t.Fatalf("expected write offset to be %d, got %d", b.Offset(), writeOffset)
		}

		expected := [][2]uint64{
			{xxhash.Sum64(entries[2].Key), 0},
			{xxhash.Sum64(entries[3].Key), uint64(n)},
			{xxhash.Sum64(entries[5].Key), uint64(n * 2)},
			{xxhash.Sum64(entries[6].Key), uint64(n * 3)},
			{xxhash.Sum64(entries[7].Key), uint64(n * 4)},
		}
		if !reflect.DeepEqual(relocations, expected) {
			t.Errorf("expected new entries to be '%v', got '%v'", expected, relocations)
		}

		if len(b.buf[1]) != 0 {
			t.Errorf("expected compact buffer to be empty")
		}

		s = b.GetStats()
		if s.LiveBytes != uint64(n*(numEntries-len(deletedIndexes))) {
			t.Errorf("expected live bytes to be %d, got %d", n*(numEntries-len(deletedIndexes)), s.LiveBytes)
		}
		if s.DeadBytes != 0 {
			t.Errorf("expected dead bytes to be 0, got %d", s.DeadBytes)
		}
		assertEntries(t, b, expectedEntries, false)
	})

	t.Run("Visits max dead entries in one step", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 4)
		data := []byte("a")
		numEntries := 11
		offsets := []uint64{}

		var offset uint64
		var n int
		var err error
		for i := range numEntries {
			offset, n, err = b.Write([]byte(strconv.Itoa(i+10)), data)
			if err != nil {
				t.Fatal(err)
			}
			offsets = append(offsets, offset)
		}

		// Delete all entries but the last.
		for _, off := range offsets[:len(offsets)-1] {
			if err := b.Delete(off); err != nil {
				t.Fatal(err)
			}
		}

		// Perform one compaction step and assert that it stops after visiting max dead entries.
		b.StartCompactor(true)
		done, _, err := b.Compact()
		if err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		if done {
			t.Error("expected compact to not be done")
		}
		expectedLastOffset := int64((n * (len(offsets) - 1)) - n)
		if b.cpLastEntryOffset != expectedLastOffset {
			t.Errorf("expected last compacted entry offset to be %d, got %d", expectedLastOffset, b.cpLastEntryOffset)
		}
	})

	t.Run("Handles concurrent delete of active entry", func(t *testing.T) {
		data := generateBytes(t, testutils.MockChunkSizes[0], 1)
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], len(data)/2) // CompactBytes is less than entry size.
		numEntries := 3

		entries := []struct {
			Key    []byte
			Offset uint64
		}{}
		var offset uint64
		var err error
		for i := range numEntries {
			k := []byte(strconv.Itoa(i + 1))
			offset, _, err = b.Write(k, data)
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, struct {
				Key    []byte
				Offset uint64
			}{Key: k, Offset: offset})
		}

		started, _, _, err := b.StartCompactor(true)
		if !started || err != nil {
			t.Fatalf("expected compactor to have started, %v", err)
		}
		if _, _, err := b.Compact(); err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		if b.cpEntry.BytesRemaining < 1 {
			t.Fatal("expected compact to start processing live entry")
		}

		// Delete the entry that is currently being processed from buf[0].
		if err := b.Delete(entries[0].Offset); err != nil {
			t.Fatalf("failed to delete entry: %v", err)
		}
		chunkIdx, pos := calcPosition(b.chunkSize[0], entries[0].Offset)
		_, _, _, deleted, err := b.read(0, chunkIdx, pos)
		if err != nil {
			t.Fatalf("failed to read entry: %v", err)
		}
		if !deleted {
			t.Error("expected entry to be deleted")
		}

		// Finish processing the entry.
		for i := 0; ; i++ {
			_, newOffsets, err := b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
			}
			if len(newOffsets) > 0 {
				if newOffsets[0][0] == xxhash.Sum64(entries[0].Key) {
					// Assert that deleted entries that are copied to the destination buffer
					// are not recorded as relocated.
					t.Fatal("expected copied deleted entry not to be recorded")
				}
				break // Break when the first live entry has been copied.
			}
			for i > 1000 {
				t.Fatal("compaction took too many steps")
			}
		}

		// Assert that entry is deleted.
		chunkIdx, pos = calcPosition(b.chunkSize[1], entries[0].Offset)
		_, _, _, deleted, err = b.read(1, chunkIdx, pos)
		if err != nil {
			t.Fatalf("failed to read entry: %v", err)
		}
		if !deleted {
			t.Error("expected entry to be deleted")
		}
	})

	t.Run("Releases chunks back to pool", func(t *testing.T) {
		t.Run("Releases chunk when next entry is in a new chunk", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])

			// Write entries that are equal to the chunk size.
			// 2(header) + 2(keyLen) + 1(key) + N(value) = chunkSize
			value := make([]byte, testutils.MockChunkSizes[0]-(2+keyLenSize+1))
			for i := range 2 {
				key := []byte(strconv.Itoa(i + 1))
				_, n, err := b.Write(key, value)
				if err != nil {
					t.Fatal(err)
				}
				if n != b.chunkSize[0] {
					t.Fatalf("expected written bytes to equal chunksize %d, got %d", b.chunkSize[0], n)
				}
			}
			b.StartCompactor(true)

			// Run compaction until the first entry has been fully processed.
			for {
				done, newOffsets, err := b.Compact()
				if err != nil {
					t.Fatalf("failed to compact: %v", err)
				}
				if len(newOffsets) > 0 {
					break
				}
				if done {
					t.Fatalf("expected compact to relocate at least one entry")
				}
			}

			// After the first entry is copied, the cnext entry starts in a new chunk,
			// making it safe to release chunk 0.
			if b.buf[0][0] != nil {
				t.Fatal("expected the first chunk to be released(nil)")
			}
		})

		t.Run("Does not release chunk when next entry is in the same chunk", func(t *testing.T) {
			b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 10)
			data := make([]byte, testutils.MockChunkSizes[0]/2)
			for i := range 2 {
				key := []byte(strconv.Itoa(i + 1))
				_, n, err := b.Write(key, data)
				if err != nil {
					t.Fatal(err)
				}
				if n >= b.chunkSize[0] {
					t.Fatalf("expected written bytes < chunksize %d, got %d", b.chunkSize[0], n)
				}
			}
			b.StartCompactor(true)

			// Run compaction until the first entry has been fully processed.
			for {
				done, newOffsets, err := b.Compact()
				if err != nil {
					t.Fatalf("failed to compact: %v", err)
				}
				if len(newOffsets) > 0 {
					break
				}
				if done {
					t.Fatalf("expected compact to relocate at least one entry")
				}
			}

			// After the first entry is copied, the next entry also starts in chunk 0,
			// so it cannot be released yet.
			if b.buf[0][0] == nil {
				t.Error("expected the first chunk to not be released")
			}
		})
	})
}

func TestBufferCompactEntries(t *testing.T) {
	compactBatchSizes := []int{4, 10, 16, 32, 64, 128, 1024}
	testCases := []struct {
		name            string
		numEntries      int
		deleteIndexes   []int   // Indices of entries to delete.
		entrySizeChunks float64 // Size of each entry in terms of chunks.
	}{
		{"All live entries", 100, []int{}, 0.5},
		{"All deleted entries", 100, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.25},
		{"Mixed live and dead entries", 100, []int{0, 1, 7, 9}, 0.25},
		{"Large entries spanning multiple chunks", 100, []int{0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89}, 2.5},
	}

	for _, tc := range testCases {
		for _, compactBytes := range compactBatchSizes {
			testName := fmt.Sprintf("%s/batchSize_%d", tc.name, compactBytes)

			t.Run(testName, func(t *testing.T) {
				b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], compactBytes)
				testData := generateBytes(t, testutils.MockChunkSizes[0], tc.entrySizeChunks)
				initialEntries := make([]testEntry, tc.numEntries)
				offsets := make([]uint64, tc.numEntries)
				entrySizes := make([]int, tc.numEntries)

				var err error
				for i := 0; i < tc.numEntries; i++ {
					key := []byte(strconv.Itoa(i + 1)) // Keys have variable length (e.g. "1", "10", "100").
					initialEntries[i] = testEntry{Key: key, Value: testData}

					var writtenBytes int
					offsets[i], writtenBytes, err = b.Write(key, testData)
					if err != nil {
						t.Fatal(err)
					}
					entrySizes[i] = writtenBytes
				}

				expectedEntries := []testEntry{}
				isDeleted := make(map[int]bool)
				for _, i := range tc.deleteIndexes {
					if err := b.Delete(offsets[i]); err != nil {
						t.Fatalf("failed to delete entry at index %d: %v", i, err)
					}
					isDeleted[i] = true
				}
				for i, entry := range initialEntries {
					if !isDeleted[i] {
						expectedEntries = append(expectedEntries, entry)
					}
				}

				// Calculate expected bytes by summing individual sizes.
				var expectedLiveBytes uint64
				var expectedDeadBytes uint64
				for i := 0; i < tc.numEntries; i++ {
					size := uint64(entrySizes[i])
					if isDeleted[i] {
						expectedDeadBytes += size
					} else {
						expectedLiveBytes += size
					}
				}

				// Assert stats before compaction.
				initialStats := b.GetStats()
				if initialStats.LiveBytes != expectedLiveBytes {
					t.Fatalf("expected live bytes %d, got %d", expectedLiveBytes, initialStats.LiveBytes)
				}
				if initialStats.DeadBytes != expectedDeadBytes {
					t.Fatalf("expected dead bytes %d, got %d", expectedDeadBytes, initialStats.DeadBytes)
				}

				_, writeOffset, _, err := b.StartCompactor(true)
				if err != nil {
					t.Fatal(err)
				}
				for {
					done, _, err := b.Compact()
					if err != nil {
						t.Fatalf("failed to compact: %v", err)
					}
					if done {
						break
					}
				}
				if writeOffset != b.Offset() {
					t.Fatalf("expected write offset to be %d, got %d", b.Offset(), writeOffset)
				}

				// Assert stats and buffer state after compaction.
				if b.stats[0].LiveBytes != expectedLiveBytes {
					t.Errorf("expected live bytes %d, got %d", expectedLiveBytes, b.stats[0].LiveBytes)
				}
				if b.stats[0].DeadBytes != 0 {
					t.Errorf("expected dead bytes to be 0, got %d", b.stats[0].DeadBytes)
				}
				if b.stats[1].LiveBytes != 0 || b.stats[1].DeadBytes != 0 {
					t.Error("expected compaction buffer stats to be reset after compaction")
				}
				if len(b.buf[1]) != 0 {
					t.Error("expected compaction buffer to be empty after compaction")
				}
				if b.state != stateIdle {
					t.Errorf("expected buffer state to be idle, got %d", b.state)
				}
				assertEntries(t, b, expectedEntries, false)
			})
		}
	}
}

func TestBufferTruncate(t *testing.T) {
	t.Run("Offset 0", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		for _, e := range generateRandomEntries(t, time.Now().UnixMicro(), testutils.MockChunkSizes[0], 10, 1) {
			b.Write(e.Key, e.Value)
		}
		b.truncate(0, 0)
		if len(b.buf[0]) != 0 {
			t.Errorf("expected 0 chunk, got %d", len(b.buf[0]))
		}
		if finalWritePos := b.writePos[0]; finalWritePos != 0 {
			t.Errorf("expected final writePos 0, got %d", finalWritePos)
		}
	})

	t.Run("Truncates empty buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.truncate(0, 10)

		if finalOffset := b.Offset(); finalOffset != 0 {
			t.Errorf("expected final offset 0, got %d", finalOffset)
		}
		if len(b.buf[0]) != 0 {
			t.Errorf("expected buffer length 0, got %d", len(b.buf[0]))
		}
		if finalWritePos := b.writePos[0]; finalWritePos != 0 {
			t.Errorf("expected final writePos 0, got %d", finalWritePos)
		}
	})

	t.Run("Offset > buffer end", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		_, n, err := b.Write([]byte("1"), generateBytes(t, testutils.MockChunkSizes[0], 1))
		if err != nil {
			t.Fatal(err)
		}
		bufferLen := len(b.buf[0])
		writePos := b.writePos[0]

		b.truncate(0, uint64(n+100))
		if bufferLen != len(b.buf[0]) {
			t.Errorf("expected buffer length %d, got %d", bufferLen, len(b.buf[0]))
		}
		if writePos != b.writePos[0] {
			t.Errorf("expected final writePos %d, got %d", writePos, b.writePos[0])
		}
	})

	t.Run("Offset at the end of chunk boundary", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		// 2(header) + 2(keyLen) + 1(key) + N(data) = chunkSize
		data := make([]byte, b.chunkSize[0]-(2+keyLenSize+1))
		_, n, err := b.Write([]byte("1"), data)
		if err != nil {
			t.Fatal(err)
		}
		if n != b.chunkSize[0] {
			t.Fatalf("expected written bytes to equal chunksize(%d), got %d", b.chunkSize[0], n)
		}
		b.Write([]byte("2"), data)
		if len(b.buf[0]) < 2 {
			t.Fatalf("expected buffer length to be > 1, got %d", len(b.buf[0]))
		}

		b.truncate(0, uint64(n))
		if len(b.buf[0]) != 0 {
			t.Errorf("expected buffer length 0, got %d", len(b.buf[0]))
		}
		if b.writePos[0] != testutils.MockChunkSizes[0] {
			t.Errorf("expected final writePos %d, got %d", testutils.MockChunkSizes[0], b.writePos[0])
		}
	})
}

func TestBufferMove(t *testing.T) {
	t.Run("Panics when src buffer is compacting", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		if started, _, _, _ := src.StartCompactor(true); !started {
			t.Fatal("expected compactor to start")
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during move")
			}
		}()
		dst.Move(src)
	})

	t.Run("Panics when dst buffer is compacting", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		if started, _, _, _ := dst.StartCompactor(true); !started {
			t.Fatal("expected compactor to start")
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during move")
			}
		}()
		dst.Move(src)
	})

	t.Run("Panics when src buffer is corrupted", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src.state = stateCorrupted
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during move")
			}
		}()
		dst.Move(src)
	})

	t.Run("Panics when dst buffer is corrupted", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		dst.state = stateCorrupted
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during move")
			}
		}()
		dst.Move(src)
	})

	t.Run("Panics on chunk size mismatch between src and dst buffer", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[1], 0)
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic during move")
			}
		}()
		dst.Move(src)
	})

	t.Run("Moves data to an empty dst buffer", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, srcPool := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)

		src.Write([]byte("1"), generateBytes(t, testutils.MockChunkSizes[0], 2.5))
		expectedNumChunks := len(src.buf[0])
		expectedWritePos := src.writePos[0]
		expectedOffset := src.Offset()
		expectedStats := src.GetStats()

		startOff, newOff := dst.Move(src)
		if srcPool.PutCalls() > 0 {
			t.Error("expected no chunks to be released to pool")
		}
		if src.Offset() != 0 {
			t.Errorf("expected src offset to be 0 after move, got %d", src.Offset())
		}
		if startOff != 0 {
			t.Errorf("expected start offset to be 0 for an empty buffer, got %d", startOff)
		}
		if newOff != expectedOffset {
			t.Errorf("expected new offset to be %d, got %d", expectedOffset, newOff)
		}
		if dst.Offset() != expectedOffset {
			t.Errorf("expected dst offset to be %d, got %d", expectedOffset, dst.Offset())
		}
		if len(dst.buf[0]) != expectedNumChunks {
			t.Errorf("expected dst to have %d chunks, got %d", expectedNumChunks, len(dst.buf[0]))
		}
		if dst.writePos[0] != expectedWritePos {
			t.Errorf("expected dst writePos to be %d, got %d", expectedWritePos, dst.writePos[0])
		}

		s := dst.GetStats()
		if !expectedStats.equal(s) {
			t.Errorf("expected dst stats %v to equal src stats, got %v", expectedStats, s)
		}
	})

	t.Run("Moves data from an empty src buffer", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)

		dst.Write([]byte("1"), []byte("abc"))
		expectedOffset := dst.Offset()
		expectedNumChunks := len(dst.buf[0])
		expectedWritePos := dst.writePos[0]
		expectedStats := dst.GetStats()

		startOff, newOff := dst.Move(src)
		if startOff != expectedOffset || newOff != expectedOffset {
			t.Errorf("expected offsets to be unchanged at %d, got prev=%d, new=%d", expectedOffset, startOff, newOff)
		}
		if len(dst.buf[0]) != expectedNumChunks {
			t.Errorf("expected chunk count to be unchanged at %d, got %d", expectedNumChunks, len(dst.buf[0]))
		}
		if dst.writePos[0] != expectedWritePos {
			t.Errorf("expected writePos to be unchanged at %d, got %d", expectedWritePos, dst.writePos[0])
		}

		s := dst.GetStats()
		if !expectedStats.equal(s) {
			t.Errorf("expected dst stats %v, got %v", expectedStats, s)
		}
	})

	t.Run("Updates buffer stats", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		srcEntries := generateRandomEntries(t, time.Now().UnixMicro(), testutils.MockChunkSizes[0], 100, 1)
		dstEntries := generateRandomEntries(t, time.Now().UnixMicro(), testutils.MockChunkSizes[0], 100, 1)
		for _, e := range srcEntries {
			src.Write(e.Key, e.Value)
		}
		for _, e := range dstEntries {
			dst.Write(e.Key, e.Value)
		}
		srcInitialStats := src.GetStats()
		dstInitialStats := dst.GetStats()
		dstInitialOffset := dst.Offset()
		srcInitialOffset := src.Offset()
		paddedBytes := uint64(dst.chunkSize[0] - dst.writePos[0])

		startOff, newOff := dst.Move(src)
		if startOff != dstInitialOffset+paddedBytes {
			t.Errorf("expected start offset to be %d, got %d", dstInitialOffset+paddedBytes, startOff)
		}
		if newOff != startOff+srcInitialOffset {
			t.Errorf("expected new write offset to be %d, got %d", startOff+srcInitialOffset, newOff)
		}

		expectedStats := stats{
			LiveBytes:   srcInitialStats.LiveBytes + dstInitialStats.LiveBytes,
			DeadBytes:   srcInitialStats.DeadBytes + dstInitialStats.DeadBytes + paddedBytes,
			PaddedBytes: paddedBytes,
		}
		dstStats := dst.GetStats()
		if !dstStats.equal(expectedStats) {
			t.Errorf("expected stats %v, got %v", expectedStats, dstStats)
		}
	})

	t.Run("Moves data to unaligned buffer with padding", func(t *testing.T) {
		// Tests padding remaining space in destination buffer's last chunk during move.
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, srcPool := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)

		entries := []testEntry{
			// 2(h) + 2(kl) + 1(k) + 122(v) = 127
			{Key: []byte("1"), Value: make([]byte, testutils.MockChunkSizes[0]-(2+keyLenSize+1)-1)},
			{Key: []byte("2"), Value: generateBytes(t, testutils.MockChunkSizes[0], 3.5)},
			{Key: []byte("3"), Value: generateBytes(t, testutils.MockChunkSizes[0], 1.5)},
		}

		// dst last chunk is partially full (127/128 bytes).
		_, n, err := dst.Write(entries[0].Key, entries[0].Value)
		if err != nil {
			t.Fatal(err)
		}
		if n > dst.chunkSize[0] {
			t.Fatal("expected written bytes to be < chunkSize")
		}

		initialOffset := dst.Offset()
		initialChunks := len(dst.buf[0])
		initialDeadBytes := dst.stats[0].DeadBytes
		initalWritePos := dst.writePos[0]
		paddingBytes := uint64(dst.chunkSize[0] - dst.writePos[0])
		if int(paddingBytes) != 1 {
			t.Fatalf("expected calculated padded bytes to be 1, got %d", paddingBytes)
		}
		if initialOffset+paddingBytes != uint64(dst.chunkSize[0]) {
			t.Errorf(
				"expected inital offset + padding to be %d (chunkSize), got %d",
				uint64(dst.chunkSize[0]),
				initialOffset+paddingBytes,
			)
		}

		src.Write(entries[1].Key, entries[1].Value)
		offset, _, err := src.Write(entries[2].Key, entries[2].Value)
		if err != nil {
			t.Fatal(err)
		}
		srcInitialOffset := src.Offset()
		srcInitialChunks := len(src.buf[0])

		startOff, newOff := dst.Move(src)
		if srcPool.PutCalls() > 0 {
			t.Error("expected no chunks to be released to pool")
		}
		if startOff != initialOffset+paddingBytes {
			t.Errorf("expected start offset to be %d, got %d", initialOffset+paddingBytes, startOff)
		}
		if newOff != startOff+srcInitialOffset {
			t.Errorf("expected new offset to be %d, got %d", startOff+srcInitialOffset, newOff)
		}
		if dst.stats[0].DeadBytes != initialDeadBytes+paddingBytes {
			t.Errorf(
				"expected dead bytes to increase by %d, got %d",
				paddingBytes,
				dst.stats[0].DeadBytes-initialDeadBytes,
			)
		}
		if len(dst.buf[0]) != initialChunks+srcInitialChunks {
			t.Errorf("expected final chunk count of %d, got %d", initialChunks+srcInitialChunks, len(dst.buf[0]))
		}

		// Read the last entry in the final buffer.
		_, value, err := dst.Read(startOff + offset)
		if err != nil {
			t.Errorf("failed to read entry at offset %d", startOff+offset)
		}
		if !bytes.Equal(value, entries[2].Value) {
			t.Errorf("expected value %s, got %s", entries[2].Value, value)
		}

		// Read the padded entry header and assert it's a deleted entry.
		r := NewReader(dst)
		_, err = r.Seek(int64(initalWritePos), io.SeekStart)
		if err != nil {
			t.Fatalf("failed to seek reader: %v", err)
		}
		h, hl, err := r.ReadHeader()
		if err != nil {
			t.Fatalf("failed to reader padded entry header: %v", err)
		}
		pl, flags := decodeHeader(h)
		if pl != int(paddingBytes)-hl {
			t.Errorf("expected payload length to be %d, got %d", int(paddingBytes)-hl, pl)
		}
		if (flags & flagDeleted) == 0 {
			t.Errorf("expected padded entry header to have deleted flag, got %08b", flags)
		}

		assertEntries(t, dst, entries, true)
	})

	t.Run("Moves data to aligned buffer with no padding", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)

		entries := []testEntry{
			// 2(h) + 2(kl) + 1(k) + N(v) = chunkSize
			{Key: []byte("1"), Value: make([]byte, dst.chunkSize[0]-(2+keyLenSize+1))},
			{Key: []byte("2"), Value: make([]byte, dst.chunkSize[0]-(2+keyLenSize+1))},
			{Key: []byte("3"), Value: generateBytes(t, testutils.MockChunkSizes[0], 3.5)},
			{Key: []byte("4"), Value: generateBytes(t, testutils.MockChunkSizes[0], 2.5)},
		}

		// dst last chunk is exactly full.
		dst.Write(entries[0].Key, entries[0].Value)
		_, n, err := dst.Write(entries[1].Key, entries[1].Value)
		if err != nil {
			t.Fatal(err)
		}
		if n != dst.chunkSize[0] {
			t.Fatal("expected written bytes to equal chunkSize")
		}
		initialOff := dst.Offset()
		initialDeadBytes := dst.stats[0].DeadBytes

		_, n1, err := src.Write(entries[2].Key, entries[2].Value)
		if err != nil {
			t.Fatal(err)
		}
		offset, n2, err := src.Write(entries[3].Key, entries[3].Value)
		if err != nil {
			t.Fatal(err)
		}
		n = n1 + n2

		startOff, newOff := dst.Move(src)
		if startOff != initialOff {
			t.Errorf("expected start offset to be %d, got %d", initialOff, startOff)
		}
		if newOff != startOff+uint64(n) {
			t.Errorf("expected new offset to be %d, got %d", startOff+uint64(n), newOff)
		}
		if dst.stats[0].DeadBytes != initialDeadBytes {
			t.Error("expected dead bytes to be unchanged for an aligned append")
		}

		_, value, err := dst.Read(startOff + offset)
		if err != nil {
			t.Errorf("failed to read entry at offset %d", startOff+offset)
		}
		if !bytes.Equal(value, entries[3].Value) {
			t.Errorf("expected value %s, got %s", entries[3].Value, value)
		}

		assertEntries(t, dst, entries, true)
	})

	t.Run("Randomized multiple moves", func(t *testing.T) {
		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		const numMoves = 3 //10
		var allEntries []testEntry
		var nextKey int = 1

		for range numMoves {
			newEntries := generateRandomEntries(t, time.Now().UnixNano(), testutils.MockChunkSizes[0], 100, nextKey)
			for _, entry := range newEntries {
				src.Write(entry.Key, entry.Value)
			}

			dst.Move(src)
			src.Reset()
			allEntries = append(allEntries, newEntries...)
			nextKey += len(newEntries)
			assertEntries(t, dst, allEntries, true)
		}
	})

	t.Run("Pads unaligned buffer's Uvarint header", func(t *testing.T) {
		// Tests that the header length is padded for the case where the natural Uvarint
		// encoded header and the payloadLen would otherwise not fit in the remaining space.

		dst, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		src, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		entries := []testEntry{
			// 2(h) + 2(kl) + 1(k) + (128 - 5 - 33)(v) = 95
			{Key: []byte("1"), Value: make([]byte, testutils.MockChunkSizes[0]-(2+keyLenSize+1)-33)},
			{Key: []byte("2"), Value: generateBytes(t, testutils.MockChunkSizes[0], 1)},
		}

		// dst last chunk is partially full (95/128 bytes).
		_, n, err := dst.Write(entries[0].Key, entries[0].Value)
		if err != nil {
			t.Fatal(err)
		}
		if n > dst.chunkSize[0] {
			t.Fatal("expected written bytes to be < chunkSize")
		}

		initialOffset := dst.Offset()
		initalWritePos := dst.writePos[0]
		paddingBytes := uint64(dst.chunkSize[0] - dst.writePos[0])

		// Special case 33.
		if paddingBytes != 33 {
			t.Fatalf("expected remaining space in last chunk to be 33, got %d", paddingBytes)
		}

		src.Write(entries[1].Key, entries[1].Value)
		startOff, _ := dst.Move(src)

		if startOff != initialOffset+paddingBytes {
			t.Errorf("expected start offset to be %d, got %d", initialOffset+paddingBytes, startOff)
		}

		// Assert that the padded entry's Uvarint header has the expected length.
		r := NewReader(dst)
		_, err = r.Seek(int64(initalWritePos), io.SeekStart)
		if err != nil {
			t.Fatalf("failed to seek reader: %v", err)
		}
		h, hl, err := r.ReadHeader()
		if err != nil {
			t.Fatalf("failed to reader padded entry header: %v", err)
		}
		pl, flags := decodeHeader(h)
		if pl != int(paddingBytes)-hl {
			t.Errorf("expected payload length to be %d, got %d", int(paddingBytes)-hl, pl)
		}
		if (flags & flagDeleted) == 0 {
			t.Errorf("expected padded entry header to have deleted flag, got %08b", flags)
		}

		assertEntries(t, dst, entries, true)
	})
}

func TestBufferGetAlignedOffset(t *testing.T) {
	t.Run("Offset zero returns zero", func(t *testing.T) {
		if off := GetAlignedOffset(testutils.MockChunkSizes[0], 0); off != 0 {
			t.Errorf("expected aligned offset to be 0, got %d", off)
		}
	})

	t.Run("Offset chunkSize returns chunk boundary offset", func(t *testing.T) {
		if off := GetAlignedOffset(
			testutils.MockChunkSizes[0],
			uint64(testutils.MockChunkSizes[0]),
		); off != uint64(testutils.MockChunkSizes[0]) {
			t.Errorf("expected aligned offset to be %d, got %d", testutils.MockChunkSizes[0], off)
		}
	})

	t.Run("Offset chunkSize+1 returns next chunk boundary offset", func(t *testing.T) {
		if off := GetAlignedOffset(
			testutils.MockChunkSizes[0],
			uint64(testutils.MockChunkSizes[0]+1),
		); off != uint64(testutils.MockChunkSizes[0]*2) {
			t.Errorf("expected aligned offset to be %d, got %d", testutils.MockChunkSizes[0]*2, off)
		}
	})
}

func TestBufferStartCompactor(t *testing.T) {
	t.Run("No-op on empty buffer", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not start on an empty buffer")
		}
	})

	t.Run("No-op when already compacting", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.state = stateCompacting
		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not start")
		}
	})

	t.Run("No-op when disabled (compactBytes <= 0)", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 0)
		b.state = stateCompacting
		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not start")
		}
	})

	t.Run("No-op when below dead chunk ratio size threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.compactDeadRatio = 0.3
		b.compactDeadChunkRatio = 0.6
		minCompactDeadBytes := uint64(float64(b.chunkSize[0]) * b.compactDeadChunkRatio)

		// Write and update entry half of chunkSize so that we have 50/50 live/dead.
		b.Write([]byte("1"), make([]byte, b.chunkSize[0]/2))
		off, _, _ := b.Write([]byte("2"), make([]byte, b.chunkSize[0]/2))
		b.Delete(off)

		if b.stats[0].DeadBytes > minCompactDeadBytes {
			t.Fatal("expected dead bytes to be below dead chunk ratio threshold")
		}
		if float64(b.stats[0].DeadBytes) <= b.compactDeadRatio*float64(b.stats[0].LiveBytes+b.stats[0].DeadBytes) {
			t.Fatal("expected dead bytes to be above dead ratio threshold")
		}

		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not have started")
		}
	})

	t.Run("No-op when dead ratio is below threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.compactDeadRatio = 0.3
		b.compactDeadChunkRatio = 0.1

		// Write 2/3 alive.
		off, _, _ := b.Write([]byte("1"), make([]byte, b.chunkSize[0]/4))
		b.Delete(off)
		b.Write([]byte("2"), make([]byte, b.chunkSize[0]/4))
		b.Write([]byte("3"), make([]byte, b.chunkSize[0]/4))
		b.Write([]byte("4"), make([]byte, b.chunkSize[0]/4))

		if float64(b.stats[0].DeadBytes) <= 0 ||
			float64(b.stats[0].DeadBytes) >= b.compactDeadRatio*float64(b.stats[0].LiveBytes+b.stats[0].DeadBytes) {
			t.Fatal("expected dead bytes to be below dead ratio threshold")
		}
		if b.stats[0].DeadBytes < uint64(float64(b.chunkSize[0])*b.compactDeadChunkRatio) {
			t.Fatal("expected dead bytes to be above dead chunk ratio threshold")
		}

		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not haved started")
		}
	})

	t.Run("No-op when all dead bytes are padded bytes", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.compactDeadRatio = 0.3
		minCompactSize := b.resizeChunkThresholds[0]

		// Write enough data to be above min compaction size.
		for b.Offset() < minCompactSize+10 {
			offset, _, _ := b.Write([]byte("1"), make([]byte, 128))
			b.Delete(offset)
		}
		b.Write([]byte("1"), make([]byte, 128)) // Keep one live entry so LiveBytes > 0.
		if float64(b.stats[0].DeadBytes) <= 0 &&
			float64(b.stats[0].DeadBytes) < b.compactDeadRatio*float64(b.stats[0].LiveBytes) {
			t.Fatal("expected dead bytes to be above dead ratio threshold")
		}

		b.stats[0].PaddedBytes = b.stats[0].DeadBytes // Mark dead bytes as padding.
		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if started {
			t.Error("expected compaction to not have started dead ratio is not met")
		}
	})

	t.Run("Returns error when buffer is corrupted", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.state = stateCorrupted
		started, _, _, err := b.StartCompactor(false)
		if !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
		if started {
			t.Error("expected compaction to not start")
		}
	})

	t.Run("Returns error when stats and offset mismatch", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.stats[0].LiveBytes = 99 // Force a mismatch
		started, _, _, err := b.StartCompactor(false)
		if !errors.Is(err, ErrBufferCorrupted) {
			t.Errorf("expected ErrBufferCorrupted, got %v", err)
		}
		if started {
			t.Error("expected compaction to not start")
		}
		if b.state != stateCorrupted {
			t.Error("expected buffer to be marked as corrupted")
		}
	})
	t.Run("Force=true starts compaction", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 0)
		b.Write([]byte("1"), []byte("a"))
		liveBytes := b.stats[0].LiveBytes

		started, offset, chunkSize, err := b.StartCompactor(true)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !started {
			t.Fatal("expected compaction to be forced to start")
		}
		if offset != liveBytes {
			t.Errorf("expected offset %d, got %d", liveBytes, offset)
		}
		if chunkSize != testutils.MockChunkSizes[0] {
			t.Errorf("expected target chunk size %d, got %d", testutils.MockChunkSizes[0], chunkSize)
		}
		if b.state != stateCompacting {
			t.Error("expected buffer state to be stateCompacting")
		}
	})

	t.Run("Start when dead ratio is above threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])
		b.compactDeadRatio = 0.3
		minCompactSize := b.resizeChunkThresholds[0]

		// Write enough data to be above min compaction size.
		for b.Offset() < minCompactSize+10 {
			offset, _, _ := b.Write([]byte("1"), make([]byte, 128))
			b.Delete(offset)
		}
		b.Write([]byte("1"), make([]byte, 128)) // Keep one live entry so LiveBytes > 0.

		if float64(b.stats[0].DeadBytes) <= 0 &&
			float64(b.stats[0].DeadBytes) < b.compactDeadRatio*float64(b.stats[0].LiveBytes) {
			t.Fatal("expected dead bytes to be above dead ratio threshold")
		}

		started, _, _, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !started {
			t.Error("expected compaction to start when dead ratio is met")
		}
	})

	t.Run("Upgrades chunk size", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], testutils.MockChunkSizes[0])

		// Write enough data to cross the first resize threshold.
		for b.stats[0].LiveBytes < b.resizeChunkThresholds[0]+10 {
			b.Write([]byte("1"), make([]byte, 20))
		}

		started, _, chunkSize, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !started {
			t.Fatal("expected compaction to start for chunk size upgrade")
		}
		if chunkSize != testutils.MockChunkSizes[1] {
			t.Errorf("expected target chunk size to be %d, got %d", testutils.MockChunkSizes[1], chunkSize)
		}
	})

	t.Run("Downgrades chunk size", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[2], testutils.MockChunkSizes[0])
		b.Write([]byte("1"), make([]byte, (b.resizeChunkThresholds[0]/2)-10))

		started, _, chunkSize, err := b.StartCompactor(false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !started {
			t.Fatal("expected compaction to start for chunk size downgrade")
		}
		if chunkSize >= testutils.MockChunkSizes[2] {
			t.Errorf("expected target chunk size to be downgraded, but got %d", chunkSize)
		}
	})
}

// runResizeTest is a helper to test the buffer resizing logic.
func runResizeTest(
	t *testing.T,
	b *Buffer[*testutils.MockChunkPool],
	liveBytesTarget uint64,
	expectedChunkSize int,
) {
	t.Helper()
	if liveBytesTarget > 1*MiB {
		// Sanity check to prevent large writes during testing.
		panic(fmt.Errorf("liveBytesTarget is too large: %d", liveBytesTarget))
	}

	// Write data until we exceed the live bytes target.
	var entries []testEntry
	key := 1
	value := make([]byte, 10)
	for b.stats[0].LiveBytes < liveBytesTarget {
		entry := testEntry{Key: []byte(strconv.Itoa(key)), Value: value}
		b.Write(entry.Key, entry.Value)
		entries = append(entries, entry)
		key++
	}

	started, _, targetChunkSize, err := b.StartCompactor(false)
	if err != nil {
		t.Fatalf("failed to start compactor: %v", err)
	}
	if started {
		if targetChunkSize != expectedChunkSize {
			t.Fatalf("expected target chunk size %d, got %d", expectedChunkSize, targetChunkSize)
		}
		for {
			done, _, err := b.Compact()
			if err != nil {
				t.Fatalf("failed to compact: %v", err)
			}
			if done {
				break
			}
		}
	}
	if b.chunkSize[0] != expectedChunkSize {
		t.Errorf("expected chunk size %d after compacting, got %d", expectedChunkSize, b.chunkSize[0])
	}
	assertEntries(t, b, entries, true) // Assert that the data is still intact.
}

func TestBufferResizeChunkSize(t *testing.T) {
	t.Run("Does not resize when smallest chunk size and live bytes are below first threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 1024)
		runResizeTest(t, b, b.resizeChunkThresholds[0]/2, testutils.MockChunkSizes[0])
	})

	t.Run("Upgrades chunk size from C to C+1 when live bytes exceed first threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 1024)
		runResizeTest(t, b, b.resizeChunkThresholds[0]+10, testutils.MockChunkSizes[1])
	})

	t.Run("Upgrades chunk size from smallest to largest when live bytes exceed last threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[0], 1024)
		runResizeTest(t, b, b.resizeChunkThresholds[1]+10, testutils.MockChunkSizes[2])
	})

	t.Run("Downgrades chunk size from largest to smallest when live bytes is below threshold", func(t *testing.T) {
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[2], 1024)
		runResizeTest(
			t,
			b,
			uint64(float64(testutils.MockChunkSizes[0])*b.chunkDowngradeRatio),
			testutils.MockChunkSizes[0],
		)
	})

	t.Run("Does not downgrade chunk size if live bytes are above lower threshold", func(t *testing.T) {
		// Start with a large chunk size and ensure it doesn't shrink.
		b, _ := SetupTestBufferWithCleanup(t, testutils.MockChunkSizes[1], 1024)
		runResizeTest(t, b, b.resizeChunkThresholds[0]+10, testutils.MockChunkSizes[1])
	})
}

func TestBufferUvarintLen(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint64
		expected int
	}{
		{
			name:     "Zero value",
			input:    0, // 0
			expected: 1,
		},
		{
			name:     "Max 1-byte value",
			input:    (1 << 7) - 1, // 127
			expected: 1,
		},
		{
			name:     "Min 2-byte value",
			input:    1 << 7, // 128
			expected: 2,
		},
		{
			name:     "Max 2-byte value",
			input:    (1 << 14) - 1, // 16383
			expected: 2,
		},
		{
			name:     "Min 3-byte value",
			input:    1 << 14, // 16384
			expected: 3,
		},
		{
			name:     "Max 3-byte value",
			input:    (1 << 21) - 1, // 2097151
			expected: 3,
		},
		{
			name:     "Min 4-byte value",
			input:    1 << 21, // 2097152
			expected: 4,
		},
		{
			name:     "Max 4-byte value",
			input:    (1 << 28) - 1, // 268435455
			expected: 4,
		},
		{
			name:     "Min 5-byte value",
			input:    1 << 28, // 268435456
			expected: 5,
		},
		{
			name:     "Max 5-byte value",
			input:    (1 << 35) - 1, // 34359738367
			expected: 5,
		},
		{
			name:     "Min 6-byte value",
			input:    1 << 35, // 34359738368
			expected: 6,
		},
		{
			name:     "Max 6-byte value",
			input:    (1 << 42) - 1, // 4398046511103
			expected: 6,
		},
		{
			name:     "Min 7-byte value",
			input:    1 << 42, // 4398046511104
			expected: 7,
		},
		{
			name:     "Max 7-byte value",
			input:    (1 << 49) - 1, // 562949953421311
			expected: 7,
		},
		{
			name:     "Min 8-byte value",
			input:    1 << 49, // 562949953421312
			expected: 8,
		},
		{
			name:     "Max 8-byte value",
			input:    (1 << 56) - 1, // 72057594037927935
			expected: 8,
		},
		{
			name:     "Min 9-byte value",
			input:    1 << 56, // 72057594037927936
			expected: 9,
		},
		{
			name:     "Max 9-byte value",
			input:    (1 << 63) - 1, // 9223372036854775807
			expected: 9,
		},
		{
			name:     "Min 10-byte value",
			input:    1 << 63, // 9223372036854775808
			expected: binary.MaxVarintLen64,
		},
		{
			name:     "Max uint64 value",
			input:    math.MaxUint64, // 18446744073709551615
			expected: binary.MaxVarintLen64,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := uvarintLen(tc.input)
			if got != tc.expected {
				t.Errorf("uvarintLen(%d): expected %d, got %d", tc.input, tc.expected, got)
			}

			// Assert against the standard library's implementation.
			var headerBuf [binary.MaxVarintLen64]byte
			expected := binary.PutUvarint(headerBuf[:], tc.input)
			if got != expected {
				t.Errorf("result mismatch with binary.PutUvarint: expected %d, got %d", expected, got)
			}
		})
	}
}
