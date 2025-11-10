package buffer

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/holmberd/go-cmap/internal/testutils"
)

func TestReaderReset(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)
	b.Write([]byte("1"), []byte("abc"))
	if _, err := reader.Seek(10, io.SeekCurrent); err != nil {
		t.Fatal(err)
	}
	prevOffset := reader.Offset()
	reader.Reset()
	if reader.Offset() != 0 || reader.Offset() == prevOffset {
		t.Errorf("expected reader offset to be reset")
	}
}

func TestReaderInit(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)
	b.Write([]byte("1"), []byte("abc"))
	reader.init(1, 0, 10)
	if reader.Offset() != 10 {
		t.Errorf("expected reader offset 10, got %d", reader.Offset())
	}
	if reader.bufIdx != 1 {
		t.Errorf("expected reader bufIdx 1, got %d", reader.bufIdx)
	}
}

func TestReaderReadByte(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)

	t.Run("Empty buffer", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		if _, err := reader.ReadByte(); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})

	t.Run("First entry byte", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		b.Write([]byte("1"), []byte("abc"))
		d, err := reader.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		if d != b.buf[0][0][0] {
			t.Errorf("expected read byte %d, got %d", b.buf[0][0][0], d)
		}
	})

	t.Run("After last entry byte (EOF)", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})

		b.Write([]byte("1"), []byte("abc")) // 1+8+3 = 12
		off, err := reader.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(b.Offset()) {
			t.Fatalf("expected reader to be at offset %d, got %d", b.Offset(), off)
		}
		if _, err := reader.ReadByte(); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})

	t.Run("After last byte in last chunk", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		b.Write([]byte("1"), make([]byte, b.chunkSize[0]-(2+keyLenSize+1))) // 2+2+1+123 = 128
		if b.writePos[0] != b.chunkSize[0] {
			t.Fatalf("expected next write pos to be %d, got %d", b.chunkSize[0], b.writePos[0])
		}
		if _, err := reader.Seek(0, io.SeekEnd); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.ReadByte(); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})
}

func TestReaderReadHeader(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)

	t.Run("Empty buffer", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		if _, _, err := reader.ReadHeader(); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})

	t.Run("Last byte (EOF)", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		k, v := []byte("1"), []byte("abc")
		b.Write(k, v)
		if _, err := reader.Seek(0, io.SeekEnd); err != nil {
			t.Fatal(err)
		}
		_, _, err := reader.ReadHeader()
		if err != io.EOF {
			t.Errorf("expected EOF error, got: %v", err)
		}
	})

	t.Run("Incomplete header Unexpected EOF", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		// We write a single byte that represents an incomplete Uvarint.
		// 0x80 means "more bytes to follow", but we will provide no more.
		chunk := b.chunkPool.Get(b.chunkSize[0])
		chunk[0] = 0x80
		b.buf[0] = append(b.buf[0], chunk)
		b.writePos[0] = 1
		_, _, err := b.Read(0)

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("Valid header", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		k, v := []byte("1"), []byte("abc")
		_, writtenBytes, err := b.Write(k, v)
		if err != nil {
			t.Fatal(err)
		}
		h, hl, err := reader.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}
		if hl != 1 {
			t.Errorf("expected read bytes to be %d, got %d", 1, hl)
		}
		pl, _ := decodeHeader(h)
		if pl != writtenBytes-hl {
			t.Errorf("expected header encoded payload length %d, got %d", writtenBytes-hl, pl)
		}
	})
}

func TestReaderRead(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)

	t.Run("Read data into empty slice", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})

		b.Write([]byte("1"), generateBytes(t, testutils.MockChunkSizes[0], 0.5))
		p := []byte{}
		if _, err := reader.Read(p); err != nil {
			t.Error(err)
		}
	})

	t.Run("Read from empty buffer", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})

		p := make([]byte, 1)
		if _, err := reader.Read(p); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})

	t.Run("Read after EOF", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})
		b.Write([]byte("1"), []byte("abc"))
		reader.Seek(0, io.SeekEnd)

		p := make([]byte, 1) // Attempt to read the next byte.
		if _, err := reader.Read(p); err != io.EOF {
			t.Errorf("expected error io.EOF, got %v:", err)
		}
	})

	t.Run("Read some but not all the bytes (io.ReadFull)", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
			b.Reset()
		})

		b.Write([]byte("1"), []byte("abc"))
		e := make([]byte, b.Offset()+1) // Modify to expect more bytes than we can read.
		if _, err := io.ReadFull(reader, e); err != io.ErrUnexpectedEOF {
			t.Errorf("expected error io.ErrUnexpectedEOF, got: %v", err)
		}
	})
}

func TestReaderSeek(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)

	// Write data equal to the length of the first chunk.
	b.Write([]byte("1"), make([]byte, b.chunkSize[0]-(2+keyLenSize+1))) // 2+2+1+123 = 128
	if b.writePos[0] != b.chunkSize[0] {
		t.Fatalf("expected data to equal chunk size %d", b.chunkSize[0])
	}
	bufferLen := int64(b.Offset())

	t.Run("seek empty buffer", func(t *testing.T) {
		b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
		reader := NewReader(b)
		newOffset, err := reader.Seek(10, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if newOffset != 10 {
			t.Errorf("expected offset after seek to be 10, got %d", newOffset)
		}
	})

	t.Run("Seek from start", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		testCases := []struct {
			name              string
			offset            int64
			expectErr         bool
			expectFinalOffset int64
			expectChunkIdx    int
			expectPos         int
		}{
			{
				name:              "Seek to start",
				offset:            0,
				expectErr:         false,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         0,
			},
			{
				name:              "Seek to end",
				offset:            bufferLen,
				expectErr:         false,
				expectFinalOffset: bufferLen,
				expectChunkIdx:    int(bufferLen) / b.chunkSize[0],
				expectPos:         int(bufferLen % int64(b.chunkSize[0])),
			},
			{
				name:              "Seek forward within buffer",
				offset:            10,
				expectErr:         false,
				expectFinalOffset: 10,
				expectChunkIdx:    0,
				expectPos:         10,
			},
			{
				name:              "Seek past end of buffer",
				offset:            bufferLen + 1,
				expectErr:         false,
				expectFinalOffset: bufferLen + 1,
				expectChunkIdx:    int(bufferLen) / b.chunkSize[0],
				expectPos:         int((bufferLen + 1) % int64(b.chunkSize[0])),
			},
			{
				name:              "Seek past start of buffer",
				offset:            -10,
				expectErr:         true,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         0,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reader.Reset()
				newOffset, err := reader.Seek(tc.offset, io.SeekStart)
				if (err != nil) != tc.expectErr {
					t.Fatalf("expected error, got: %v", err)
				}
				if newOffset != tc.expectFinalOffset {
					t.Errorf("expected returned offset to be %d, got %d", tc.expectFinalOffset, newOffset)
				}
				if reader.chunkIdx != tc.expectChunkIdx || reader.pos != tc.expectPos {
					t.Errorf(
						"expected reader position to be (%d, %d), got (%d, %d)",
						tc.expectChunkIdx, tc.expectPos, reader.chunkIdx, reader.pos,
					)
				}
			})
		}
	})

	t.Run("Seek from current", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		const initialOffset = 7
		reader.Seek(initialOffset, io.SeekStart)

		testCases := []struct {
			name              string
			offset            int64
			expectErr         bool
			expectFinalOffset int64
			expectChunkIdx    int
			expectPos         int
		}{
			{
				name:              "Seek forward",
				offset:            7,
				expectErr:         false,
				expectFinalOffset: initialOffset + 7,
				expectChunkIdx:    0,
				expectPos:         initialOffset + 7,
			},
			{
				name:              "Seek backward",
				offset:            -7,
				expectErr:         false,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         0,
			},
			{
				name:              "Seek past end",
				offset:            int64(b.chunkSize[0]),
				expectErr:         false,
				expectFinalOffset: initialOffset + int64(b.chunkSize[0]),
				expectChunkIdx:    (initialOffset + int(bufferLen)) / b.chunkSize[0],
				expectPos:         int(bufferLen) + initialOffset - b.chunkSize[0],
			},
			{
				name:              "Seek before start",
				offset:            -15,
				expectErr:         true,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         initialOffset,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reader.Seek(initialOffset, io.SeekStart)
				newOffset, err := reader.Seek(tc.offset, io.SeekCurrent)
				if (err != nil) != tc.expectErr {
					t.Fatalf("expected error, got: %v", err)
				}
				if newOffset != tc.expectFinalOffset {
					t.Errorf("expected returned offset to be %d, got %d", tc.expectFinalOffset, newOffset)
				}
				if reader.chunkIdx != tc.expectChunkIdx || reader.pos != tc.expectPos {
					t.Errorf(
						"expected reader position to be (%d, %d), got (%d, %d)",
						tc.expectChunkIdx, tc.expectPos, reader.chunkIdx, reader.pos,
					)
				}
			})
		}
	})

	t.Run("Seek from end", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		testCases := []struct {
			name              string
			offset            int64
			expectErr         bool
			expectFinalOffset int64
			expectChunkIdx    int
			expectPos         int
		}{
			{
				name:              "Seek to end",
				offset:            0,
				expectErr:         false,
				expectFinalOffset: bufferLen,
				expectChunkIdx:    int(bufferLen) / b.chunkSize[0],
				expectPos:         int(bufferLen) % b.chunkSize[0],
			},
			{
				name:              "Seek to start",
				offset:            -bufferLen,
				expectErr:         false,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         0,
			},
			{
				name:              "Seek past end",
				offset:            1,
				expectErr:         false,
				expectFinalOffset: bufferLen + 1,
				expectChunkIdx:    int(bufferLen+1) / b.chunkSize[0],
				expectPos:         int(bufferLen+1) % b.chunkSize[0],
			},
			{
				name:              "Seek before start",
				offset:            -(bufferLen + 1),
				expectErr:         true,
				expectFinalOffset: 0,
				expectChunkIdx:    0,
				expectPos:         0,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reader.Reset()
				newOffset, err := reader.Seek(tc.offset, io.SeekEnd)
				if (err != nil) != tc.expectErr {
					t.Fatalf("expected error, got: %v", err)
				}
				if newOffset != tc.expectFinalOffset {
					t.Errorf("expected returned offset to be %d, got %d", tc.expectFinalOffset, newOffset)
				}
				if reader.chunkIdx != tc.expectChunkIdx || reader.pos != tc.expectPos {
					t.Errorf(
						"expected reader position to be (%d, %d), got (%d, %d)",
						tc.expectChunkIdx, tc.expectPos, reader.chunkIdx, reader.pos)
				}
			})
		}
	})

	t.Run("Seek reader when outside of bounds", func(t *testing.T) {
		t.Cleanup(func() {
			reader.Reset()
		})
		reader.chunkIdx = 1
		reader.pos = 0
		newOffset, err := reader.Seek(-10, io.SeekCurrent) // 16 - 10 = 6
		if err != nil {
			t.Errorf("failed to seek reader: %v", err)
		}
		if newOffset != int64(b.Offset()-10) {
			t.Errorf("expected offset after seek to be %d, got %d", b.Offset()-10, newOffset)
		}

		reader.chunkIdx = 1
		reader.pos = 0
		newOffset, err = reader.Seek(bufferLen, io.SeekStart)
		if err != nil {
			t.Errorf("failed to seek reader: %v", err)
		}
		if newOffset != bufferLen {
			t.Errorf("expected offset after seek to be %d, got %d", bufferLen, newOffset)
		}

		reader.chunkIdx = 1
		reader.pos = 0
		newOffset, err = reader.Seek(-bufferLen, io.SeekEnd)
		if err != nil {
			t.Errorf("failed to seek reader: %v", err)
		}
		if newOffset != 0 {
			t.Errorf("expected offset after seek to be %d, got %d", 0, newOffset)
		}
	})
}

func TestReaderReadEntry(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)

	key := []byte("1")
	value := generateBytes(t, testutils.MockChunkSizes[0], 0.5)
	offset, _, err := b.Write(key, value)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = b.Read(offset)
	if err != nil {
		t.Fatalf("failed to read entry from buffer: %v", err)
	}
	h, _, err := reader.ReadHeader()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}
	pl, _ := decodeHeader(h)
	expectedPLen := keyLenSize + 1 + len(value)
	if pl != expectedPLen {
		t.Errorf("expected payload length in header to be %d, got %d", expectedPLen, pl)
	}

	e := make([]byte, pl)
	if _, err := reader.Read(e); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}
	if len(e) != pl {
		t.Errorf("expected payload to have length %d, got %d", pl, len(e))
	}
	readKey, readValue, err := decodeEntry(e)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}
	if !bytes.Equal(key, readKey) {
		t.Errorf("expected key %s, got %s", key, readKey)
	}
	if !bytes.Equal(readValue, value) {
		t.Errorf("expected value %q, got %q", value, readValue)
	}

	if _, _, err := reader.ReadHeader(); err != io.EOF {
		t.Errorf("expected error io.EOF, got %v:", err)
	}
}

func TestReaderReadMultipleEntries(t *testing.T) {
	b, _ := NewTestBuffer(t, testutils.MockChunkSizes[0], 0)
	reader := NewReader(b)
	entries := generateRandomEntries(t, time.Now().UnixMicro(), testutils.MockChunkSizes[0], 10, 1)

	for _, e := range entries {
		b.Write(e.Key, e.Value)
	}
	for range entries {
		h, _, err := reader.ReadHeader()
		if err != nil {
			t.Fatalf("failed to read header: %v", err)
		}
		pl, _ := decodeHeader(h)
		p := make([]byte, pl)
		if _, err := io.ReadFull(reader, p); err != nil {
			t.Fatalf("failed to read payload")
		}
	}
	if _, _, err := reader.ReadHeader(); err != io.EOF {
		t.Errorf("expected error io.EOF, got %v:", err)
	}
}
