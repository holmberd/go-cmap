package cmap

import (
	"bytes"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/holmberd/go-cmap/internal/buffer"
	"github.com/holmberd/go-cmap/internal/testutils"
)

func keyHash(k []byte) uint64 {
	return xxhash.Sum64(k)
}

func TestBucketInsertAndGet(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	value := []byte("hello world")
	keys := [][]byte{[]byte("1"), []byte("2"), []byte("1")}
	for _, k := range keys {
		if _, err := b.Insert(k, value, keyHash(k)); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	if b.Len() != len(keys)-1 {
		t.Fatalf("expected %d entries, got %d", len(keys)-1, b.Len())
	}
	for _, k := range keys {
		gotKey, gotValue, found, err := b.Get(keyHash(k))
		if err != nil || !found {
			t.Fatalf("failed to get: %v", err)
		}
		if !bytes.Equal(gotKey, k) {
			t.Errorf("expected key %q, got %q", k, gotKey)
		}
		if !bytes.Equal(gotValue, value) {
			t.Errorf("expected value %q, got %q", value, gotValue)
		}
	}
}

func TestBucketUpdate(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	key := []byte("99")
	_, err := b.Insert(key, []byte("initial"), keyHash(key))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	updatedValue := []byte("updated")
	existed, err := b.Insert(key, updatedValue, keyHash(key))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if !existed {
		t.Error("expected entry to exist")
	}
	gotKey, gotValue, found, err := b.Get(keyHash(key))
	if err != nil || !found {
		t.Fatalf("failed to get: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("expected key %q, got %q", key, gotKey)
	}
	if !bytes.Equal(gotValue, updatedValue) {
		t.Errorf("expected value %q, got %q", updatedValue, gotValue)
	}
	if b.Len() != 1 {
		t.Errorf("expected one entry, got %d", b.Len())
	}
}

func TestBucketHas(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	key := []byte("1")
	if _, err := b.Insert(key, make([]byte, 10), keyHash(key)); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if exists := b.Has(keyHash(key)); !exists {
		t.Error("expected key to exists")
	}
	if _, err := b.Delete(keyHash(key)); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if exists := b.Has(keyHash(key)); exists {
		t.Error("expected key not to exists")
	}
}

func TestBucketGetMissing(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	key := []byte(strconv.Itoa(0xdeadbeef))
	k, v, ok, err := b.Get(keyHash(key))
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if ok {
		t.Error("expected entry to be missing")
	}
	if k != nil {
		t.Error("expected key to be nil")
	}
	if v != nil {
		t.Error("expected value to be nil")
	}
}

func TestBucketGetReturnCopy(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	key := []byte("1")
	b.Insert(key, []byte("immutable"), keyHash(key))
	_, got1, ok, err := b.Get(keyHash(key))
	if err != nil || !ok {
		t.Fatalf("failed to get: %v", err)
	}
	got1[0] = 'x' // Modify return entry data.
	_, got2, ok, err := b.Get(keyHash(key))
	if err != nil || !ok {
		t.Fatalf("failed to get: %v", err)
	}
	if got2[0] != 'i' {
		t.Error("expected entry to be unmodified")
	}
}

func TestBucketDelete(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	key := []byte("1")
	b.Insert(key, []byte("data"), keyHash(key))
	existed, err := b.Delete(keyHash(key))
	if err != nil {
		t.Errorf("failed to delete: %v", err)
	}
	if !existed {
		t.Error("expected key to exist during delete")
	}
	exist := b.Has(keyHash(key))
	if exist {
		t.Error("expected entry to have been deleted")
	}
}

func TestBucketDeleteMissing(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	h := uint64(1)
	exists, err := b.Delete(h)
	if err != nil {
		t.Errorf("failed to delete: %v", err)
	}
	if exists {
		t.Error("expected key to no exists")
	}
}

func TestBucketReset(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	config := buffer.DefaultConfig(pool)
	b := newBucket(pool, config)
	b.Insert([]byte("1"), []byte("abc"), keyHash([]byte("1")))
	b.Insert([]byte("2"), []byte("xyz"), keyHash([]byte("2")))
	b.Reset()
	if b.Len() != 0 {
		t.Error("expected number of entries in bucket to be 0")
	}
	if b.buf[0].Offset() != 0 {
		t.Error("expected buf[0] ")
	}
	if b.buf[1] != nil {
		t.Error("expected buf[1] to be nil")
	}
	if b.keys[0] == nil {
		t.Error("expected keys[0] to be initialized")
	}
	if b.keys[1] != nil {
		t.Error("Expected keys[1] to be nil")
	}
	if b.config != config {
		t.Error("expected config not to be reset")
	}
	if b.baseOffset != 0 {
		t.Error("expected baseOffset to be 0")
	}
	if b.state != stateIdle {
		t.Error("expected state to be idle")
	}
}

func TestBucketConcurrentInsertAndGet(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	n := 1000
	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i + 1)} // uint8 rollover.
			if _, err := b.Insert(key, key, keyHash(key)); err != nil {
				t.Errorf("failed to insert: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i + 1)}
			gotKey, gotValue, ok, err := b.Get(keyHash(key))
			if err != nil || !ok {
				t.Errorf("failed to get: %v", err)
				return
			}
			if !bytes.Equal(gotKey, key) {
				t.Errorf("expected key %q, got %q", key, gotKey)
			}
			want := []byte{byte(i + 1)}
			if !bytes.Equal(gotValue, want) {
				t.Errorf("expected value %v with key %s, got %v", want, key, gotValue)
			}
		}(i)
	}
	wg.Wait()
}

func TestBucketConcurrentInsertAndDelete(t *testing.T) {
	pool := NewChunkPool(DefaultChunkPoolConfig())
	b := newBucket(pool, buffer.DefaultConfig(pool))
	n := 1000
	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i + 1)}
			if _, err := b.Insert(key, key, keyHash(key)); err != nil {
				t.Errorf("failed to insert: %v", err)
				return
			}
			if _, err := b.Delete(keyHash(key)); err != nil {
				t.Errorf("failed to delete: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()
}

// assertEntries checks that the bucket contains the expected entries.
func assertEntries(t *testing.T, b *bucket[*testutils.MockChunkPool], expected map[uint64][]byte) {
	t.Helper()
	if b.isCompacting() {
		t.Fatal("illegal entries assertion: bucket is compacting")
		return
	}
	if b.Len() != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), b.Len())
	}

	entries := make(map[uint64][]byte, b.Len())
	for _, k := range slices.Collect(maps.Keys(b.keys[0])) {
		_, v, ok, err := b.Get(k)
		if err != nil || !ok {
			t.Fatalf("failed to get: %v", err)
		}
		entries[k] = v
	}
	if !maps.EqualFunc(entries, expected, bytes.Equal) {
		t.Error("expected entries to match")
	}
}

// assertCompactingState checks that the bucket is in a valid compacting state.
func assertCompactingState(t *testing.T, b *bucket[*testutils.MockChunkPool]) {
	t.Helper()
	if !b.isCompacting() {
		t.Fatalf("expected compaction to have started, state is %v", b.state)
	}
	if b.baseOffset <= 0 {
		t.Fatalf("expected baseOffset to be set, got %v", b.baseOffset)
	}
	if b.buf[1] == nil {
		t.Fatal("expected buf[1] to be initialized")
	}
}

// assertInStaging checks that a key exists in the staging map (keys[1])
// and not in the source map (keys[0]).
func assertInStaging(t *testing.T, b *bucket[*testutils.MockChunkPool], key uint64) {
	t.Helper()
	if _, ok := b.keys[0][key]; ok {
		t.Errorf("expected key %d to not be in source (keys[0])", key)
	}
	off, ok := b.keys[1][key]
	if !ok {
		t.Errorf("expected key %d to be in staging (keys[1])", key)
	}
	if !(off >= b.baseOffset) {
		t.Errorf("expected key %d offset %d to be >= baseOffset %d", key, off, b.baseOffset)
	}
}

// assertValue checks that b.Get(key) returns the expected value.
func assertValue(t *testing.T, b *bucket[*testutils.MockChunkPool], key uint64, expected []byte) {
	t.Helper()
	_, got, ok, err := b.Get(key)
	if err != nil || !ok {
		t.Fatalf("failed to get key %d: %v", key, err)
	}
	if got == nil {
		t.Fatalf("expected key %d to exist", key)
	}
	if !bytes.Equal(got, expected) {
		t.Errorf("value mismatch for key %d: expected %s, got %s", key, expected, got)
	}
}

// assertIdleState checks that the bucket is in a clean idle state.
func assertIdleState(t *testing.T, b *bucket[*testutils.MockChunkPool], expectedLen int) {
	t.Helper()
	if b.state != stateIdle {
		t.Fatalf("expected bucket to be in an idle state, got %v", b.state)
	}
	if len(b.keys[1]) != 0 {
		t.Errorf("expected keys[1] to be empty and reset, len is %d", len(b.keys[1]))
	}
	if b.baseOffset != 0 {
		t.Errorf("expected baseOffset to be 0, got %d", b.baseOffset)
	}
	if len(b.keys[0]) != expectedLen {
		t.Errorf("expected keys[0] to have %d keys, got %d", expectedLen, len(b.keys[0]))
	}
}

func TestBucketCompaction(t *testing.T) {
	pool := &testutils.MockChunkPool{}
	config := buffer.Config{
		P:                     0.75,
		ChunkSize:             testutils.MockChunkSizes[0],
		CompactBytes:          testutils.MockChunkSizes[0],
		CompactDeadRatio:      0.1, // Trigger compaction on 10% dead space.
		CompactDeadChunkRatio: 0.5,
	}
	b := newBucket(pool, config)

	keyIndex := 0
	genKey := func() []byte {
		keyIndex++
		return []byte(strconv.Itoa(keyIndex))
	}

	numEntries := 1000
	entries := make(map[uint64][]byte, numEntries) // hash -> value
	keys := make(map[uint64][]byte, numEntries)    // hash -> key
	for range numEntries {
		k := genKey()
		keys[keyHash(k)] = k
		entries[keyHash(k)] = make([]byte, testutils.MockChunkSizes[0]/2)
	}

	// Fill buffer with entries.
	for h, v := range entries {
		if _, err := b.Insert(keys[h], v, h); err != nil {
			t.Fatal(err)
		}
	}

	// Ensure any compaction started as a result of inserting the entries has completed.
	for i := 2000; b.isCompacting(); i-- {
		if err := b.compactStep(); err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		if i == 0 {
			t.Fatal("compaction took too many steps")
		}
	}
	assertEntries(t, b, entries)

	// Phase 1: Buffer Compaction.

	// Delete entries until compaction is triggered.
	hashes := slices.Collect(maps.Keys(entries))
	for i := 0; !b.isCompacting(); i++ {
		if i > len(keys)-1 {
			t.Fatal("not enough entries to trigger compaction")
		}
		h := hashes[i]
		if _, err := b.Delete(h); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		delete(entries, h)
	}

	// Verify compaction state
	s := b.buf[0].GetStats()
	deadRatio := float64(s.DeadBytes) / float64(s.LiveBytes)
	if deadRatio < config.CompactDeadRatio {
		t.Fatalf(
			"expected buffer to exceed dead ratio of %f, got %f",
			config.CompactDeadRatio,
			deadRatio,
		)
	}
	assertCompactingState(t, b)

	// Perform operations during compaction:

	hashes = slices.Collect(maps.Keys(entries))
	slices.Sort(hashes)

	// Delete key from source during compaction.
	h := hashes[0]
	if existed, err := b.Delete(h); !existed || err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	delete(entries, h)
	if b.Has(h) {
		t.Error("expected key to have been deleted")
	}

	// Update an existing key in source to staging during compaction.
	h = hashes[1]
	value := []byte("updated")
	if _, err := b.Insert(keys[h], value, h); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	entries[h] = value
	assertInStaging(t, b, h)
	assertValue(t, b, h, value)

	// Delete key from staging during compaction.
	key := genKey()
	h = keyHash(key)
	value = []byte("delete")
	if _, err := b.Insert(key, value, h); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if _, ok := b.keys[1][h]; !ok {
		t.Error("expected new key to be in staging (keys[1])")
	}
	if existed, err := b.Delete(h); !existed || err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if b.Has(h) {
		t.Error("expected key to have been deleted")
	}

	// Insert a new key to staging during compaction.
	newKey := genKey()
	newHash := keyHash(newKey)
	value = []byte("new")
	if _, err := b.Insert(newKey, value, newHash); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	entries[newHash] = value
	keys[newHash] = newKey
	assertInStaging(t, b, newHash)
	assertValue(t, b, newHash, value)

	// Update key in staging during compaction.
	value = []byte("updated-staging")
	if _, err := b.Insert(newKey, value, newHash); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	entries[newHash] = value
	assertInStaging(t, b, newHash)
	assertValue(t, b, newHash, value)

	// Phase 2: Key Migration.

	// Run compaction until buffer merge and migration start.
	// After merge, all data is in buf[0], but keys are fragmented across keys[0] and keys[1].
	for i := 2000; b.state != stateMigratingKeys; i-- {
		if err := b.compactStep(); err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		if i == 0 {
			t.Fatal("compaction took to many steps")
		}
	}

	hashes = slices.Collect(maps.Keys(entries))
	slices.Sort(hashes)

	// Read keys from source and staging during migration.
	assertValue(t, b, newHash, entries[newHash])     // Check staging key.
	assertValue(t, b, hashes[0], entries[hashes[0]]) // Check source key.

	// Update key during migration.
	value = []byte("updated-migration")
	if updated, err := b.Insert(keys[hashes[0]], value, hashes[0]); !updated || err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	entries[hashes[0]] = value
	assertValue(t, b, hashes[0], value)
	assertInStaging(t, b, hashes[0])

	// Delete key during migration.
	if existed, err := b.Delete(hashes[1]); !existed || err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	delete(entries, hashes[1])
	delete(keys, hashes[1])
	if b.Has(hashes[1]) {
		t.Error("expected key to be deleted")
	}

	// Insert new key during migration.
	newKey = genKey()
	h = keyHash(newKey)
	value = []byte("migration")
	if _, err := b.Insert(newKey, value, h); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	entries[h] = value
	keys[h] = newKey

	if _, ok := b.keys[0][h]; ok {
		t.Error("expected new key to not be in source")
	}
	if _, ok := b.keys[1][h]; !ok {
		t.Error("expected new key to be in staging")
	}
	assertValue(t, b, h, value)

	// Phase 3: Migrate all keys.

	keysToMigrate := len(b.keys[0])
	keysInNewMap := len(b.keys[1])
	if !(keysToMigrate > 0) {
		t.Error("keys[0] should have old keys to migrate")
	}
	if !(keysInNewMap > 0) {
		t.Error("keys[1] should have new/updated keys")
	}
	totalKeys := b.Len()

	// Complete key migration.
	for i := 2000; b.isCompacting(); i-- {
		if err := b.compactStep(); err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		if i == 0 {
			t.Fatal("compaction took too many steps")
		}
	}
	assertIdleState(t, b, totalKeys)
	assertEntries(t, b, entries)
}

func TestBucketCompactionWithEmptyStaging(t *testing.T) {
	pool := &testutils.MockChunkPool{}
	config := buffer.Config{
		P:                     0.75,
		ChunkSize:             testutils.MockChunkSizes[0],
		CompactBytes:          testutils.MockChunkSizes[0],
		CompactDeadRatio:      0.1, // Trigger compaction on 10% dead space.
		CompactDeadChunkRatio: 0.5,
	}
	b := newBucket(pool, config)

	// Fill buffer with entries until compaction is triggered
	entries := make(map[uint64][]byte)
	for i := 0; !b.isCompacting(); i++ {
		k := []byte(strconv.Itoa(i + 1))
		v := make([]byte, testutils.MockChunkSizes[0]/2)
		if _, err := b.Insert(k, v, keyHash(k)); err != nil {
			t.Fatal(err)
		}
		entries[keyHash(k)] = v

		for i > 1000 {
			t.Fatal("took too many steps to trigger compaction")
		}
	}
	totalKeysBefore := b.Len()

	if b.state != stateCompacting {
		t.Errorf("expected bucket state to be %s, got %s", stateCompacting, b.state)
	}

	for i := 1000; b.state == stateCompacting; i-- {
		if err := b.compactStep(); err != nil {
			t.Fatalf("failed to compact: %v", err)
		}
		for i == 0 {
			t.Fatalf("compaction took too many steps")
		}
	}

	// Assert that key migration is skipped when there's not new keys in staging during merge.
	if b.isCompacting() {
		t.Errorf("expected bucket state to be idle, got %s", b.state)
	}
	if totalKeysBefore != b.Len() {
		t.Errorf("expected total keys to be %d, got %d", totalKeysBefore, b.Len())
	}
	assertEntries(t, b, entries)
}
