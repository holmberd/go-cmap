# Buffer

## Entry Layout
Entries are stored sequentially across fixed-sized chunks, with a variable-length header followed by the payload.

`<header(Uvarint)><keyLen(2 bytes)<key(N bytes)><value(M bytes)>`

The header is a Uvarint-encoded integer where the lower bits are reserved for flags and the remaining bits store the length of the key-value payload.

The payload is a encoded key-value entry pair:
```
| Offset | Length | Field     | Type     | Encoding
|--------|--------|-----------|----------|-----------
| 0      | 2      | KeyLen(N) | uint16   | LE
| 2      | N      | Key       | []byte   |
| N      | M      | Value     | []byte   |
```

## Flags
Flags are used to mark the state of an entry without needing to read the full payload.

- Deleted: Marks an entry as permanently deleted. This allows the compactor to identify and skip dead entries by reading only the header.

- Tombstone: A temporary state used only during compaction to mark an entry in the source buffer that has been deleted after compaction has started.

## Compaction
Compaction is the process of reclaiming space from dead entries. It works by incrementally copying a small amount of live data bytes from a source buffer to a new destination buffer. This helps to avoid latency spikes when compacting very large entries.

An alternative is to copy one whole entry at a time. This would simplify the logic by removing the need for a separate write buffer, as new inserts could append directly to the destination buffer. However, this would introduce unacceptable latency for large entries. The incremental byte-based copy provides more predictable performance that can be amortized across operations.

A 3-buffer strategy is employed to handle new inserts and deletes that occur during a long-running compaction process:
- Buffer A (Source Buffer): The original buffer being compacted. During compaction, Live entries are copied from here.
- Buffer B (Destination Buffer): A new temporary buffer where live entries from Buffer A are copied to.
- Buffer C (Write Buffer): A separate buffer maintained by the caller, that accepts all new inserts that occur while compaction is in progress.

While compaction is in progress, entries can still be flagged as "deleted" in both the source and destination buffer.

### Write Offset Integrity
To handle a delete request for an entry in the source buffer during compaction, we cannot simply mark it as deleted, because the compactor would then skip it and violate the invariant: A compacted buffer's final write offset must be equal to the `LiveBytes` of the source buffer at the moment compaction began.

Instead, a two-phase process is used:
1. The entry in the source buffer is marked with the tombstone flag. The compactor still sees this as a "live" entry that needs to be processed and not skipped.
2. When the compactor copies the tombstoned entry to the destination buffer, it converts the flag from tombstone to deleted so that it will be removed during the next compaction run.

This ensures that the space occupied by the dead entry is correctly accounted for in the final merged buffer's stats, which is required for maintaining correct write offsets when merging the compacted buffer with a write buffer.

## Adaptive Chunk Resizing
Adaptive chunk resizing is used to reduce GOGC pressure by minimizing the number of chunks (pointers) that are stored in the buffer. As the buffer size grows and shrinks, the size of its chunks will grow and shrink depending on the config parameter `P`.

As a general rule, larger chunks will reduce GC presure at the cost of higher potential memory waste. Using smaller chunks will have the opposite effect. See `config.go`.
