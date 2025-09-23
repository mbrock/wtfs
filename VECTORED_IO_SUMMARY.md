# SegmentedStringBuffer Vectored I/O Implementation

## Summary

Successfully implemented efficient vectored I/O for SegmentedStringBuffer's TokenReader using a three-writeVec pattern that maps geometric shelf structure directly to `pwritev` syscalls.

## Key Innovation: Limited Token Approach

The critical insight was to create a "limited token" that respects the `std.Io.Limit` parameter. This is now encapsulated in a clean `Token.limit()` method:

```zig
// Clean API for creating limited tokens
const limited_token = self.token.limit(self.pos, limit);
const seg_view = self.buffer.view(limited_token);
```

The `Token.limit(pos, io_limit)` method:
- Takes a position offset within the token
- Respects the `std.Io.Limit` parameter (unlimited or specific byte count)
- Returns a new token covering exactly the allowed bytes
- Uses saturating arithmetic to handle edge cases safely

This prevents `WriteFailed` errors by ensuring the segmented view only includes data that fits within constraints.

## Three-WriteVec Pattern

```zig
// 1. Head: partial first shelf
if (seg_view.head.len > 0) {
    const wrote = try w.writeVec(&[_][]const u8{seg_view.head});
    total_written += wrote;
}

// 2. Body: complete geometric shelves (perfect vector)
if (seg_view.body.len > 0) {
    const wrote = try w.writeVec(seg_view.body);
    total_written += wrote;
}

// 3. Tail: partial last shelf
if (seg_view.tail.len > 0) {
    const wrote = try w.writeVec(&[_][]const u8{seg_view.tail});
    total_written += wrote;
}
```

## Real System Call Traces

### Small File (38 bytes, prealloc=16)
```
pwritev(3, [{iov_len=38}], 1, 0) = 38
```
- Single syscall for entire file (fits in one shelf)

### Medium File (10240 bytes, prealloc=16, no limits)
```
pwritev(3, [{16}, {32}, {64}, {128}, {256}, {512}, {1024}, {2048}, {4096}], 9, 0) = 8176
pwritev(3, [{iov_len=2064}], 1, 8176) = 2064
```
- First call: 9 geometric shelves in one vectored syscall (16+32+64+128+256+512+1024+2048+4096 = 8176)
- Second call: Remaining 2064 bytes
- Total: 2 syscalls for 10KB file

### Medium File with Reader Limits (1024 bytes per call)
```
pwritev(3, [{16}, {32}, {64}, {128}, {256}, {512}, {16}], 7, 0) = 1024
pwritev(3, [{1008}, {16}], 2, 1024) = 1024
pwritev(3, [{1024}], 1, 2048) = 1024
...
```
- Each call respects 1024-byte limit exactly
- Multiple memory segments combined efficiently
- Perfect geometric progression patterns

## Performance Benefits

1. **Vectored Efficiency**: Multiple memory segments written in single syscalls
2. **Limit Compliance**: Natural respect for Reader interface limits
3. **No Buffer Copying**: Direct writes from geometric shelves
4. **Predictable Performance**: Each writeVec maps to one pwritev syscall
5. **Scalable**: Works efficiently from bytes to megabytes

## Comparison with Alternatives

### Byte-by-Byte Writing
- ✓ Simple implementation
- ✓ Works with any buffer size
- ✗ Relies on writer buffering
- ✗ Less explicit about I/O patterns

### Naive Vectored Writing
- ✗ Can exceed limit parameters → WriteFailed
- ✗ Complex limit checking logic
- ✗ Potential for partial writes

### Limited Token Approach (Our Solution)
- ✓ Respects all constraints naturally
- ✓ Optimal syscall patterns
- ✓ Clean three-writeVec implementation
- ✓ Maps geometric structure to efficient I/O

## Files Modified

- `src/SegmentedStringBuffer.zig`: Enhanced TokenReader.stream() with vectored I/O
- `docs/zig-io-primer.md`: Added advanced vectored I/O section with real traces
- `vector_demo.zig`: Clean demonstration program
- `bytewise_demo.zig`: Byte-by-byte comparison implementation

## Usage

```bash
# Build and test vectored I/O demo
zig build-exe vector_demo.zig
./vector_demo medium.txt 1024

# Observe system calls
strace -e trace=pwritev ./vector_demo medium.txt 1024
```

This implementation demonstrates how geometric data structures can create naturally efficient I/O patterns that map well to modern POSIX vectored I/O capabilities.