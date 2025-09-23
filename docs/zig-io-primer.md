# Zig 0.15 std.Io.Writer / std.Io.Reader — A Brisk Primer for Engineers

Zig 0.15's IO is built around two concrete structs—Writer and Reader—with a small vtable for slow-path behavior. The trick: the fast paths operate on a visible in-struct buffer. This lets the standard helpers (print, writeVec, writeSplat, read*, etc.) run without virtual calls when possible, and only touch the vtable when the buffer can't satisfy the request.

Think "hot path inlined, cold path delegated."

## Why This Design?

- **Predictable hot path**: things like `writer.print()` and `writer.writeVec()` manipulate `writer.buffer[0..end]` directly. No dynamic dispatch when it fits.
- **Pluggable slow path**: when data doesn't fit, `Writer.vtable.drain()` gets called with a `[]const[]const u8` multi-slice plus a splat count (repeat the last slice).
- **Zero-copy friendly**: `writeVec` and `sendFile` map cleanly to `writev`/`pwritev` and `sendfile`.
- **Symmetric for reading**: Reader exposes a buffer + indices (seek, end) and delegates to `vtable.stream()` when it needs more bytes.

### Performance Rationale

This design eliminates virtual calls in the common case where operations fit within the buffer. Consider these scenarios:

```zig
// Fast path - no virtual calls
try writer.writeAll("hello");           // fits in buffer
try writer.print("count: {}", .{42});   // fits in buffer

// Slow path - triggers drain() once
try writer.writeAll(huge_string);       // exceeds buffer, calls drain()
```

The buffer acts as a write-combining cache, batching small writes into efficient larger operations.

## Anatomy (Writer)

```zig
const Writer = struct {
  vtable: *const VTable,
  buffer: []u8, // visible user buffer
  end: usize,   // bytes staged in buffer

  pub const VTable = struct {
    drain: *const fn (w: *Writer, data: []const []const u8, splat: usize) Error!usize,
    sendFile: *const fn (w:*Writer, fr:*File.Reader, limit: Limit) FileError!usize = unimplementedSendFile,
    flush: *const fn (w:*Writer) Error!void = defaultFlush,
    rebase: *const fn (w:*Writer, preserve:usize, capacity:usize) Error!void = defaultRebase,
  };
};
```

- `write` / `writeVec` first try to place bytes into `buffer[0..end]`.
- If not enough space, they call `vtable.drain` (and sometimes `rebase`).

### Understanding the Multi-Slice Pattern

The `data: []const []const u8` parameter to `drain()` is crucial. It represents:

1. **Batch efficiency**: Multiple discrete write operations can be combined into a single syscall
2. **Zero-copy semantics**: No need to copy disparate buffers into a single contiguous block
3. **Natural mapping**: Maps directly to vectored IO operations like `writev()`

For example, if you have buffered bytes plus new data:
```zig
// Conceptually, drain() receives something like:
const data = &.{
    buffered_bytes,     // what was already staged
    new_slice_1,        // from your writeVec() call
    new_slice_2,        // from your writeVec() call
    pattern_slice,      // for splat operations (repeated 'splat' times)
};
```

## Anatomy (Reader)

```zig
const Reader = struct {
  vtable: *const VTable,
  buffer: []u8,
  seek: usize, // consumed from buffer
  end: usize,  // produced into buffer

  pub const VTable = struct {
    stream: *const fn (r:*Reader, w:*Writer, limit:Limit) StreamError!usize,
    discard:*const fn (r:*Reader, limit:Limit) Error!usize = defaultDiscard,
    readVec:*const fn (r:*Reader, data:[][]u8) Error!usize = defaultReadVec,
    rebase:*const fn (r:*Reader, capacity:usize) RebaseError!void = defaultRebase,
  };
};
```

- `stream(r, w, limit)` pushes up to `limit` bytes into `w` (often using `writeVec`).
- `readVec` can opt to fill `Reader.buffer` directly for zero-copy.

### The Stream-to-Writer Pattern

The Reader doesn't just fill buffers—it streams data to Writers. This creates a composable pipeline:

```
Source → Reader → Writer → Sink
```

Each stage can transform, buffer, or redirect data. For example:
- A file Reader streams to a compression Writer
- The compression Writer streams to a network Writer
- All using the same interface

## Minimal Custom Writer (Counting Bytes)

Showcasing parent-field pointer (`@fieldParentPtr`) to find your object from the embedded Writer:

```zig
const std = @import("std");

const CountingWriter = struct {
    writer: std.Io.Writer,
    count: usize = 0,

    pub fn init() CountingWriter {
        return .{ .writer = .{ .vtable = &vtable, .buffer = &.{} } };
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *CountingWriter = @fieldParentPtr("writer", w);
        var total: usize = 0;

        // Process all slices except the last one normally
        for (data[0 .. data.len - 1]) |d| {
            self.count += d.len;
            total += d.len;
        }

        // Handle the last slice with potential splatting
        const pat = data[data.len - 1];
        self.count += pat.len * splat;
        total += pat.len * splat;

        return total; // pretend all consumed
    }

    fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void { }

    const vtable: std.Io.Writer.VTable = .{
        .drain = drain,
        .flush = flush,
        .rebase = std.Io.Writer.defaultRebase,
    };
};

// usage
test "counting writer" {
    var cw = CountingWriter.init();
    try cw.writer.writeAll("hello");
    try cw.writer.writeSplat(&.{ "ab" }, 3); // "ababab"
    try std.testing.expectEqual(@as(usize, 5 + 6), cw.count);
}
```

### Key Insights on @fieldParentPtr

The `@fieldParentPtr` builtin is essential for this pattern. It recovers the containing struct from a pointer to one of its fields:

```zig
const self: *CountingWriter = @fieldParentPtr("writer", w);
//    ^-- your struct           ^-- field name  ^-- field pointer
```

This works because:
1. Zig guarantees struct field layout
2. The offset from struct start to field is compile-time constant
3. Simple pointer arithmetic recovers the parent

This pattern appears throughout Zig's codebase for embedded callback contexts.

## Advanced Vectored I/O with SegmentedStringBuffer

The SegmentedStringBuffer demonstrates sophisticated vectored I/O patterns that achieve optimal system call efficiency. The key insight is that Zig's geometric shelf structure naturally maps to efficient `pwritev` syscalls.

### Three-WriteVec Pattern

The TokenReader implements a three-writeVec approach that respects both buffer limits and vectored I/O constraints:

```zig
fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) StreamError!usize {
    // Create limited token that respects the limit parameter
    const limited_token = self.token.limit(self.pos, limit);
    const seg_view = self.buffer.view(limited_token);

    // Three writeVec calls map to efficient pwritev syscalls:

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
}
```

### Real System Call Traces

With `prealloc=16` and geometric growth, here are actual `strace` traces showing the vectored I/O efficiency:

**Small file (38 bytes):**
```
pwritev(4, [{iov_base="...", iov_len=16}, {iov_base="...", iov_len=22}], 2, 0) = 38
```
- Head: 16 bytes (first shelf)
- Tail: 22 bytes (remainder)
- Total: 1 syscall for entire file

**Medium file (10240 bytes) with limit=1024:**
```
pwritev(4, [{iov_base="...", iov_len=16}, {32}, {64}, {128}, {256}, {512}, {16}], 7, 0) = 1024
pwritev(4, [{iov_base="...", iov_len=1008}, {iov_base="...", iov_len=16}], 2, 1024) = 1024
pwritev(4, [{iov_base="...", iov_len=1024}], 1, 2048) = 1024
...
```
- Each call writes exactly 1024 bytes (the limit)
- Multiple memory segments combined into single syscalls
- Perfect geometric progression: 16+32+64+128+256+512+16 = 1024

### Key Benefits

1. **Limit Compliance**: Creates limited tokens that naturally respect Reader limits
2. **Vectored Efficiency**: Multiple memory segments written in single syscalls
3. **No Buffer Copying**: Direct vectored writes from geometric shelves
4. **Predictable Performance**: Each writeVec maps to one pwritev syscall

### Comparison with Byte-by-Byte

Byte-by-byte writes rely on writer buffering:
- 10240 individual `writeByte()` calls
- Writer buffers everything, flushes as single `pwritev`
- Works well but less explicit about I/O patterns

Vectored approach:
- 3 `writeVec()` calls per limit-sized chunk
- Each writeVec immediately becomes efficient `pwritev`
- Explicit control over system call patterns

This demonstrates how geometric data structures can create naturally efficient I/O patterns that map well to modern POSIX vectored I/O capabilities.

## Minimal Custom Reader (From a Fixed Slice)

```zig
const SliceReader = struct {
    reader: std.Io.Reader,
    src: []const u8,
    pos: usize = 0,

    pub fn init(src: []const u8) SliceReader {
        return .{
            .reader = .{ .vtable = &vtable, .buffer = &.{} },
            .src = src
        };
    }

    fn stream(rp:*std.Io.Reader, w:*std.Io.Writer, limit: std.Io.Limit)
        std.Io.Reader.StreamError!usize
    {
        const self:*SliceReader = @fieldParentPtr("reader", rp);
        if (self.pos >= self.src.len) return 0;

        const available = self.src.len - self.pos;
        const n = @min(limit.toInt(available), available);

        // Prefer writeVec so the sink can batch efficiently
        const chunk = self.src[self.pos .. self.pos + n];
        const wrote = try w.writeVec(&.{ chunk });
        self.pos += wrote;
        return wrote;
    }

    const vtable: std.Io.Reader.VTable = .{ .stream = stream };
};
```

### Why writeVec Instead of writeAll?

Using `writeVec()` instead of `writeAll()` is more than just preference—it's about composability:

- `writeVec()` can handle partial writes gracefully
- It maps to vectored syscalls when writing to files/sockets
- It allows the destination Writer to batch multiple slices efficiently
- The caller can retry with remaining data if needed

## How the Standard File Writer Works (Roughly)

`std.fs.File.Writer`'s drain:
- Builds a small stack array of `iovec` entries combining:
  - any buffered bytes already in `writer.buffer[0..end]`,
  - your `data[0..len-2]`,
  - a splat expansion of `data[len-1]` if needed (it even synthesizes a scratch run for single-byte patterns).
- Calls `writev`/`pwritev` once.
- Updates its internal position and the Writer buffer state.

**Takeaway**: if you can give a `[]const[]const u8` (like shelf slices), `writeVec(All)` will flow straight to an efficient `writev`.

### The Magic of Vectored IO

Consider what happens when writing segmented data:

```zig
// Your segmented buffer has data across multiple "shelves"
const segments = &.{
    shelf_0[10..50],    // 40 bytes
    shelf_1[0..100],    // 100 bytes
    shelf_2[0..25],     // 25 bytes
};

try writer.writeVec(segments);
```

Without vectored IO, this would require:
1. Allocate a 165-byte temporary buffer
2. Copy all segments into it
3. Single write() syscall
4. Free the temporary buffer

With vectored IO:
1. Single `writev()` syscall with 3 `iovec` entries
2. No copying, no allocation
3. Same or better performance

## The Built-ins You'll Actually Use

- **Buffered fast path**: `writer.write(bytes)`, `writer.print(fmt, args)`, `writer.writeVec(slices)`.
- **Pattern-repeat**: `writer.writeSplat(slices, splat)`, `writer.splatByteAll(byte, n)`—great for padding (e.g., tar's 512-byte zero blocks).
- **Streaming**: `reader.streamExact(writer, n)`; `reader.readAllAlloc(alloc, max)` (helpers built on stream/readVec).

### Common Patterns

```zig
// Writing formatted data efficiently
try writer.print("Header: {s}\n", .{header});
try writer.writeVec(data_segments);        // Zero-copy multi-segment write
try writer.splatByteAll(0, padding_size);  // Efficient padding

// Streaming between sources and sinks
try source_reader.streamExact(dest_writer, byte_count);

// Bounded writing to prevent overruns
var buf: [4096]u8 = undefined;
var fixed_writer = std.Io.Writer.fixed(&buf);
try fixed_writer.print("Safe: {}", .{value});
const written = fixed_writer.getWritten();
```

## "Fixed" Writer (Bounded, No Growth)

Zig ships this already:

```zig
var scratch: [4096]u8 = undefined;
var w = std.Io.Writer.fixed(scratch[0..]); // writes until full → error.WriteFailed
```

You can wrap this in a higher-level API: write many small prints to scratch, then flush the staged bytes into your real sink (e.g., reservation inside your buffer).

### Pattern: Staged Writing

```zig
// Pattern: accumulate formatted output, then commit atomically
var scratch: [8192]u8 = undefined;
var staging = std.Io.Writer.fixed(&scratch);

// Build up complex output
try staging.print("Record #{}\n", .{id});
try staging.writeVec(field_segments);
try staging.print("Checksum: 0x{x}\n", .{checksum});

// Atomically commit to real destination
const staged_data = staging.getWritten();
try real_writer.writeAll(staged_data);
```

This pattern is invaluable for:
- Building packets/records that need length prefixes
- Ensuring atomic writes (all-or-nothing)
- Working with APIs that need contiguous buffers

## Pattern for a Practical Writer Implementation

1. **Stage into writer.buffer (fast)**.
2. **If full**: drain gets called with a multi-slice (data) and an optional splat; implement it by:
   - committing buffered bytes first,
   - sending data efficiently to your sink (maybe with writeVec to a file, or memcpy into a large ring/reservation),
   - returning how many bytes you consumed from data (short consumption is fine—helpers will retry).
3. **Optional rebase**: grow/rotate your visible buffer while keeping the last preserve bytes.

The Reader mirror is similar: stream pushes new bytes to a Writer (or fills Reader.buffer and returns 0 to signal "read from my buffer").

### Handling Partial Writes

One tricky aspect is handling partial consumption in `drain()`:

```zig
fn drain(w: *Writer, data: []const []const u8, splat: usize) Error!usize {
    const self: *MyWriter = @fieldParentPtr("writer", w);

    // Flush any buffered data first
    try self.flushBuffered();

    var consumed: usize = 0;

    // Try to write each slice
    for (data[0..data.len-1]) |slice| {
        const written = try self.writeToSink(slice);
        consumed += written;
        if (written < slice.len) {
            // Partial write - return early, caller will retry
            return consumed;
        }
    }

    // Handle splat pattern
    const pattern = data[data.len - 1];
    var remaining_splat = splat;
    while (remaining_splat > 0) {
        const written = try self.writeToSink(pattern);
        consumed += written;
        if (written < pattern.len) break;
        remaining_splat -= 1;
    }

    return consumed;
}
```

## A Tiny std.Io.Reader View for Your Segmented Buffer

Suppose your "token" points to len bytes starting at (shelf, off), possibly crossing shelves. This Reader will stream that region as 1–2 slices per shelf using writeVec:

```zig
const SegBufReader = struct {
    reader: std.Io.Reader,
    seg: *const SegBuf, // your structure
    shelf: usize,
    off: usize,
    left: usize,

    pub fn init(seg: *const SegBuf, tok: SegBuf.Token) SegBufReader {
        return .{
            .reader = .{ .vtable = &vtable, .buffer = &.{} },
            .seg = seg,
            .shelf = tok.shelfIndex(),
            .off = tok.byteOffset(),
            .left = tok.length(),
        };
    }

    fn stream(r:*std.Io.Reader, w:*std.Io.Writer, limit: std.Io.Limit)
        std.Io.Reader.StreamError!usize
    {
        const self:*SegBufReader = @fieldParentPtr("reader", r);
        if (self.left == 0) return 0;

        var want = limit.toInt(self.left);
        var local: [std.fs.File.Writer.max_buffers_len][]const u8 = undefined;
        var n: usize = 0;

        // Build slice vector from segmented storage
        while (want != 0 and n < local.len) {
            const shelf_cap = self.seg.shelfCapacity(self.shelf) orelse break;
            const shelf_data = self.seg.shelves[self.shelf];
            const take = @min(want, @min(shelf_cap - self.off, shelf_data.len - self.off));

            local[n] = shelf_data[self.off .. self.off + take];
            n += 1;
            want -= take;
            self.left -= take;

            // Move to next shelf
            self.shelf += 1;
            self.off = 0;
        }

        if (n == 0) return 0;

        // Write as many slices as writer can take; helpers retry if short
        const vec = local[0..n];
        const wrote = try w.writeVec(vec);

        // Note: In a complete implementation, you'd need to handle the case
        // where 'wrote' is less than the total slice length and adjust
        // shelf/off accordingly. For simplicity, this assumes full writes.

        return wrote;
    }

    const vtable: std.Io.Reader.VTable = .{ .stream = stream };
};
```

### Integration with Standard Libraries

This integrates perfectly with tar/http/zstd writers: they'll call `reader.streamExact(writer, n)`, and your stream will keep giving them shelf slices via `writeVec`, zero-copy.

```zig
// Example: stream segmented data to a tar archive
var seg_reader = SegBufReader.init(&my_segmented_buffer, token);
try tar_writer.writeFile("data.txt", token.length(), seg_reader.reader);
// The tar writer internally calls seg_reader.stream(), which provides
// shelf slices directly via writeVec() - no intermediate copying!
```

## Minimal "File-like" Writer Sketch

If you ever implement a file/socket-ish sink yourself, your drain will look very much like std's: gather iovecs from buffer + data + splat, call writev/pwritev, and then tell the Writer how many bytes were consumed (and shrink end accordingly). The beauty is: you don't need to re-invent it unless you're doing something fancier—the std File Writer already does the right thing.

```zig
const FileishWriter = struct {
    writer: std.Io.Writer,
    fd: std.os.fd_t,
    pos: u64,

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) Error!usize {
        const self: *FileishWriter = @fieldParentPtr("writer", w);

        // Build iovec array (simplified)
        var iovecs: [16]std.os.iovec = undefined;
        var iov_count: usize = 0;

        // Add buffered data
        if (w.end > 0) {
            iovecs[iov_count] = .{ .iov_base = w.buffer.ptr, .iov_len = w.end };
            iov_count += 1;
        }

        // Add data slices
        for (data[0..data.len-1]) |slice| {
            if (iov_count >= iovecs.len) break;
            iovecs[iov_count] = .{ .iov_base = slice.ptr, .iov_len = slice.len };
            iov_count += 1;
        }

        // Handle splat (simplified - real implementation would expand patterns)
        // ...

        // Single vectored write
        const written = std.os.writev(self.fd, iovecs[0..iov_count]);
        self.pos += written;

        // Update writer state
        if (written >= w.end) {
            w.end = 0;  // Buffer fully consumed
            return written - w.end;  // Return data consumption
        } else {
            // Partial buffer consumption (rare)
            std.mem.copyForwards(u8, w.buffer[0..], w.buffer[written..w.end]);
            w.end -= written;
            return 0;  // No data consumed
        }
    }
};
```

## Advanced Patterns

### Chaining Writers

You can chain writers for layered processing:

```zig
// Data flows: source → compression → encryption → network
var network_writer = NetworkWriter.init(socket);
var crypto_writer = CryptoWriter.init(network_writer.writer, key);
var compress_writer = CompressWriter.init(crypto_writer.writer);

// All writes flow through the chain
try compress_writer.writer.writeAll(plaintext_data);
```

### Circular Buffer Writer

For streaming applications, a circular buffer writer can provide bounded memory usage:

```zig
const CircularWriter = struct {
    writer: std.Io.Writer,
    buffer: []u8,
    head: usize,
    tail: usize,

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) Error!usize {
        // Write to circular buffer, potentially overwriting old data
        // Return how much was consumed
    }
};
```

### Memory-Mapped Writer

For very large outputs, memory-mapped writers can provide efficient access:

```zig
const MmapWriter = struct {
    writer: std.Io.Writer,
    mapping: []align(std.mem.page_size) u8,
    pos: usize,

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) Error!usize {
        // Write directly to memory-mapped region
        // Automatically grows the mapping if needed
    }
};
```

## TL;DR Checklists

### When implementing a Writer:
- ✅ Put your struct as a parent with an embedded `.writer`.
- ✅ Fill `.buffer` eagerly on hot path.
- ✅ In drain, commit buffered bytes first, then handle multi-slice + splat.
- ✅ Return the number of bytes consumed from data (0..sum), not counting buffered.
- ✅ Optionally implement rebase to grow the visible buffer (or leave default).

### When implementing a Reader:
- ✅ Keep your own position; expose a `.reader` with vtable.
- ✅ In stream, push up to limit into the sink Writer (prefer writeVec).
- ✅ Short reads are OK; caller will retry.
- ✅ Optionally fill Reader.buffer yourself and return 0 to enable zero-copy reads.

### Performance Considerations:
- ✅ Minimize allocations in hot paths
- ✅ Prefer `writeVec()` over `writeAll()` for multi-segment data
- ✅ Use appropriate buffer sizes (usually 4KB-64KB)
- ✅ Consider memory alignment for performance-critical applications
- ✅ Profile actual usage patterns rather than optimizing prematurely

### Error Handling:
- ✅ Handle partial writes gracefully in `drain()`
- ✅ Ensure resources are cleaned up on error paths
- ✅ Provide meaningful error messages
- ✅ Consider retry logic for transient failures

## Conclusion

Once these patterns click, the whole std IO stack (tar, http, zstd, file/socket) plugs together cleanly—with your buffer sitting in the middle as either a writer target (builder/reservation with a fixed writer) or a reader source (shelf-slice streamer).

The key insight is that Zig's IO system optimizes for composition and zero-copy operation while maintaining a simple, predictable interface. The visible buffer design eliminates virtual calls in the common case, while the multi-slice pattern enables efficient vectored operations.

This design scales from simple byte counting to complex streaming pipelines, all using the same fundamental abstractions. Whether you're building a network protocol, file format, or custom storage system, these patterns provide a solid foundation for efficient IO operations.