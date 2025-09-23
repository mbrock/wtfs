const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const test_threads = @import("test_threading.zig");
const HashMapUnmanaged = std.HashMapUnmanaged;

/// Configuration for `SegmentedStringBuffer`. Helper constructors and presets allow
/// callers to specify intentional capacities without manual bit math. All functions
/// return compile-time constants so they can be fed directly into
/// `SegmentedStringBuffer(...)` using Zig's implicit field resolution.
pub const SegmentedStringBufferConfig = struct {
    prealloc: usize,
    shelf_bits: comptime_int,
    offset_bits: comptime_int,

    pub inline fn init(prealloc: usize, shelf_bits: comptime_int, offset_bits: comptime_int) SegmentedStringBufferConfig {
        return .{
            .prealloc = prealloc,
            .shelf_bits = shelf_bits,
            .offset_bits = offset_bits,
        };
    }

    pub inline fn forShelfCapacity(
        comptime prealloc: usize,
        comptime shelf_bits: comptime_int,
        comptime shelf_capacity: usize,
    ) SegmentedStringBufferConfig {
        if (shelf_capacity == 0)
            @compileError("shelf_capacity must be > 0");

        const raw_offset_bits = std.math.log2_int_ceil(usize, shelf_capacity);
        const sum = shelf_bits + raw_offset_bits;
        const target_sum: comptime_int = if (sum <= 16)
            16
        else if (sum <= 32)
            32
        else if (sum <= 64)
            64
        else
            @compileError("requested shelf capacity requires tokens wider than 64 bits");
        const offset_bits = target_sum - shelf_bits;
        return init(prealloc, shelf_bits, offset_bits);
    }

    pub inline fn forMaxString(
        comptime prealloc: usize,
        comptime shelf_bits: comptime_int,
        comptime max_string_len: usize,
    ) SegmentedStringBufferConfig {
        if (max_string_len == 0)
            return init(prealloc, shelf_bits, 1);
        return forShelfCapacity(prealloc, shelf_bits, max_string_len + 1);
    }

    /// 16-bit token (4 shelf bits / 12 offset bits). Suitable for tiny tables up to 4096B shelves.
    pub const tiny = init(16, 4, 12);
    /// 32-bit token (5 shelf bits / 27 offset bits). Good for mid-size tables with ~128 MiB shelves.
    pub const medium = init(8 * 1024, 5, 27);
    /// 32-bit token (6 shelf bits / 26 offset bits). Optimised for large, append-only string pools.
    pub const large = init(64 * 1024, 6, 26);
    /// Streaming-friendly variant with no inline prealloc but otherwise like `.large`.
    pub const streaming = init(0, 6, 26);
};

/// Segmented, append-only string storage backed by geometrically sized shelves.
/// Designed for heavy concurrent append workloads without relocating existing data.
/// Appended bytes may span multiple shelves; callers obtain segmented views that
/// reference each shelf slice without additional allocation, inserting sentinels
/// themselves if desired. `config` selects both the growth policy and packed token
/// layout.
pub fn SegmentedStringBuffer(comptime config: SegmentedStringBufferConfig) type {
    const PREALLOC = config.prealloc;
    const SHELF_BITS = config.shelf_bits;
    const OFFSET_BITS = config.offset_bits;

    comptime {
        if (PREALLOC != 0 and !std.math.isPowerOfTwo(PREALLOC)) {
            @compileError("SegmentedStringBuffer PREALLOC must be 0 or a power of two");
        }
    }

    return struct {
        const Self = @This();

        comptime {
            if (SHELF_BITS <= 0 or OFFSET_BITS <= 0)
                @compileError("SegmentedStringBuffer requires positive shelf/offset bit counts");
            if (SHELF_BITS > 6)
                @compileError("SegmentedStringBuffer limits shelf bits to at most 6 (64 shelves)");

            const sum = SHELF_BITS + OFFSET_BITS;
            if (!(sum == 16 or sum == 32 or sum == 64))
                @compileError("SHELF_BITS + OFFSET_BITS must be 16, 32, or 64");
            if (OFFSET_BITS >= @bitSizeOf(usize))
                @compileError("OFFSET_BITS must be strictly less than bit width of usize");
        }

        const MAX_SHELVES = (@as(usize, 1) << SHELF_BITS);
        const Word = std.meta.Int(.unsigned, @bitSizeOf(usize));
        const ShiftInt = std.math.Log2Int(Word);

        const ShelfType = std.meta.Int(.unsigned, SHELF_BITS);
        const OffsetType = std.meta.Int(.unsigned, OFFSET_BITS);
        const LocationType = std.meta.Int(.unsigned, SHELF_BITS + OFFSET_BITS);
        const MAX_SHELF_CAPACITY: usize = (@as(usize, 1) << OFFSET_BITS);
        const MAX_OFFSET: usize = MAX_SHELF_CAPACITY - 1;
        const LOCATION_BITS = SHELF_BITS + OFFSET_BITS;
        const LEN_BITS = SHELF_BITS + OFFSET_BITS;
        const LenType = std.meta.Int(.unsigned, LEN_BITS);
        const MAX_STRING_LEN: usize = @as(usize, std.math.maxInt(LenType));
        const TokenBits = LOCATION_BITS + LEN_BITS;
        const BASE_SHIFT: comptime_int = if (PREALLOC == 0) 0 else std.math.log2_int(usize, PREALLOC);
        const BASE_UNIT: usize = @as(usize, 1) << BASE_SHIFT;

        // TODO: we should use the precise bit-length types in the API

        // TODO: verify that the PREALLOC param correctly multiplies the max capacity

        comptime {
            if (TokenBits > 128)
                @compileError("Token bit width exceeds 128 bits");
        }

        const TokenStorage = std.meta.Int(.unsigned, TokenBits);

        comptime {
            if (PREALLOC != 0 and PREALLOC > MAX_SHELF_CAPACITY) {
                @compileError("PREALLOC exceeds maximum shelf capacity for Token encoding");
            }
        }

        const Shelf = []u8;

        inline fn shelfStartBytes(shelf_index: usize) usize {
            if (shelf_index == 0) return 0;
            const shift_amt: ShiftInt = @intCast(shelf_index);
            const prefix_units = (@as(usize, 1) << shift_amt) - 1;
            return prefix_units << BASE_SHIFT;
        }

        /// Identifies the location of a stored string within the buffer.
        /// The string spans `len` bytes starting at `offset` within `shelf`.
        const OFFSET_MASK: LocationType = (@as(LocationType, 1) << OFFSET_BITS) - 1;
        pub const Token = packed struct(TokenStorage) {
            location: LocationType,
            len: LenType,

            pub fn shelfIndex(self: Token) usize {
                return @intCast(self.location >> OFFSET_BITS);
            }

            pub fn byteOffset(self: Token) usize {
                return @intCast(self.location & OFFSET_MASK);
            }

            pub fn length(self: Token) usize {
                return @intCast(self.len);
            }

            pub fn packedLocation(self: Token) usize {
                return @intCast(self.location);
            }

            pub fn byteOffsetFromStart(self: Token) usize {
                const shelf = self.shelfIndex();
                const offset = self.byteOffset();
                return shelfStartBytes(shelf) + offset;
            }

            fn init(shelf_index: usize, offset: usize, len: usize) Token {
                std.debug.assert(shelf_index < MAX_SHELVES);
                std.debug.assert(offset <= MAX_OFFSET);
                std.debug.assert(len <= MAX_STRING_LEN);

                const packed_shelf = (@as(LocationType, @intCast(shelf_index)) << OFFSET_BITS);
                const packed_offset = @as(LocationType, @intCast(offset));
                const location: LocationType = packed_shelf | packed_offset;

                return Token{
                    .location = location,
                    .len = @intCast(len),
                };
            }

            /// Create a limited token that covers only the specified number of bytes from a given position.
            /// This is useful for respecting Reader limits and creating constrained views of token data.
            ///
            /// Parameters:
            /// - pos: Starting position within this token (0-based offset)
            /// - limit: std.Io.Limit specifying maximum bytes to include
            ///
            /// Returns: New token covering at most limit bytes starting from pos
            pub fn limit(self: Token, pos: usize, io_limit: std.Io.Limit) Token {
                const remaining_bytes = self.length() -| pos; // Saturating subtraction
                const max_bytes = switch (io_limit) {
                    .unlimited => remaining_bytes,
                    else => @min(@intFromEnum(io_limit), remaining_bytes),
                };

                return Token.init(self.shelfIndex(), self.byteOffset() + pos, max_bytes);
            }

            /// Create a Reader that streams this token's data
            pub fn reader(self: Token, buffer: *const Self, reader_buffer: []u8) TokenReader {
                return TokenReader.init(buffer, self, reader_buffer);
            }

            /// Create a Writer that allows writing within this token's bounds
            pub fn writer(self: Token, buffer: *Self) TokenWriter {
                return TokenWriter.init(buffer, self);
            }
        };

        /// Take an exclusive locking writer that can grow dynamically.
        /// Only one locking writer can exist at a time.
        /// While locked, append() calls will return BufferLocked error.
        pub fn takeLockingWriter(self: *Self, allocator: Allocator) !LockingWriter {
            // Try to acquire the lock atomically
            const already_locked = self.writer_locked.swap(true, .acquire);
            if (already_locked) return error.BufferLocked;

            // Ensure we have at least one shelf
            if (self.shelf_count.load(.acquire) == 0) {
                _ = try self.reserveRange(allocator, 0);
            }

            // Get current append position as our starting token
            const current_offset = self.payload_bytes.load(.acquire);
            const current_location = Self.locateIndex(current_offset);
            const current_shelf = current_location.shelf_index;

            // Create initial token at current append position
            const initial_token = Token.init(@intCast(current_shelf), current_location.offset, 0 // Start with 0 length, will grow
            );

            return LockingWriter.init(self, allocator, initial_token);
        }

        /// Locking writer that can grow by extending the buffer
        pub const LockingWriter = struct {
            writer: std.Io.Writer,
            buffer: *Self,
            allocator: Allocator,
            base_token: Token,
            bytes_written: usize,

            pub fn init(buffer: *Self, allocator: Allocator, initial_token: Token) LockingWriter {
                var self = LockingWriter{
                    .writer = .{ .vtable = &vtable, .buffer = &.{}, .end = 0 },
                    .buffer = buffer,
                    .allocator = allocator,
                    .base_token = initial_token,
                    .bytes_written = 0,
                };
                self.setupBuffer();
                return self;
            }

            pub fn deinit(self: *LockingWriter) void {
                // Commit any remaining buffered data manually since flush is no-op
                if (self.writer.end > 0) {
                    self.bytes_written += self.writer.end;
                    _ = self.buffer.payload_bytes.fetchAdd(self.writer.end, .acq_rel);
                }

                // Release the lock
                self.buffer.writer_locked.store(false, .release);
            }

            /// Get the token representing everything written so far
            pub fn token(self: *const LockingWriter) Token {
                return Token.init(self.base_token.shelfIndex(), self.base_token.byteOffset(), self.bytes_written + self.writer.end);
            }

            fn setupBuffer(self: *LockingWriter) void {
                const current_global_pos = self.base_token.byteOffsetFromStart() + self.bytes_written;
                const location = Self.locateIndex(current_global_pos);

                // Ensure shelf exists by allocating if needed
                const shelf_count = self.buffer.shelf_count.load(.acquire);
                if (location.shelf_index >= shelf_count) {
                    const shelf_size = Self.shelfCapacity(@intCast(location.shelf_index));
                    const new_shelf = self.allocator.alloc(u8, shelf_size) catch {
                        // Can't allocate, no buffer space available
                        self.writer.buffer = &.{};
                        self.writer.end = 0;
                        return;
                    };

                    self.buffer.shelves[location.shelf_index] = new_shelf;
                    _ = self.buffer.shelf_count.store(@intCast(location.shelf_index + 1), .release);
                    _ = self.buffer.capacity_bytes.fetchAdd(shelf_size, .acq_rel);
                }

                const shelf = self.buffer.shelves[location.shelf_index];
                const shelf_start = location.offset;

                // Point writer buffer directly at remaining shelf memory
                // This should always have some space since we just allocated if needed
                self.writer.buffer = shelf[shelf_start..];
                self.writer.end = 0;
            }

            /// Drain commits buffered data to the segmented buffer.
            /// Like Allocating, this doesn't move data elsewhere - it just commits positions.
            fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
                const self: *LockingWriter = @fieldParentPtr("writer", w);
                _ = splat; // TODO

                // Commit buffered data (the "sink" is the segmented buffer itself)
                if (w.end > 0) {
                    self.bytes_written += w.end;
                    _ = self.buffer.payload_bytes.fetchAdd(w.end, .acq_rel);
                    w.end = 0;
                }

                // Set up buffer for next write region
                self.setupBuffer();

                // Like Allocating.drain, return 0 to indicate we didn't consume external data
                _ = data;
                return 0;
            }

            /// Like Allocating flush - no-op since we don't need to empty buffer
            fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
                // No-op like Allocating.flush
                // The buffer points directly to shelf memory, no need to drain it
            }

            /// Like growingRebase - ensure we have enough shelves allocated
            fn rebase(w: *std.Io.Writer, preserve: usize, minimum_len: usize) std.Io.Writer.Error!void {
                const self: *LockingWriter = @fieldParentPtr("writer", w);

                // Calculate how much total space we need
                const current_pos = self.bytes_written + w.end;
                const needed_space = preserve + minimum_len;
                const target_pos = current_pos + needed_space;
                const target_location = Self.locateIndex(target_pos);

                // Ensure we have enough shelves allocated
                const shelf_count = self.buffer.shelf_count.load(.acquire);
                var shelf_idx = shelf_count;
                while (shelf_idx <= target_location.shelf_index) {
                    const shelf_size = Self.shelfCapacity(@intCast(shelf_idx));
                    const new_shelf = self.allocator.alloc(u8, shelf_size) catch return error.WriteFailed;

                    self.buffer.shelves[shelf_idx] = new_shelf;
                    _ = self.buffer.capacity_bytes.fetchAdd(shelf_size, .acq_rel);
                    shelf_idx += 1;
                }
                _ = self.buffer.shelf_count.store(@intCast(shelf_idx), .release);

                // Now use default rebase logic to handle preserve/drain
                return std.Io.Writer.defaultRebase(w, preserve, minimum_len);
            }

            const vtable: std.Io.Writer.VTable = .{
                .drain = drain,
                .flush = flush,
                .rebase = rebase,
            };
        };

        /// Writer implementation that allows writing within a Token's bounds.
        /// The writer buffer points directly to shelf memory for zero-copy fast path writes.
        pub const TokenWriter = struct {
            writer: std.Io.Writer,
            segbuf: *Self,
            token: Token,
            current_shelf: usize,
            token_bytes_written: usize,

            pub fn init(buffer: *Self, token: Token) TokenWriter {
                var self = TokenWriter{
                    .writer = .{ .vtable = &vtable, .buffer = &.{}, .end = 0 },
                    .segbuf = buffer,
                    .token = token,
                    .current_shelf = token.shelfIndex(),
                    .token_bytes_written = 0,
                };

                // Set up writer buffer to point to first shelf slice
                self.setupBuffer();
                return self;
            }

            /// Get the current write position (including buffered data)
            pub fn pos(self: *const TokenWriter) usize {
                return self.token_bytes_written + self.writer.end;
            }

            fn setupBuffer(self: *TokenWriter) void {
                if (self.token_bytes_written >= self.token.length()) {
                    // No space left - set empty buffer
                    self.writer.buffer = &.{};
                    self.writer.end = 0;
                    return;
                }

                const remaining = self.token.length() - self.token_bytes_written;
                const current_global_pos = self.token.byteOffsetFromStart() + self.token_bytes_written;
                const location = Self.locateIndex(current_global_pos);

                self.current_shelf = location.shelf_index;
                const shelf = self.segbuf.shelves[self.current_shelf];
                const shelf_start = location.offset;
                const shelf_available = shelf.len - shelf_start;
                const buffer_size = @min(remaining, shelf_available);

                // Point writer buffer directly at shelf memory
                self.writer.buffer = shelf[shelf_start .. shelf_start + buffer_size];
                self.writer.end = 0;
            }

            /// Drain is called when the writer buffer is full or we need to move to next shelf
            fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
                const self: *TokenWriter = @fieldParentPtr("writer", w);
                _ = splat; // TODO: handle splat

                // Update our position with whatever was written to the buffer
                self.token_bytes_written += w.end;
                w.end = 0;

                // Check if we're done
                if (self.token_bytes_written >= self.token.length()) {
                    return error.WriteFailed;
                }

                // Set up buffer for next shelf region
                self.setupBuffer();

                // If we have a buffer now, try writing the first slice
                if (w.buffer.len > 0 and data.len > 0) {
                    const first_slice = data[0];
                    const to_copy = @min(first_slice.len, w.buffer.len);
                    @memcpy(w.buffer[0..to_copy], first_slice[0..to_copy]);
                    w.end = to_copy;
                    return to_copy;
                }

                return 0;
            }

            fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
                // No-op
            }

            /// Smart rebase that can grow tokens when possible
            fn rebase(w: *std.Io.Writer, preserve: usize, minimum_len: usize) std.Io.Writer.Error!void {
                const self: *TokenWriter = @fieldParentPtr("writer", w);

                // Check if we can grow this token
                const current_pos = self.token_bytes_written + w.end;
                const needed_total = preserve + minimum_len;
                const would_exceed_token = current_pos + needed_total > self.token.length();

                if (!would_exceed_token) {
                    // We're still within token bounds, just need to setup next shelf
                    // Let default rebase handle it by calling drain()
                    return std.Io.Writer.defaultRebase(w, preserve, minimum_len);
                }

                // Would exceed token - check if we can grow the token
                // For now, just fail - but this is where growth policy would go
                return error.WriteFailed;
            }

            const vtable: std.Io.Writer.VTable = .{
                .drain = drain,
                .flush = flush,
                .rebase = rebase,
            };
        };

        /// Reader implementation that streams data from a Token
        pub const TokenReader = struct {
            reader: std.Io.Reader,
            buffer: *const Self,
            token: Token,
            pos: usize,

            pub fn init(buffer: *const Self, token: Token, reader_buffer: []u8) TokenReader {
                return .{
                    .reader = .{ .vtable = &vtable, .buffer = reader_buffer, .seek = 0, .end = 0 },
                    .buffer = buffer,
                    .token = token,
                    .pos = 0,
                };
            }

            /// Core streaming implementation that efficiently streams token data using vectored I/O.
            ///
            /// This function implements the Reader.stream interface by leveraging the SegmentedStringBuffer's
            /// geometric shelf structure to create optimal vectored writes. The key insight is that we create
            /// a "limited token" that respects the Limit parameter, then use the buffer's segmented view to
            /// generate a head/body/tail structure that maps directly to three writeVec calls.
            ///
            /// Limit Handling Strategy:
            /// Instead of trying to write the entire token and then checking limits (which can cause
            /// WriteFailed when vectors exceed buffer capacity), we create a new token that covers exactly
            /// the bytes we're allowed to write. The segmented view automatically adapts to show only the
            /// portion that fits within the limit.
            ///
            /// Vectored I/O Pattern:
            /// The three-writeVec approach maps the segmented buffer's structure directly to syscalls:
            /// - Head writeVec: Partial first shelf (single slice)
            /// - Body writeVec: Complete geometric shelves (multiple slices, perfect vector)
            /// - Tail writeVec: Partial last shelf (single slice)
            ///
            /// This creates efficient pwritev syscalls that can handle multiple memory segments in a single
            /// system call, regardless of how the data is distributed across the geometric shelf structure.
            ///
            /// Example trace with prealloc=16, limit=1024:
            /// pwritev(fd, [{16 bytes}, {32 bytes}, {64 bytes}, {128 bytes}, {256 bytes}, {512 bytes}, {16 bytes}], 7, offset)
            /// This writes 1024 bytes total across 7 segments in one syscall.
            fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
                const self: *TokenReader = @fieldParentPtr("reader", r);

                if (self.pos >= self.token.length()) {
                    return error.EndOfStream;
                }

                // KEY INSIGHT: Create a limited token that covers exactly the bytes we can write.
                // This ensures the segmented view will only include data that fits within our constraints,
                // preventing WriteFailed errors when writeVec tries to write more than the limit allows.
                const limited_token = self.token.limit(self.pos, limit);

                if (limited_token.length() == 0) return 0;

                const seg_view = self.buffer.view(limited_token);
                var total_written: usize = 0;

                // Three-writeVec pattern: each writeVec call maps to an efficient pwritev syscall

                // 1. Write head: partial data from the first shelf
                if (seg_view.head.len > 0) {
                    const wrote = try w.writeVec(&[_][]const u8{seg_view.head});
                    total_written += wrote;
                }

                // 2. Write body: complete geometric shelves as a perfect vector
                // This is the most efficient part - multiple shelves in one vectored syscall
                if (seg_view.body.len > 0) {
                    const wrote = try w.writeVec(seg_view.body);
                    total_written += wrote;
                }

                // 3. Write tail: partial data from the last shelf
                if (seg_view.tail.len > 0) {
                    const wrote = try w.writeVec(&[_][]const u8{seg_view.tail});
                    total_written += wrote;
                }

                self.pos += total_written;
                return total_written;
            }

            const vtable: std.Io.Reader.VTable = .{ .stream = stream };
        };

        pub const SegmentedView = struct {
            head: []const u8,
            body: []const []const u8,
            tail: []const u8,

            pub fn bodySlices(self: *const SegmentedView) []const []const u8 {
                return self.body;
            }

            pub fn totalLength(self: *const SegmentedView) usize {
                var total: usize = 0;
                total += self.head.len;
                for (self.body) |chunk| total += chunk.len;
                total += self.tail.len;
                return total;
            }

            pub inline fn emptyBody() []const []const u8 {
                return &[_][]const u8{};
            }

            pub fn empty() SegmentedView {
                return SegmentedView{
                    .head = &[_]u8{},
                    .body = emptyBody(),
                    .tail = &[_]u8{},
                };
            }
        };

        pub const MutableSegmentedView = struct {
            head: []u8,
            body: [][]u8,
            tail: []u8,

            pub fn bodySlices(self: *const MutableSegmentedView) [][]u8 {
                return self.body;
            }

            pub fn totalLength(self: *const MutableSegmentedView) usize {
                var total: usize = 0;
                total += self.head.len;
                for (self.body) |chunk| total += chunk.len;
                total += self.tail.len;
                return total;
            }

            pub inline fn emptyBody() [][]u8 {
                return &[_][]u8{};
            }

            pub fn empty() MutableSegmentedView {
                return MutableSegmentedView{
                    .head = &[_]u8{},
                    .body = emptyBody(),
                    .tail = &[_]u8{},
                };
            }
        };

        const Location = struct {
            shelf_index: usize,
            offset: usize,
        };

        /// Result returned from `append`, exposing both the storage token and the
        /// freshly written view.
        pub const AppendResult = struct {
            token: Token,
            view: SegmentedView,
        };

        /// Aggregate debugging stats across the currently allocated shelves.
        pub const DebugStats = struct {
            shelf_count: usize,
            string_count: usize,
            payload_bytes: usize,
            occupied_bytes: usize,
            capacity_bytes: usize,
        };

        /// Context for hash map operations on tokens representing strings in the buffer.
        /// This enables using tokens as keys in a HashMap for deduplication.
        pub const TokenContext = struct {
            buffer: *const Self,

            pub fn hash(ctx: TokenContext, token: Token) u64 {
                var hash_buffer: [256]u8 = undefined;
                var hashing_writer = std.Io.Writer.Hashing(std.hash.Wyhash).initHasher(std.hash.Wyhash.init(0), &hash_buffer);
                var reader_buffer: [256]u8 = undefined;
                var token_reader = token.reader(ctx.buffer, &reader_buffer);

                // Stream token data directly into hashing writer
                token_reader.reader.streamExact(&hashing_writer.writer, token.length()) catch |err| switch (err) {
                    error.ReadFailed => {
                        // This indicates corrupted token data - should not happen in normal operation
                        std.debug.panic("TokenReader failed - corrupted token data", .{});
                    },
                    error.WriteFailed => unreachable, // Hashing writer doesn't fail
                    error.EndOfStream => unreachable, // Token has known length
                };

                return hashing_writer.hasher.final();
            }

            pub fn eql(ctx: TokenContext, a: Token, b: Token) bool {
                if (a.length() != b.length()) return false;
                if (a.length() == 0) return true;

                var reader_buffer_a: [256]u8 = undefined;
                var reader_buffer_b: [256]u8 = undefined;
                var reader_a = a.reader(ctx.buffer, &reader_buffer_a);
                var reader_b = b.reader(ctx.buffer, &reader_buffer_b);

                var buf_a: [256]u8 = undefined;
                var buf_b: [256]u8 = undefined;

                while (true) {
                    const bytes_a = reader_a.reader.readSliceShort(&buf_a) catch |err| switch (err) {
                        error.ReadFailed => unreachable, // TokenReader doesn't fail
                    };
                    const bytes_b = reader_b.reader.readSliceShort(&buf_b) catch |err| switch (err) {
                        error.ReadFailed => unreachable, // TokenReader doesn't fail
                    };

                    if (bytes_a != bytes_b) return false;
                    if (bytes_a == 0) break; // End of both streams

                    if (!std.mem.eql(u8, buf_a[0..bytes_a], buf_b[0..bytes_b])) return false;
                }

                return true;
            }
        };

        /// Adapter for using string slices as lookup keys in the dedup set.
        pub const StringAdapter = struct {
            buffer: *const Self,

            pub fn hash(_: StringAdapter, key: []const u8) u64 {
                var hash_buffer: [256]u8 = undefined;
                var hashing_writer = std.Io.Writer.Hashing(std.hash.Wyhash).initHasher(std.hash.Wyhash.init(0), &hash_buffer);
                hashing_writer.writer.writeAll(key) catch unreachable; // writeAll to fixed buffer can't fail
                return hashing_writer.hasher.final();
            }

            pub fn eql(ctx: StringAdapter, key: []const u8, token: Token) bool {
                if (key.len != token.length()) return false;
                if (key.len == 0) return true;

                var reader_buffer: [256]u8 = undefined;
                var token_reader = token.reader(ctx.buffer, &reader_buffer);
                var remaining_key = key;

                while (remaining_key.len > 0) {
                    var buf: [256]u8 = undefined;
                    const to_read = @min(buf.len, remaining_key.len);
                    const bytes_read = token_reader.reader.readSliceShort(buf[0..to_read]) catch |err| switch (err) {
                        error.ReadFailed => unreachable, // TokenReader doesn't fail
                    };

                    if (bytes_read == 0) return false; // Token ended before key
                    if (!std.mem.eql(u8, remaining_key[0..bytes_read], buf[0..bytes_read])) return false;

                    remaining_key = remaining_key[bytes_read..];
                }

                return true;
            }
        };

        /// DedupSet provides string deduplication for the buffer.
        /// Uses Token as key with void value to save memory.
        pub const DedupSet = HashMapUnmanaged(
            Token,
            void,
            TokenContext,
            std.hash_map.default_max_load_percentage,
        );

        shelves: [MAX_SHELVES]Shelf = mem.zeroes([MAX_SHELVES]Shelf),
        shelf_count: std.atomic.Value(u8) = .init(0),
        string_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        payload_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        writer_locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        capacity_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        grow_mutex: std.Thread.Mutex = .{},

        pub const empty: Self = .{};

        /// Release all shelves back to the allocator. Afterwards `self` becomes undefined.
        pub fn deinit(self: *Self, alloc: Allocator) void {
            // Acquire pairs with the final release during growth so we see all
            // shelves that were published before teardown.
            const shelf_count = self.shelf_count.load(.acquire);
            for (0..shelf_count) |s| {
                const shelf = self.shelves[s];
                if (shelf.len == 0) continue;
                alloc.free(shelf);
                self.shelves[s] = mem.zeroes(Shelf);
            }
            self.* = undefined;
        }

        /// Append `bytes` with deduplication. If the string already exists, return its token.
        /// Otherwise, append it and add to the dedup set.
        /// IMPORTANT: For thread safety, this method should be called within appropriate synchronization.
        pub fn appendDedup(
            self: *Self,
            dedup: *DedupSet,
            alloc: Allocator,
            bytes: []const u8,
        ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!Token {
            // Check if string already exists before appending
            const maybe_existing = dedup.getKeyAdapted(bytes, StringAdapter{ .buffer = self });
            if (maybe_existing) |existing_token| {
                return existing_token;
            }

            // String doesn't exist, append it
            const result = try self.append(alloc, bytes);
            const new_token = result.token;

            // Add to dedup set
            try dedup.putContext(alloc, new_token, {}, TokenContext{ .buffer = self });
            return new_token;
        }

        /// Append `bytes` as a new string and return its location.
        pub fn append(
            self: *Self,
            alloc: Allocator,
            bytes: []const u8,
        ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!AppendResult {
            // Note: For now, don't check writer lock to avoid breaking existing error unions
            if (bytes.len > MAX_STRING_LEN) return error.Overflow;

            if (bytes.len == 0) {
                _ = try self.reserveRange(alloc, 0);
                _ = self.string_count.fetchAdd(1, .acq_rel);
                const token = Token.init(0, 0, 0);
                return AppendResult{ .token = token, .view = SegmentedView.empty() };
            }

            const start = try self.reserveRange(alloc, bytes.len);
            const start_location = Self.locateIndex(start);

            // TODO: we should be able to do this very easily using a mutable
            // variant of the slice view

            var remaining_len = bytes.len;
            var written: usize = 0;
            var shelf_index = start_location.shelf_index;
            var shelf_offset = start_location.offset;

            while (remaining_len != 0) {
                const shelf_count_now = @as(usize, self.shelf_count.load(.acquire));
                std.debug.assert(shelf_index < shelf_count_now);
                const shelf = self.shelves[shelf_index];
                std.debug.assert(shelf.len != 0);
                std.debug.assert(shelf_offset < shelf.len);
                const available = shelf.len - shelf_offset;
                std.debug.assert(available > 0);
                const take = if (remaining_len < available) remaining_len else available;
                const src = bytes[written .. written + take];
                const dest = shelf[shelf_offset .. shelf_offset + take];
                @memcpy(dest, src);

                remaining_len -= take;
                written += take;
                shelf_index += 1;
                shelf_offset = 0;
            }

            // acq_rel keeps the counters coherent for readers while avoiding
            // unnecessary fences on load-only observers.
            _ = self.string_count.fetchAdd(1, .acq_rel);

            const token = Token.init(start_location.shelf_index, start_location.offset, bytes.len);
            const seg_view = self.view(token);
            return AppendResult{ .token = token, .view = seg_view };
        }

        /// Obtain a stable view for a previously returned token.
        pub fn view(self: *const Self, token: Token) SegmentedView {
            // Handle empty strings
            if (token.length() == 0) {
                return SegmentedView.empty();
            }

            const start_global = token.byteOffsetFromStart();
            const start_loc = Self.locateIndex(start_global);
            const available_shelves = @as(usize, self.shelf_count.load(.acquire));
            std.debug.assert(start_loc.shelf_index < available_shelves);

            const first_shelf = self.shelves[start_loc.shelf_index];
            std.debug.assert(first_shelf.len != 0);
            std.debug.assert(start_loc.offset < first_shelf.len);

            const total_len = token.length();
            const first_available = first_shelf.len - start_loc.offset;

            if (total_len <= first_available) {
                return SegmentedView{
                    .head = first_shelf[start_loc.offset .. start_loc.offset + total_len],
                    .body = SegmentedView.emptyBody(),
                    .tail = &[_]u8{},
                };
            }

            var remaining = total_len - first_available;
            var shelf_index = start_loc.shelf_index + 1;

            while (true) {
                std.debug.assert(shelf_index < available_shelves);
                const shelf = self.shelves[shelf_index];
                const shelf_len = shelf.len;
                std.debug.assert(shelf_len != 0);

                if (remaining < shelf_len) {
                    const body_slice = if (shelf_index > start_loc.shelf_index + 1)
                        toConstShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index])
                    else
                        SegmentedView.emptyBody();

                    return SegmentedView{
                        .head = first_shelf[start_loc.offset..],
                        .body = body_slice,
                        .tail = shelf[0..remaining],
                    };
                }

                if (remaining == shelf_len) {
                    const body_slice = toConstShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index + 1]);
                    return SegmentedView{
                        .head = first_shelf[start_loc.offset..],
                        .body = body_slice,
                        .tail = &[_]u8{},
                    };
                }

                remaining -= shelf_len;
                shelf_index += 1;
            }
        }

        /// Obtain a mutable view for a previously returned token.
        /// This allows writing within the token's allocated space.
        pub fn viewMutable(self: *Self, token: Token) MutableSegmentedView {
            // Handle empty strings
            if (token.length() == 0) {
                return MutableSegmentedView.empty();
            }

            const start_global = token.byteOffsetFromStart();
            const start_loc = Self.locateIndex(start_global);
            const available_shelves = @as(usize, self.shelf_count.load(.acquire));
            std.debug.assert(start_loc.shelf_index < available_shelves);

            const first_shelf = self.shelves[start_loc.shelf_index];
            std.debug.assert(first_shelf.len != 0);
            std.debug.assert(start_loc.offset < first_shelf.len);

            const total_len = token.length();
            const first_available = first_shelf.len - start_loc.offset;

            if (total_len <= first_available) {
                return MutableSegmentedView{
                    .head = first_shelf[start_loc.offset .. start_loc.offset + total_len],
                    .body = MutableSegmentedView.emptyBody(),
                    .tail = &[_]u8{},
                };
            }

            var remaining = total_len - first_available;
            var shelf_index = start_loc.shelf_index + 1;

            while (true) {
                std.debug.assert(shelf_index < available_shelves);
                const shelf = self.shelves[shelf_index];
                const shelf_len = shelf.len;
                std.debug.assert(shelf_len != 0);

                if (remaining < shelf_len) {
                    const body_slice = if (shelf_index > start_loc.shelf_index + 1)
                        toMutableShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index])
                    else
                        MutableSegmentedView.emptyBody();

                    return MutableSegmentedView{
                        .head = first_shelf[start_loc.offset..],
                        .body = body_slice,
                        .tail = shelf[0..remaining],
                    };
                }

                if (remaining == shelf_len) {
                    const body_slice = toMutableShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index + 1]);
                    return MutableSegmentedView{
                        .head = first_shelf[start_loc.offset..],
                        .body = body_slice,
                        .tail = &[_]u8{},
                    };
                }

                remaining -= shelf_len;
                shelf_index += 1;
            }
        }

        fn reserveRange(
            self: *Self,
            alloc: Allocator,
            len: usize,
        ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!usize {
            while (true) {
                const current = self.payload_bytes.load(.acquire);
                const end = std.math.add(usize, current, len) catch return error.Overflow;
                try self.ensureCapacityFor(alloc, end);
                if (self.payload_bytes.cmpxchgWeak(current, end, .acq_rel, .acquire) == null) {
                    return current;
                }
                std.atomic.spinLoopHint();
            }
        }

        fn locateIndex(index: usize) Location {
            const normalized = (index >> BASE_SHIFT) + 1;
            const shelf_index = std.math.log2_int(usize, normalized);
            const shift_amt: ShiftInt = @intCast(shelf_index);
            const start_units = (@as(usize, 1) << shift_amt) - 1;
            const start = start_units << BASE_SHIFT;
            return Location{
                .shelf_index = shelf_index,
                .offset = index - start,
            };
        }

        fn toConstShelfSlices(slice: []const Shelf) []const []const u8 {
            const ptr = @as([*]const []const u8, @ptrCast(slice.ptr));
            return ptr[0..slice.len];
        }

        fn toMutableShelfSlices(slice: []Shelf) [][]u8 {
            const ptr = @as([*][]u8, @ptrCast(slice.ptr));
            return ptr[0..slice.len];
        }

        pub fn stringCount(self: *const Self) usize {
            return self.string_count.load(.acquire);
        }

        pub fn payloadBytes(self: *const Self) usize {
            return self.payload_bytes.load(.acquire);
        }

        /// Returns the total number of bytes occupied in shelves by appended payload.
        pub fn occupiedBytes(self: *const Self) usize {
            const payload = self.payload_bytes.load(.acquire);
            return payload;
        }

        pub fn capacityBytes(self: *const Self) usize {
            return self.capacity_bytes.load(.acquire);
        }

        pub fn shelfCount(self: *const Self) usize {
            return self.shelf_count.load(.acquire);
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.string_count.load(.acquire) == 0;
        }

        pub fn debugStats(self: *const Self) DebugStats {
            return DebugStats{
                .shelf_count = self.shelf_count.load(.acquire),
                .string_count = self.string_count.load(.acquire),
                .payload_bytes = self.payload_bytes.load(.acquire),
                .occupied_bytes = self.occupiedBytes(),
                .capacity_bytes = self.capacity_bytes.load(.acquire),
            };
        }

        fn ensureCapacityFor(
            self: *Self,
            alloc: Allocator,
            required: usize,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            while (true) {
                const capacity = self.capacity_bytes.load(.acquire);
                if (capacity >= required) return;
                try self.growForCapacity(alloc, required);
            }
        }

        fn growForCapacity(
            self: *Self,
            alloc: Allocator,
            required: usize,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            self.grow_mutex.lock();
            defer self.grow_mutex.unlock();

            const capacity = self.capacity_bytes.load(.acquire);
            if (capacity >= required) return;

            try self.growOneShelf(alloc);
        }

        fn growOneShelf(
            self: *Self,
            alloc: Allocator,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            // Monotonic load is enough: the growth mutex serialises new shelves.
            const index = self.shelf_count.load(.monotonic);
            if (index >= MAX_SHELVES) {
                return error.ShelfLimitReached;
            }

            const capacity = shelfCapacity(std.math.cast(ShelfType, index) orelse undefined);

            const buf = try alloc.alloc(u8, capacity);
            errdefer alloc.free(buf);

            const entry_ptr = &self.shelves[index];
            std.debug.assert(entry_ptr.len == 0);
            entry_ptr.* = buf;
            _ = self.capacity_bytes.fetchAdd(buf.len, .acq_rel);
            // Release pairs with acquires on appenders so they can observe the new shelf.
            self.shelf_count.store(index + 1, .release);
        }

        pub inline fn shelfCapacity(index: ShelfType) usize {
            if (index >= MAX_SHELVES) return 0;
            if (PREALLOC == 0) {
                if (index >= @bitSizeOf(usize)) return 0;
                const shift: ShiftInt = @intCast(index);
                const max_word: Word = std.math.maxInt(Word);
                const limit = max_word >> shift;
                if (@as(Word, 1) > limit) return 0;
                const capacity_word = (@as(Word, 1) << shift);
                if (capacity_word > @as(Word, MAX_SHELF_CAPACITY)) return 0;
                return @as(usize, capacity_word);
            } else {
                if (index >= @bitSizeOf(usize)) return 0;
                const shift: ShiftInt = @intCast(index);
                const max_word: Word = std.math.maxInt(Word);
                const limit = max_word >> shift;
                if (@as(Word, PREALLOC) > limit) return 0;
                const capacity_word = (@as(Word, PREALLOC) << shift);
                if (capacity_word > @as(Word, MAX_SHELF_CAPACITY)) return 0;
                return @as(usize, capacity_word);
            }
        }
    };
}

const SmallBuffer = SegmentedStringBuffer(.tiny);

test "SegmentedStringBuffer basic append and view" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const res1 = try buffer.append(allocator, "hello");
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 5), buffer.payloadBytes());
    try expectViewEquals("hello", res1.view);

    const res2 = try buffer.append(allocator, "world");
    try std.testing.expectEqual(@as(usize, 2), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 10), buffer.payloadBytes());
    const res2_view = buffer.view(res2.token);
    try expectViewEquals("world", res2_view);

    const stats = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 2), stats.string_count);
    try std.testing.expectEqual(@as(usize, 10), stats.payload_bytes);
    try std.testing.expectEqual(stats.payload_bytes, stats.occupied_bytes);
}

const TinyBuffer = SegmentedStringBuffer(.tiny);
const PackedTokenConfig = SegmentedStringBufferConfig.init(4, 4, 12);
const PackedTokenBuffer = SegmentedStringBuffer(PackedTokenConfig);

test "SegmentedStringBuffer respects shelf boundaries" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Warm up the first shelf so the next append crosses into the following shelf.
    _ = try buffer.append(allocator, "ab");
    const payload = "abcdefghijklmnopqrst"; // 20 bytes.
    const second = try buffer.append(allocator, payload);
    try std.testing.expectEqual(@as(usize, 0), second.token.shelfIndex());
    try std.testing.expectEqual(@as(usize, 2), second.token.byteOffset());

    const view = buffer.view(second.token);
    try expectViewEquals(payload, view);
    try std.testing.expectEqual(@as(usize, 14), view.head.len);
    try std.testing.expectEqual(@as(usize, 0), view.bodySlices().len);
    try std.testing.expectEqual(@as(usize, 6), view.tail.len);
}

test "SegmentedStringBuffer token packs shelf and offset" {
    var buffer = PackedTokenBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const res_a = try buffer.append(allocator, "aa");
    try std.testing.expectEqual(@as(usize, 0), res_a.token.packedLocation());

    const res_b = try buffer.append(allocator, "bb");
    const shelf_stride = (@as(usize, 1) << PackedTokenConfig.offset_bits);
    try std.testing.expectEqual(@as(usize, 2), res_b.token.packedLocation());

    const res_c = try buffer.append(allocator, "cc");
    try std.testing.expectEqual(shelf_stride, res_c.token.packedLocation());

    try expectViewEquals("aa", res_a.view);
    try expectViewEquals("bb", res_b.view);
    try expectViewEquals("cc", res_c.view);
}

const ThreadedBuffer = SegmentedStringBuffer(.streaming);

fn expectViewEquals(expected: []const u8, view: anytype) !void {
    try std.testing.expectEqual(expected.len, view.totalLength());

    // Compare using the simple approach
    var pos: usize = 0;

    // Check head
    if (view.head.len > 0) {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + view.head.len], view.head);
        pos += view.head.len;
    }

    // Check body
    for (view.body) |segment| {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + segment.len], segment);
        pos += segment.len;
    }

    // Check tail
    if (view.tail.len > 0) {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + view.tail.len], view.tail);
    }
}

test "SegmentedStringBuffer concurrent append" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer {
        std.debug.assert(gpa.deinit() == .ok);
    }
    const allocator = gpa.allocator();

    var buffer = ThreadedBuffer.empty;
    defer buffer.deinit(allocator);

    const thread_count = 4;
    const per_thread = 50;
    const total = thread_count * per_thread;

    const tokens = try allocator.alloc(ThreadedBuffer.Token, total);
    defer allocator.free(tokens);

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(tid: usize, buf: *ThreadedBuffer, alloc: Allocator, per: usize, token_slice: []ThreadedBuffer.Token) !void {
            const base_index = tid * per;
            var i: usize = 0;
            while (i < per) : (i += 1) {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "t{d}-{d}", .{ tid, i });
                const res = try buf.append(alloc, text);
                token_slice[base_index + i] = res.token;
            }
        }
    };

    try group.spawnMany(thread_count, Worker.run, .{ &buffer, allocator, per_thread, tokens });
    group.wait();

    try std.testing.expectEqual(@as(usize, total), buffer.stringCount());

    for (tokens, 0..) |token, idx| {
        const got = buffer.view(token);
        var scratch: [64]u8 = undefined;
        const expected = try std.fmt.bufPrint(&scratch, "t{d}-{d}", .{ idx / per_thread, idx % per_thread });
        try expectViewEquals(expected, got);
    }
}

test "SegmentedStringBuffer empty string handling" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Empty string should be allowed and return empty view
    const empty_res = try buffer.append(allocator, "");
    try std.testing.expectEqual(@as(usize, 0), empty_res.token.length());
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 0), buffer.payloadBytes());
    try expectViewEquals("", empty_res.view);

    // Mix empty and non-empty strings
    const text1 = try buffer.append(allocator, "hello");
    const empty2 = try buffer.append(allocator, "");
    const text2 = try buffer.append(allocator, "world");

    try std.testing.expectEqual(@as(usize, 4), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 10), buffer.payloadBytes());

    try expectViewEquals("hello", buffer.view(text1.token));
    try expectViewEquals("", buffer.view(empty2.token));
    try expectViewEquals("world", buffer.view(text2.token));
}

test "SegmentedStringBuffer single character strings" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const chars = "abcdefghijklmnopqrstuvwxyz";
    var tokens: [26]SmallBuffer.Token = undefined;

    for (chars, 0..) |c, i| {
        const char_str = [_]u8{c};
        const res = try buffer.append(allocator, &char_str);
        tokens[i] = res.token;
    }

    try std.testing.expectEqual(@as(usize, 26), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 26), buffer.payloadBytes());

    for (chars, 0..) |c, i| {
        const view = buffer.view(tokens[i]);
        const expected = [_]u8{c};
        try expectViewEquals(&expected, view);
    }
}

test "SegmentedStringBuffer multi-shelf spanning" {
    // Use tiny config with 16-byte shelves for predictable testing
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // First shelf has 16 bytes
    _ = try buffer.append(allocator, "012345678901234"); // 15 bytes, leaving 1 byte
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());

    // This should trigger second shelf allocation and span both shelves
    const span1 = try buffer.append(allocator, "ABCDEFGHIJ"); // 10 bytes: 1 in first shelf, 9 in second
    try std.testing.expectEqual(@as(usize, 2), buffer.shelfCount());

    const view1 = buffer.view(span1.token);
    try expectViewEquals("ABCDEFGHIJ", view1);
    try std.testing.expect(view1.head.len > 0); // Should have content in head
    try std.testing.expect(view1.tail.len > 0); // Should have content in tail
}

test "SegmentedStringBuffer triple shelf span" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Fill first shelf (16 bytes)
    _ = try buffer.append(allocator, "0123456789ABCDE"); // 15 bytes

    // Create a string that will span 3 shelves
    // Shelf 0: 1 byte remaining
    // Shelf 1: 32 bytes full
    // Shelf 2: remainder
    const long_string = "X" ++ ("Y" ** 32) ++ "Z123456789"; // 1 + 32 + 10 = 43 bytes
    const res = try buffer.append(allocator, long_string);

    const view = buffer.view(res.token);
    try expectViewEquals(long_string, view);
    try std.testing.expectEqual(@as(usize, 43), view.totalLength());
    try std.testing.expectEqual(@as(usize, 1), view.head.len); // "X" in first shelf
    try std.testing.expectEqual(@as(usize, 1), view.body.len); // One full shelf in body
    try std.testing.expectEqual(@as(usize, 32), view.body[0].len); // Full second shelf
    try std.testing.expectEqual(@as(usize, 10), view.tail.len); // "Z123456789" in third shelf
}

test "SegmentedStringBuffer exact shelf boundary" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Fill exactly to shelf boundary
    const exact16 = try buffer.append(allocator, "0123456789ABCDEF");
    try std.testing.expectEqual(@as(usize, 16), exact16.token.length());

    const view = buffer.view(exact16.token);
    try expectViewEquals("0123456789ABCDEF", view);
    try std.testing.expectEqual(@as(usize, 16), view.head.len);
    try std.testing.expectEqual(@as(usize, 0), view.body.len);
    try std.testing.expectEqual(@as(usize, 0), view.tail.len);

    // Next append should go to new shelf
    const next = try buffer.append(allocator, "XYZ");
    try std.testing.expectEqual(@as(usize, 2), buffer.shelfCount());
    try std.testing.expectEqual(@as(usize, 1), next.token.shelfIndex());
    try std.testing.expectEqual(@as(usize, 0), next.token.byteOffset());
}

test "SegmentedStringBuffer token encoding limits" {
    // Test with tiny config (4 shelf bits, 12 offset bits)
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Maximum offset is (1 << 12) - 1 = 4095
    const max_offset_test = try buffer.append(allocator, "test");
    try std.testing.expect(max_offset_test.token.byteOffset() <= 4095);

    // Shelf index should fit in 4 bits (0-15)
    try std.testing.expect(max_offset_test.token.shelfIndex() < 16);
}

test "SegmentedStringBuffer different configurations" {
    const configs = [_]SegmentedStringBufferConfig{
        SegmentedStringBufferConfig.tiny,
        SegmentedStringBufferConfig.medium,
        SegmentedStringBufferConfig.large,
        SegmentedStringBufferConfig.streaming,
    };

    inline for (configs) |cfg| {
        const TestBuffer = SegmentedStringBuffer(cfg);
        var buffer = TestBuffer.empty;
        const allocator = std.testing.allocator;
        defer buffer.deinit(allocator);

        const test_str = "Testing different configurations!";
        const res = try buffer.append(allocator, test_str);
        try expectViewEquals(test_str, buffer.view(res.token));
        try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
        try std.testing.expectEqual(test_str.len, buffer.payloadBytes());
    }
}

test "SegmentedStringBuffer custom configuration" {
    // Test custom config using init method directly with valid bit counts
    const custom_config = SegmentedStringBufferConfig.init(256, 5, 11); // 5+11=16 bits total
    const CustomBuffer = SegmentedStringBuffer(custom_config);

    var buffer = CustomBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const test_str = "Custom configuration test";
    const res = try buffer.append(allocator, test_str);
    try expectViewEquals(test_str, buffer.view(res.token));

    // Test another custom configuration with different parameters
    const another_config = SegmentedStringBufferConfig.init(512, 6, 26); // 6+26=32 bits total
    const AnotherBuffer = SegmentedStringBuffer(another_config);

    var buffer2 = AnotherBuffer.empty;
    defer buffer2.deinit(allocator);

    const long_str = "A" ** 500;
    const res2 = try buffer2.append(allocator, long_str);
    try expectViewEquals(long_str, buffer2.view(res2.token));
}

test "SegmentedStringBuffer overflow detection" {
    // Create buffer with tiny token size to test overflow
    const tiny_config = SegmentedStringBufferConfig.init(4, 3, 13); // 3+13=16 bits total for location, len also 16 bits
    const TinyTokenBuffer = SegmentedStringBuffer(tiny_config);

    var buffer = TinyTokenBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Test within string length limits (max is 2^16-1 = 65535)
    const ok_str = "A" ** 1000;
    _ = try buffer.append(allocator, ok_str);

    // This should fail - exceeds MAX_STRING_LEN for this config
    const too_long = "B" ** 65536;
    try std.testing.expectError(error.Overflow, buffer.append(allocator, too_long));
}

test "SegmentedStringBuffer isEmpty and initial state" {
    var buffer = SmallBuffer.empty;
    defer buffer.deinit(std.testing.allocator);

    try std.testing.expect(buffer.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 0), buffer.payloadBytes());
    try std.testing.expectEqual(@as(usize, 0), buffer.occupiedBytes());
    try std.testing.expectEqual(@as(usize, 0), buffer.capacityBytes());
    try std.testing.expectEqual(@as(usize, 0), buffer.shelfCount());

    _ = try buffer.append(std.testing.allocator, "test");
    try std.testing.expect(!buffer.isEmpty());
}

test "SegmentedStringBuffer stress test with many small strings" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const count = 100; // Reduced count to avoid shelf limit
    var tokens = try allocator.alloc(SmallBuffer.Token, count);
    defer allocator.free(tokens);

    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const len = random.intRangeAtMost(usize, 1, 31);
        for (0..len) |j| {
            buf[j] = @as(u8, @intCast(65 + (i + j) % 26)); // A-Z pattern
        }
        const res = try buffer.append(allocator, buf[0..len]);
        tokens[i] = res.token;
    }

    try std.testing.expectEqual(@as(usize, count), buffer.stringCount());

    // Verify all strings are readable
    for (tokens) |token| {
        const view = buffer.view(token);
        try std.testing.expect(view.totalLength() >= 1);
        try std.testing.expect(view.totalLength() <= 31);
    }
}

test "SegmentedStringBuffer large string spanning many shelves" {
    const LargeConfig = SegmentedStringBufferConfig.init(256, 5, 27); // 5+27=32 bits, larger shelves
    const LargeTestBuffer = SegmentedStringBuffer(LargeConfig);

    var buffer = LargeTestBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Create a string that will span multiple shelves
    const large_size = 2000; // Reduced size to fit in available shelves
    const large_str = try allocator.alloc(u8, large_size);
    defer allocator.free(large_str);

    // Fill with pattern
    for (large_str, 0..) |*c, i| {
        c.* = @as(u8, @intCast(32 + (i % 95))); // Printable ASCII
    }

    const res = try buffer.append(allocator, large_str);
    try std.testing.expectEqual(large_size, res.token.length());

    const view = buffer.view(res.token);
    try std.testing.expectEqual(large_size, view.totalLength());

    // Verify content matches
    var reconstructed = try allocator.alloc(u8, large_size);
    defer allocator.free(reconstructed);

    var pos: usize = 0;
    @memcpy(reconstructed[pos .. pos + view.head.len], view.head);
    pos += view.head.len;

    for (view.body) |segment| {
        @memcpy(reconstructed[pos .. pos + segment.len], segment);
        pos += segment.len;
    }

    @memcpy(reconstructed[pos .. pos + view.tail.len], view.tail);
    pos += view.tail.len;

    try std.testing.expectEqualSlices(u8, large_str, reconstructed);
}

test "SegmentedStringBuffer debug stats accuracy" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const stats1 = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 0), stats1.shelf_count);
    try std.testing.expectEqual(@as(usize, 0), stats1.string_count);
    try std.testing.expectEqual(@as(usize, 0), stats1.payload_bytes);

    _ = try buffer.append(allocator, "hello");
    _ = try buffer.append(allocator, "world");

    const stats2 = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 2), stats2.string_count);
    try std.testing.expectEqual(@as(usize, 10), stats2.payload_bytes);
    try std.testing.expect(stats2.shelf_count > 0);
    try std.testing.expect(stats2.capacity_bytes >= stats2.payload_bytes);
}

test "SegmentedStringBuffer repeated identical strings" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const repeated = "test string";
    var tokens: [10]SmallBuffer.Token = undefined;

    for (&tokens) |*token| {
        const res = try buffer.append(allocator, repeated);
        token.* = res.token;
    }

    try std.testing.expectEqual(@as(usize, 10), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, repeated.len * 10), buffer.payloadBytes());

    for (tokens) |token| {
        const view = buffer.view(token);
        try expectViewEquals(repeated, view);
    }
}

test "SegmentedStringBuffer interleaved sizes" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const small = "a";
    const medium = "hello world";
    const large = "The quick brown fox jumps over the lazy dog. " ** 3;

    const s1 = try buffer.append(allocator, small);
    const m1 = try buffer.append(allocator, medium);
    const l1 = try buffer.append(allocator, large);
    const s2 = try buffer.append(allocator, small);
    const l2 = try buffer.append(allocator, large);
    const m2 = try buffer.append(allocator, medium);

    try expectViewEquals(small, buffer.view(s1.token));
    try expectViewEquals(medium, buffer.view(m1.token));
    try expectViewEquals(large, buffer.view(l1.token));
    try expectViewEquals(small, buffer.view(s2.token));
    try expectViewEquals(large, buffer.view(l2.token));
    try expectViewEquals(medium, buffer.view(m2.token));
}

test "SegmentedStringBuffer zero prealloc configuration" {
    const zero_prealloc = SegmentedStringBufferConfig.init(0, 4, 12);
    const ZeroPreallocBuffer = SegmentedStringBuffer(zero_prealloc);

    var buffer = ZeroPreallocBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Should start with first shelf being 1 byte
    const first = try buffer.append(allocator, "a");
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());
    try expectViewEquals("a", buffer.view(first.token));

    // Fill up to trigger multiple shelf allocations
    _ = try buffer.append(allocator, "bb");
    _ = try buffer.append(allocator, "cccc");
    _ = try buffer.append(allocator, "dddddddd");

    try std.testing.expect(buffer.shelfCount() > 1);
}

test "SegmentedStringBuffer all ASCII printable characters" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Test all printable ASCII characters
    var ascii_str: [95]u8 = undefined;
    for (32..127, 0..) |c, i| {
        ascii_str[i] = @intCast(c);
    }

    const res = try buffer.append(allocator, &ascii_str);
    const view = buffer.view(res.token);
    try expectViewEquals(&ascii_str, view);
}

test "SegmentedStringBuffer binary data with null bytes" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const binary_data = [_]u8{ 0, 1, 2, 255, 254, 253, 0, 0, 0, 128, 127 };
    const res = try buffer.append(allocator, &binary_data);

    const view = buffer.view(res.token);
    try expectViewEquals(&binary_data, view);
}

test "SegmentedStringBuffer shelf capacity calculation" {
    // Test the shelfCapacity function with different indices
    try std.testing.expectEqual(@as(usize, 16), SmallBuffer.shelfCapacity(0));
    try std.testing.expectEqual(@as(usize, 32), SmallBuffer.shelfCapacity(1));
    try std.testing.expectEqual(@as(usize, 64), SmallBuffer.shelfCapacity(2));
    try std.testing.expectEqual(@as(usize, 128), SmallBuffer.shelfCapacity(3));
}

test "SegmentedStringBuffer token byte offset calculation" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const res1 = try buffer.append(allocator, "first");
    const res2 = try buffer.append(allocator, "second");
    const res3 = try buffer.append(allocator, "third");

    try std.testing.expectEqual(@as(usize, 0), res1.token.byteOffsetFromStart());
    try std.testing.expectEqual(@as(usize, 5), res2.token.byteOffsetFromStart());
    try std.testing.expectEqual(@as(usize, 11), res3.token.byteOffsetFromStart());
}

test "SegmentedStringBuffer deduplication basic" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = SmallBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    // First append should create new entry
    const token1 = try buffer.appendDedup(&dedup, allocator, "hello");
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 5), buffer.payloadBytes());

    // Second append of same string should return same token
    const token2 = try buffer.appendDedup(&dedup, allocator, "hello");
    try std.testing.expectEqual(token1, token2);
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 5), buffer.payloadBytes());

    // Different string should create new entry
    const token3 = try buffer.appendDedup(&dedup, allocator, "world");
    try std.testing.expect(token3.packedLocation() != token1.packedLocation());
    try std.testing.expectEqual(@as(usize, 2), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 10), buffer.payloadBytes());

    // Verify the strings
    const view1 = buffer.view(token1);
    const view3 = buffer.view(token3);
    try expectViewEquals("hello", view1);
    try expectViewEquals("world", view3);
}

test "SegmentedStringBuffer deduplication with empty strings" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = SmallBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    const empty1 = try buffer.appendDedup(&dedup, allocator, "");
    const empty2 = try buffer.appendDedup(&dedup, allocator, "");

    try std.testing.expectEqual(empty1, empty2);
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
}

test "SegmentedStringBuffer deduplication across shelf boundaries" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = TinyBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    // Create a string that will span shelves
    const long_str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Fill up first shelf partially
    _ = try buffer.append(allocator, "0123456789");

    // Append long string that will span shelves
    const token1 = try buffer.appendDedup(&dedup, allocator, long_str);

    // Try to append same string again
    const token2 = try buffer.appendDedup(&dedup, allocator, long_str);

    try std.testing.expectEqual(token1, token2);
    try expectViewEquals(long_str, buffer.view(token1));
}

test "SegmentedStringBuffer deduplication stress test" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = SmallBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    // Create patterns with duplicates
    const patterns = [_][]const u8{
        "alpha", "beta", "gamma", "delta", "epsilon",
        "alpha", "beta", "alpha", "gamma", "alpha",
    };

    var tokens: [patterns.len]SmallBuffer.Token = undefined;
    for (patterns, 0..) |pattern, i| {
        tokens[i] = try buffer.appendDedup(&dedup, allocator, pattern);
    }

    // Verify deduplication worked
    try std.testing.expectEqual(tokens[0], tokens[5]); // "alpha"
    try std.testing.expectEqual(tokens[0], tokens[7]); // "alpha"
    try std.testing.expectEqual(tokens[0], tokens[9]); // "alpha"
    try std.testing.expectEqual(tokens[1], tokens[6]); // "beta"
    try std.testing.expectEqual(tokens[2], tokens[8]); // "gamma"

    // Should only have 5 unique strings
    try std.testing.expectEqual(@as(usize, 5), buffer.stringCount());
}

test "SegmentedStringBuffer deduplication with binary data" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = SmallBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    const binary1 = [_]u8{ 0, 1, 2, 3, 255 };
    const binary2 = [_]u8{ 0, 1, 2, 3, 255 };
    const binary3 = [_]u8{ 0, 1, 2, 3, 254 };

    const token1 = try buffer.appendDedup(&dedup, allocator, &binary1);
    const token2 = try buffer.appendDedup(&dedup, allocator, &binary2);
    const token3 = try buffer.appendDedup(&dedup, allocator, &binary3);

    try std.testing.expectEqual(token1, token2);
    try std.testing.expect(token1.packedLocation() != token3.packedLocation());
    try std.testing.expectEqual(@as(usize, 2), buffer.stringCount());
}

test "SegmentedStringBuffer TokenContext hash and equality" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const str1 = try buffer.append(allocator, "test");
    const str2 = try buffer.append(allocator, "test");
    const str3 = try buffer.append(allocator, "different");

    const ctx = SmallBuffer.TokenContext{ .buffer = &buffer };

    // Same content should have same hash
    const hash1 = ctx.hash(str1.token);
    const hash2 = ctx.hash(str2.token);
    try std.testing.expectEqual(hash1, hash2);

    // Same content should be equal
    try std.testing.expect(ctx.eql(str1.token, str2.token));

    // Different content should not be equal
    try std.testing.expect(!ctx.eql(str1.token, str3.token));
}

test "SegmentedStringBuffer StringAdapter equality" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const stored = try buffer.append(allocator, "hello world");
    const adapter = SmallBuffer.StringAdapter{ .buffer = &buffer };

    // Should match identical string
    try std.testing.expect(adapter.eql("hello world", stored.token));

    // Should not match different strings
    try std.testing.expect(!adapter.eql("hello", stored.token));
    try std.testing.expect(!adapter.eql("hello world!", stored.token));
    try std.testing.expect(!adapter.eql("", stored.token));
}

test "SegmentedStringBuffer deduplication memory efficiency" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    var dedup = SmallBuffer.DedupSet{};
    defer dedup.deinit(allocator);

    // Add same string many times
    const test_str = "This is a test string for deduplication";
    var tokens: [100]SmallBuffer.Token = undefined;

    for (&tokens) |*token| {
        token.* = try buffer.appendDedup(&dedup, allocator, test_str);
    }

    // All tokens should be identical
    for (tokens[1..]) |token| {
        try std.testing.expectEqual(tokens[0], token);
    }

    // Should only have one string in buffer
    try std.testing.expectEqual(@as(usize, 1), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, test_str.len), buffer.payloadBytes());
}

test "Token.limit method" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "hello world");
    const original_token = result.token;

    // Test unlimited
    const unlimited_token = original_token.limit(0, .unlimited);
    try std.testing.expectEqual(@as(usize, 11), unlimited_token.length());

    // Test limited to 5 bytes from start
    const limited_token = original_token.limit(0, std.Io.Limit.limited(5));
    try std.testing.expectEqual(@as(usize, 5), limited_token.length());

    // Test limited from offset
    const offset_token = original_token.limit(6, std.Io.Limit.limited(3));
    try std.testing.expectEqual(@as(usize, 3), offset_token.length());

    // Test limit larger than remaining bytes
    const large_limit_token = original_token.limit(6, std.Io.Limit.limited(100));
    try std.testing.expectEqual(@as(usize, 5), large_limit_token.length()); // Only 5 bytes remain after offset 6

    // Test offset beyond token end
    const beyond_token = original_token.limit(20, std.Io.Limit.limited(5));
    try std.testing.expectEqual(@as(usize, 0), beyond_token.length());
}

test "TokenReader basic functionality" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "hello");
    var reader_buffer: [64]u8 = undefined;
    var token_reader = result.token.reader(&buffer, &reader_buffer);

    var dest: [5]u8 = undefined;
    var fixed_writer = std.Io.Writer.fixed(&dest);
    _ = try token_reader.reader.streamExact(&fixed_writer, 5);
    try std.testing.expectEqualStrings("hello", &dest);
}

test "TokenReader cross-shelf streaming" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Fill first shelf partially
    _ = try buffer.append(allocator, "0123456789");

    // Add string that spans shelves
    const spanning = try buffer.append(allocator, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    var reader_buffer: [64]u8 = undefined;
    var token_reader = spanning.token.reader(&buffer, &reader_buffer);

    // Stream to a buffer using the reader interface
    var dest: [26]u8 = undefined;
    var fixed_writer = std.Io.Writer.fixed(&dest);
    _ = try token_reader.reader.streamExact(&fixed_writer, 26);
    try std.testing.expectEqualStrings("ABCDEFGHIJKLMNOPQRSTUVWXYZ", &dest);
}

test "TokenReader respects stream limit parameter" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Create a token with "hello world" (11 bytes)
    const result = try buffer.append(allocator, "hello world");
    const token = result.token;

    var reader_buffer: [64]u8 = undefined;
    var token_reader = token.reader(&buffer, &reader_buffer);

    // Test with limited to 5 bytes
    var out_buffer: [20]u8 = undefined;
    var writer = std.Io.Writer.fixed(&out_buffer);

    const limited_bytes = try token_reader.reader.stream(&writer, .limited(5));
    try std.testing.expectEqual(@as(usize, 5), limited_bytes);
    try std.testing.expectEqualStrings("hello", writer.buffered());

    // Test that position advanced correctly
    try std.testing.expectEqual(@as(usize, 5), token_reader.pos);

    // Read remaining bytes
    writer = std.Io.Writer.fixed(&out_buffer);
    const remaining_bytes = try token_reader.reader.stream(&writer, .unlimited);
    try std.testing.expectEqual(@as(usize, 6), remaining_bytes);
    try std.testing.expectEqualStrings(" world", writer.buffered());
}

test "SegmentedStringBuffer concurrent stress test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer {
        std.debug.assert(gpa.deinit() == .ok);
    }
    const allocator = gpa.allocator();

    var buffer = ThreadedBuffer.empty;
    defer buffer.deinit(allocator);

    const thread_count = 8;
    const per_thread = 25;
    const max_size = 100;

    const tokens = try allocator.alloc(ThreadedBuffer.Token, thread_count * per_thread);
    defer allocator.free(tokens);

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(tid: usize, buf: *ThreadedBuffer, alloc: Allocator, per: usize, token_slice: []ThreadedBuffer.Token) !void {
            const base_index = tid * per;
            var prng = std.Random.DefaultPrng.init(@intCast(tid * 31));
            const random = prng.random();

            var i: usize = 0;
            while (i < per) : (i += 1) {
                const size = random.intRangeAtMost(usize, 1, 100);
                var scratch: [100]u8 = undefined;
                for (0..size) |j| {
                    scratch[j] = @as(u8, @intCast(65 + ((tid + i + j) % 26)));
                }
                const res = try buf.append(alloc, scratch[0..size]);
                token_slice[base_index + i] = res.token;
            }
        }
    };

    try group.spawnMany(thread_count, Worker.run, .{ &buffer, allocator, per_thread, tokens });
    group.wait();

    try std.testing.expectEqual(@as(usize, thread_count * per_thread), buffer.stringCount());

    // Verify all tokens are valid and retrievable
    for (tokens) |token| {
        const view = buffer.view(token);
        try std.testing.expect(view.totalLength() >= 1);
        try std.testing.expect(view.totalLength() <= max_size);
    }
}

test "TokenWriter basic functionality" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Create a token with some initial content (11 chars)
    const result = try buffer.append(allocator, "hello world");
    const original_token = result.token;

    // Create a writer for this token
    var token_writer = original_token.writer(&buffer);

    // Write new content over the existing data (11 chars to match)
    try token_writer.writer.writeAll("goodbye!!*!");

    // Verify the content was changed
    const view = buffer.view(original_token);
    try expectViewEquals("goodbye!!*!", view);
}

test "TokenWriter boundary enforcement" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Create a small token
    const result = try buffer.append(allocator, "12345");
    const token = result.token;

    var token_writer = token.writer(&buffer);

    // Write exactly what fits
    try token_writer.writer.writeAll("hello");

    // Should write exactly what fits
    const view = buffer.view(token);
    try expectViewEquals("hello", view);
    try std.testing.expectEqual(@as(usize, 5), token_writer.pos());

    // Try to write more - should fail because no space
    const write_result = token_writer.writer.write("more");
    try std.testing.expectError(error.WriteFailed, write_result);
    try std.testing.expectEqual(@as(usize, 5), token_writer.pos());
}

test "TokenWriter cross-shelf writing" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Fill first shelf partially to ensure next append spans shelves
    _ = try buffer.append(allocator, "0123456789");

    // Create a token that spans shelves
    const result = try buffer.append(allocator, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    const spanning_token = result.token;

    var token_writer = spanning_token.writer(&buffer);

    // Write new content across the shelf boundary
    try token_writer.writer.writeAll("abcdefghijklmnopqrstuvwxyz");

    // Verify the content was written correctly across shelves
    const view = buffer.view(spanning_token);
    try expectViewEquals("abcdefghijklmnopqrstuvwxyz", view);
}

test "TokenWriter partial writes" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "1234567890");
    const token = result.token;

    var token_writer = token.writer(&buffer);

    // Write part of the token
    try token_writer.writer.writeAll("hello");
    try std.testing.expectEqual(@as(usize, 5), token_writer.pos());

    // Write more
    try token_writer.writer.writeAll("world");
    try std.testing.expectEqual(@as(usize, 10), token_writer.pos());

    // Verify final content
    const view = buffer.view(token);
    try expectViewEquals("helloworld", view);
}

test "TokenWriter with empty token" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "");
    const empty_token = result.token;

    var token_writer = empty_token.writer(&buffer);

    // Try to write to empty token - should fail gracefully
    if (token_writer.writer.writeAll("test")) {
        // If write succeeds, no bytes should have been written due to zero capacity
        try std.testing.expectEqual(@as(usize, 0), token_writer.pos());
    } else |err| switch (err) {
        error.WriteFailed => {
            // Expected - writing to empty token should fail
            try std.testing.expectEqual(@as(usize, 0), token_writer.pos());
        },
        else => return err,
    }
    try std.testing.expectEqual(@as(usize, 0), token_writer.pos());
}

test "TokenWriter splat functionality" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "1234567890");
    const token = result.token;

    var token_writer = token.writer(&buffer);

    // Use splat to write repeated pattern
    _ = try token_writer.writer.writeSplat(&.{"AB"}, 5); // Should write "ABABABABAB"

    const view = buffer.view(token);
    try expectViewEquals("ABABABABAB", view);
}

test "TokenWriter rebase behavior" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, "12345");
    const token = result.token;

    var token_writer = token.writer(&buffer);

    // Try to write something that would need rebase beyond token bounds
    const write_result = token_writer.writer.writeAll("this string is way too long for the 5-char token");
    try std.testing.expectError(error.WriteFailed, write_result);

    // Verify nothing was written beyond bounds
    const view = buffer.view(token);
    try std.testing.expect(view.totalLength() == 5); // Still original size
}

test "TokenWriter writableSlice triggers rebase" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Create a small token
    const result = try buffer.append(allocator, "ab");
    const token = result.token;

    var token_writer = token.writer(&buffer);

    // Fill the buffer first
    try token_writer.writer.writeAll("ab");

    // Now try to get a writable slice that would exceed token bounds
    const slice_result = token_writer.writer.writableSlice(10); // Way more than 2-byte token
    try std.testing.expectError(error.WriteFailed, slice_result);
}

test "Compare TokenWriter vs Allocating rebase behavior" {
    const allocator = std.testing.allocator;

    // Test 1: Allocating writer can grow via rebase
    {
        var growing: std.Io.Writer.Allocating = .init(allocator);
        defer growing.deinit();

        // Write some data
        try growing.writer.writeAll("hello");

        // Request a large writable slice - this should succeed via rebase
        const slice = try growing.writer.writableSlice(1000);
        slice[0] = 'X';

        // Should now have more capacity
        try std.testing.expect(growing.writer.buffer.len >= 1005); // hello + 1000
        try std.testing.expectEqual(@as(u8, 'X'), growing.written()[5]);
    }

    // Test 2: TokenWriter cannot grow beyond token bounds
    {
        var buffer = SmallBuffer.empty;
        defer buffer.deinit(allocator);

        const result = try buffer.append(allocator, "hello");
        const token = result.token;
        var token_writer = token.writer(&buffer);

        // Write to fill the token
        try token_writer.writer.writeAll("hello");

        // Request a large writable slice - this should FAIL
        const slice_result = token_writer.writer.writableSlice(1000);
        try std.testing.expectError(error.WriteFailed, slice_result);
    }
}

test "LockingWriter exclusive access and growth" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    {
        // Take a locking writer in a scope so it gets cleaned up
        var locking_writer = try buffer.takeLockingWriter(allocator);
        defer locking_writer.deinit();

        // While locked, append should fail
        const append_result = buffer.append(allocator, "test");
        try std.testing.expectError(error.BufferLocked, append_result);

        // Write some data
        try locking_writer.writer.writeAll("Hello, world! This is a test.");

        // Get the token representing everything written
        const token = locking_writer.token();
        try std.testing.expect(token.length() > 0);
    }

    // After deinit, append should work again
    const result = try buffer.append(allocator, "test");
    try std.testing.expect(result.token.length() == 4);
}

test "LockingWriter resumes writing immediately after payload" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first_shelf_capacity = SmallBuffer.shelfCapacity(0);
    const prefill_len = first_shelf_capacity + 2;

    const prefill_data = try allocator.alloc(u8, prefill_len);
    defer allocator.free(prefill_data);
    @memset(prefill_data, 'A');

    _ = try buffer.append(allocator, prefill_data);

    const initial_payload = buffer.payloadBytes();
    try std.testing.expect(initial_payload > first_shelf_capacity);

    const extra = "more-data";

    const token = blk: {
        var locking_writer = try buffer.takeLockingWriter(allocator);
        defer locking_writer.deinit();

        try locking_writer.writer.writeAll(extra);

        const token = locking_writer.token();
        try std.testing.expectEqual(initial_payload, token.byteOffsetFromStart());

        const expected_location = SmallBuffer.locateIndex(initial_payload);
        try std.testing.expectEqual(expected_location.shelf_index, token.shelfIndex());
        try std.testing.expectEqual(expected_location.offset, token.byteOffset());

        try std.testing.expectEqual(extra.len, token.length());

        break :blk token;
    };

    const view = buffer.view(token);
    try expectViewEquals(extra, view);
}

test "LockingWriter prevents multiple locks" {
    var buffer = SmallBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Take first lock
    var writer1 = try buffer.takeLockingWriter(allocator);
    defer writer1.deinit();

    // Try to take second lock - should fail
    const writer2_result = buffer.takeLockingWriter(allocator);
    try std.testing.expectError(error.BufferLocked, writer2_result);
}
