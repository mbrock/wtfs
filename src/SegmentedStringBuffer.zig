const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const test_threads = @import("test_threading.zig");

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
/// Every appended string is NUL-terminated and stays within a single shelf so
/// callers can form stable `[:0]const u8` views without additional allocation or
/// copying. `config` selects both the growth policy and packed token layout.
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
        const LenType = OffsetType;
        const MAX_SHELF_CAPACITY: usize = (@as(usize, 1) << OFFSET_BITS);
        const MAX_OFFSET: usize = MAX_SHELF_CAPACITY - 1;
        const MAX_STRING_LEN: usize = MAX_OFFSET;
        const TokenBits = SHELF_BITS + OFFSET_BITS + OFFSET_BITS;
        comptime {
            if (TokenBits > 127)
                @compileError("Token bit width exceeds 127 bits");
        }
        const TokenStorage = std.meta.Int(.unsigned, TokenBits);

        comptime {
            if (PREALLOC != 0 and PREALLOC > MAX_SHELF_CAPACITY) {
                @compileError("PREALLOC exceeds maximum shelf capacity for Token encoding");
            }
        }

        const Shelf = struct {
            addr: ?[*]u8 = null,
        };

        /// Identifies the location of a stored string within the buffer.
        /// The string spans `len` bytes starting at `offset` within `shelf`.
        pub const Token = packed struct(TokenStorage) {
            shelf: ShelfType,
            offset: OffsetType,
            len: LenType,

            pub fn shelfIndex(self: Token) usize {
                return @intCast(self.shelf);
            }

            pub fn byteOffset(self: Token) usize {
                return @intCast(self.offset);
            }

            pub fn length(self: Token) usize {
                return @intCast(self.len);
            }

            fn init(shelf_index: usize, offset: usize, len: usize) Token {
                std.debug.assert(shelf_index < MAX_SHELVES);
                std.debug.assert(offset <= MAX_OFFSET);
                std.debug.assert(len <= MAX_STRING_LEN);

                return Token{
                    .shelf = @intCast(shelf_index),
                    .offset = @intCast(offset),
                    .len = @intCast(len),
                };
            }
        };

        /// Result returned from `append`, exposing both the storage token and the
        /// freshly written `[:0]const u8` view.
        pub const AppendResult = struct {
            token: Token,
            view: [:0]const u8,
        };

        const Claim = struct {
            base: [*]u8,
            shelf_index: usize,
            offset: usize,
        };

        /// Aggregate debugging stats across the currently allocated shelves.
        pub const DebugStats = struct {
            shelf_count: usize,
            string_count: usize,
            payload_bytes: usize,
            occupied_bytes: usize,
            capacity_bytes: usize,
        };

        shelves: [MAX_SHELVES]Shelf = mem.zeroes([MAX_SHELVES]Shelf),
        shelf_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        string_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        payload_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        capacity_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        last_remaining: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        grow_mutex: std.Thread.Mutex = .{},

        pub const empty: Self = .{};

        /// Release all shelves back to the allocator. Afterwards `self` becomes undefined.
        pub fn deinit(self: *Self, alloc: Allocator) void {
            // Acquire pairs with the final release during growth so we see all
            // shelves that were published before teardown.
            const shelf_count = self.shelf_count.load(.acquire);
            for (0..shelf_count) |s| {
                const entry = self.shelves[s];
                const base = entry.addr orelse continue;
                const capacity = shelfCapacity(s) orelse continue;
                alloc.free(base[0..capacity]);
                self.shelves[s].addr = null;
            }
            self.* = undefined;
        }

        fn claimLastShelf(self: *Self, need: usize) ?Claim {
            while (true) {
                const count_before = self.shelf_count.load(.acquire);
                if (count_before == 0) return null;

                const shelf_index = count_before - 1;
                const entry_ptr = &self.shelves[shelf_index];
                const base_opt = entry_ptr.addr;
                if (base_opt == null) {
                    std.atomic.spinLoopHint();
                    continue;
                }

                const cap = shelfCapacity(shelf_index) orelse return null;
                const remaining_before = self.last_remaining.load(.acquire);
                if (remaining_before < need) return null;

                const new_remaining = remaining_before - need;
                if (self.last_remaining.cmpxchgWeak(remaining_before, new_remaining, .acq_rel, .acquire) != null) {
                    std.atomic.spinLoopHint();
                    continue;
                }

                const count_after = self.shelf_count.load(.acquire);
                if (count_after != count_before) {
                    _ = self.last_remaining.fetchAdd(need, .acq_rel);
                    continue;
                }

                if (remaining_before > cap) {
                    _ = self.last_remaining.fetchAdd(need, .acq_rel);
                    continue;
                }

                const offset = cap - remaining_before;
                std.debug.assert(offset + need <= cap);
                return Claim{
                    .base = base_opt.?,
                    .shelf_index = shelf_index,
                    .offset = offset,
                };
            }
        }

        /// Append `bytes` as a new, NUL-terminated string and return its location.
        pub fn append(
            self: *Self,
            alloc: Allocator,
            bytes: []const u8,
        ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!AppendResult {
            const need_total = try std.math.add(usize, bytes.len, 1);
            if (need_total > MAX_SHELF_CAPACITY) return error.Overflow;

            while (true) {
                try self.ensureShelfFor(alloc, need_total);
                const claim = self.claimLastShelf(need_total) orelse continue;

                const start = claim.base + claim.offset;
                const dest = start[0..bytes.len];
                @memcpy(dest, bytes);
                start[bytes.len] = 0;

                // acq_rel keeps the counters coherent for readers while
                // avoiding unnecessary fences on load-only observers.
                _ = self.string_count.fetchAdd(1, .acq_rel);
                _ = self.payload_bytes.fetchAdd(bytes.len, .acq_rel);

                const token = Token.init(claim.shelf_index, claim.offset, bytes.len);
                const slice_view: [:0]const u8 = start[0..bytes.len :0];
                return AppendResult{ .token = token, .view = slice_view };
            }
        }

        /// Obtain a stable `[:0]const u8` view for a previously returned token.
        pub fn view(self: *const Self, token: Token) [:0]const u8 {
            // Acquire pairs with the release that publishes a new shelf.
            const shelf_index = token.shelfIndex();
            const offset = token.byteOffset();
            const len = token.length();
            std.debug.assert(shelf_index < self.shelf_count.load(.acquire));
            const entry = self.shelves[shelf_index];
            const base_ptr = entry.addr orelse unreachable;
            const cap = shelfCapacity(shelf_index) orelse unreachable;
            std.debug.assert(len <= cap);
            std.debug.assert(offset <= cap);
            std.debug.assert(offset + len <= cap);
            const base: [*]const u8 = base_ptr;
            return base[offset .. offset + len :0];
        }

        pub fn stringCount(self: *const Self) usize {
            return self.string_count.load(.acquire);
        }

        pub fn payloadBytes(self: *const Self) usize {
            return self.payload_bytes.load(.acquire);
        }

        /// Returns the total number of bytes occupied in shelves, including NUL sentinels.
        pub fn occupiedBytes(self: *const Self) usize {
            const payload = self.payload_bytes.load(.acquire);
            const strings = self.string_count.load(.acquire);
            return std.math.add(usize, payload, strings) catch std.math.maxInt(usize);
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

        fn ensureShelfFor(
            self: *Self,
            alloc: Allocator,
            need: usize,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            std.debug.assert(need <= MAX_SHELF_CAPACITY);
            while (true) {
                const count = self.shelf_count.load(.acquire);
                if (count > 0) {
                    _ = shelfCapacity(count - 1) orelse return error.ShelfLimitReached;
                    if (self.shelves[count - 1].addr != null) {
                        const remaining = self.last_remaining.load(.acquire);
                        if (remaining >= need) {
                            return;
                        }
                    }
                }
                try self.growForNeed(alloc, need);
            }
        }

        fn growForNeed(
            self: *Self,
            alloc: Allocator,
            need: usize,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            std.debug.assert(need <= MAX_SHELF_CAPACITY);
            self.grow_mutex.lock();
            defer self.grow_mutex.unlock();

            const count = self.shelf_count.load(.acquire);
            if (count > 0) {
                _ = shelfCapacity(count - 1) orelse return error.ShelfLimitReached;
                if (self.shelves[count - 1].addr != null) {
                    const remaining = self.last_remaining.load(.acquire);
                    if (remaining >= need) {
                        return;
                    }
                }
            }

            try self.growOneShelf(alloc, need);
        }

        fn growOneShelf(
            self: *Self,
            alloc: Allocator,
            need: usize,
        ) (Allocator.Error || error{ShelfLimitReached})!void {
            _ = need; // recorded for debugging; growth order never skips a shelf even if it cannot satisfy this request.
            // Monotonic load is enough: the growth mutex serialises new shelves.
            const index = self.shelf_count.load(.monotonic);
            if (index >= MAX_SHELVES) {
                return error.ShelfLimitReached;
            }

            const capacity = shelfCapacity(index) orelse return error.ShelfLimitReached;

            const buf = try alloc.alloc(u8, capacity);
            errdefer alloc.free(buf);

            const entry_ptr = &self.shelves[index];
            std.debug.assert(entry_ptr.addr == null);
            entry_ptr.addr = buf.ptr;
            // Publish the fresh remaining capacity before the shelf becomes visible.
            self.last_remaining.store(capacity, .release);

            _ = self.capacity_bytes.fetchAdd(capacity, .acq_rel);
            // Release pairs with acquires on appenders so they can observe the new shelf.
            self.shelf_count.store(index + 1, .release);
        }

        pub inline fn shelfCapacity(index: usize) ?usize {
            if (index >= MAX_SHELVES) return null;
            if (PREALLOC == 0) {
                if (index >= @bitSizeOf(usize)) return null;
                const shift: ShiftInt = @intCast(index);
                const max_word: Word = std.math.maxInt(Word);
                const limit = max_word >> shift;
                if (@as(Word, 1) > limit) return null;
                const capacity_word = (@as(Word, 1) << shift);
                if (capacity_word > @as(Word, MAX_SHELF_CAPACITY)) return null;
                return @as(usize, capacity_word);
            } else {
                if (index >= @bitSizeOf(usize)) return null;
                const shift: ShiftInt = @intCast(index);
                const max_word: Word = std.math.maxInt(Word);
                const limit = max_word >> shift;
                if (@as(Word, PREALLOC) > limit) return null;
                const capacity_word = (@as(Word, PREALLOC) << shift);
                if (capacity_word > @as(Word, MAX_SHELF_CAPACITY)) return null;
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
    try std.testing.expectEqualStrings("hello", res1.view);

    const res2 = try buffer.append(allocator, "world");
    try std.testing.expectEqual(@as(usize, 2), buffer.stringCount());
    try std.testing.expectEqual(@as(usize, 10), buffer.payloadBytes());
    try std.testing.expectEqualStrings("world", buffer.view(res2.token));

    const stats = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 2), stats.string_count);
    try std.testing.expectEqual(@as(usize, 10), stats.payload_bytes);
    try std.testing.expect(stats.occupied_bytes >= stats.payload_bytes);
}

const TinyBuffer = SegmentedStringBuffer(.tiny);

test "SegmentedStringBuffer respects shelf boundaries" {
    var buffer = TinyBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    // Warm up the first shelf so the next append has to respect its capacity budget.
    _ = try buffer.append(allocator, "ab");
    const second = try buffer.append(allocator, "cccccccc");
    const second_capacity = TinyBuffer.shelfCapacity(second.token.shelfIndex()) orelse unreachable;
    try std.testing.expect(second.token.length() + second.token.byteOffset() <= second_capacity);
    const view = buffer.view(second.token);
    try std.testing.expectEqual(@as(usize, 8), view.len);
    try std.testing.expectEqualStrings("cccccccc", view);
}

const ThreadedBuffer = SegmentedStringBuffer(.streaming);

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
        try std.testing.expectEqualStrings(expected, got);
    }
}
