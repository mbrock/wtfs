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
                // TODO: i'm too tired
                _ = self;
                return undefined;
            }

            pub fn end(self: Token) Token {
                _ = self;
                return Token{
                    .location = undefined, // TODO: uhhh...
                    .len = 0,
                };
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

        shelves: [MAX_SHELVES]Shelf = mem.zeroes([MAX_SHELVES]Shelf),
        shelf_count: std.atomic.Value(u8) = .init(0),
        string_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        payload_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
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

        /// Append `bytes` as a new string and return its location.
        pub fn append(
            self: *Self,
            alloc: Allocator,
            bytes: []const u8,
        ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!AppendResult {
            if (bytes.len > MAX_STRING_LEN) return error.Overflow;

            if (bytes.len == 0) {
                _ = try self.reserveRange(alloc, 0);
                _ = self.string_count.fetchAdd(1, .acq_rel);
                const token = Token.init(0, 0, 0);
                return AppendResult{ .token = token, .view = SegmentedView.empty() };
            }

            const start = try self.reserveRange(alloc, bytes.len);
            const start_location = locateIndex(start);

            // TODO: we should be able to do this very easily using a mutable
            // variant of the slice view

            var remaining_len = bytes.len;
            var written: usize = 0;
            var shelf_index = start_location.shelf_index;
            var shelf_offset = start_location.offset;

            while (remaining_len != 0) {
                std.debug.assert(shelf_index < self.shelf_count.load(.acquire));
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
            // TODO: verify with tests
            // i actually don't see a need for explicit bounds checking
            // since debug mode will check slices anyway
            // and we supposedly have a token which is a promise it should be ok
            const y0 = token.shelfIndex();
            const x0 = token.byteOffset();
            const y1 = token.end().shelfIndex();
            const x1 = token.end().byteOffset();
            const head = self.shelves[y0][x0..];
            const tail = self.shelves[y1][0..x1];
            const body = self.shelves[y0 + 1 .. y1];

            return SegmentedView{
                .head = head,
                .body = body,
                .tail = tail,
            };
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
            // TODO: make efficient and correct using "math" and/or "logic"
            var remaining = index;
            var shelf_index: usize = 0;
            inline for (0..MAX_SHELVES) |y| {
                if (remaining < shelfCapacity(y)) {
                    return Location{
                        .shelf_index = shelf_index,
                        .offset = remaining,
                    };
                }
                remaining -= shelfCapacity(y);
                shelf_index += 1;
            }
            return .{
                .shelf_index = shelf_index,
                .offset = remaining,
            };
        }

        fn toConstShelfSlices(slice: []const Shelf) []const []const u8 {
            const ptr = @as([*]const []const u8, @ptrCast(slice.ptr));
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
    var cursor: usize = 0;

    try std.testing.expect(cursor + view.head.len <= expected.len);
    try std.testing.expectEqualStrings(expected[cursor .. cursor + view.head.len], view.head);
    cursor += view.head.len;

    for (view.bodySlices()) |segment| {
        try std.testing.expect(cursor + segment.len <= expected.len);
        try std.testing.expectEqualStrings(expected[cursor .. cursor + segment.len], segment);
        cursor += segment.len;
    }

    try std.testing.expect(cursor + view.tail.len <= expected.len);
    try std.testing.expectEqualStrings(expected[cursor .. cursor + view.tail.len], view.tail);
    cursor += view.tail.len;

    try std.testing.expectEqual(expected.len, cursor);
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
