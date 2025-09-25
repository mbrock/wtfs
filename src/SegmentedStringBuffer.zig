const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const CapacityLatch = @import("CapacityLatch.zig");
const test_threads = @import("test_threading.zig");
const segments = @import("segments.zig");

pub const SegmentedStringBuffer = struct {
    const Self = @This();

    const base_shift: comptime_int = 10; // 1024 bytes
    const base_unit: usize = @as(usize, 1) << base_shift;
    const max_supported_shelves: usize = 32;
    const shelf_bit_count: comptime_int = 6;
    const offset_bit_count: comptime_int = @bitSizeOf(usize) - shelf_bit_count;
    const OffsetInt = std.meta.Int(.unsigned, offset_bit_count);

    comptime {
        std.debug.assert(offset_bit_count > 0);
        std.debug.assert(max_supported_shelves <= (1 << shelf_bit_count));
    }

    pub const first_shelf_bytes = base_unit;

    pub const DebugStats = struct {
        shelf_count: usize,
        length: usize,
        occupied_bytes: usize,
        capacity_bytes: usize,
    };

    pub const SegmentedView = struct {
        head: []const u8,
        body: []const []const u8,
        tail: []const u8,

        pub fn totalLength(self: *const SegmentedView) usize {
            var total: usize = self.head.len + self.tail.len;
            for (self.body) |chunk| total += chunk.len;
            return total;
        }

        pub fn bodySlices(self: *const SegmentedView) []const []const u8 {
            return self.body;
        }

        pub fn emptyBody() []const []const u8 {
            return &[_][]const u8{};
        }

        pub fn empty() SegmentedView {
            return .{
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

        pub fn totalLength(self: *const MutableSegmentedView) usize {
            var total: usize = self.head.len + self.tail.len;
            for (self.body) |chunk| total += chunk.len;
            return total;
        }

        pub fn bodySlices(self: *const MutableSegmentedView) [][]u8 {
            return self.body;
        }

        pub fn emptyBody() [][]u8 {
            return &[_][]u8{};
        }

        pub fn empty() MutableSegmentedView {
            return .{
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

    pub const Index = packed union {
        word: usize,
        parts: packed struct {
            shelf: u6,
            offset: OffsetInt,
        },

        pub fn init(shelf_index: usize, offset: usize) Index {
            std.debug.assert(shelf_index < (1 << shelf_bit_count));
            std.debug.assert(offset <= shelfCapacity(shelf_index));
            return .{
                .parts = .{
                    .shelf = @intCast(shelf_index),
                    .offset = @intCast(offset),
                },
            };
        }

        pub fn shelfIndex(self: Index) usize {
            return @as(usize, self.parts.shelf);
        }

        pub fn byteOffset(self: Index) usize {
            return @as(usize, self.parts.offset);
        }

        pub fn byteOffsetFromStart(self: Index) usize {
            return shelfStartBytes(self.shelfIndex()) + self.byteOffset();
        }

        pub fn raw(self: Index) usize {
            return self.word;
        }

        pub fn fromRaw(raw_word: usize) Index {
            return .{ .word = raw_word };
        }
    };

    pub const Slice = struct {
        index: Index,
        len: usize,

        pub fn startIndex(self: Slice) Index {
            return self.index;
        }

        pub fn shelfIndex(self: Slice) usize {
            return self.index.shelfIndex();
        }

        pub fn byteOffset(self: Slice) usize {
            return self.index.byteOffset();
        }

        pub fn length(self: Slice) usize {
            return self.len;
        }

        pub fn byteOffsetFromStart(self: Slice) usize {
            return self.index.byteOffsetFromStart();
        }

        pub fn skip(self: Slice, n: usize) Slice {
            return self.limit(n, .unlimited);
        }

        pub fn limit(self: Slice, pos: usize, io_limit: std.Io.Limit) Slice {
            const remaining = self.length() -| pos;
            const allowed = switch (io_limit) {
                .unlimited => remaining,
                else => @min(@intFromEnum(io_limit), remaining),
            };
            const global_start = self.byteOffsetFromStart() + pos;
            const next_loc = Self.locateIndex(global_start);
            const next_index = Index.init(next_loc.shelf_index, next_loc.offset);
            return init(next_index, allowed);
        }

        pub fn reader(self: Slice, buffer: *const Self, scratch: []u8) segments.SegmentsReader {
            const i = self.shelfIndex();

            return segments.SegmentsReader.init(
                scratch,
                buffer.shelves[i..],
                self.byteOffset(),
                .limited(self.len),
            );
        }

        pub fn writer(self: Slice, buffer: *Self) segments.SegmentsWriter {
            const i = self.shelfIndex();
            const j = buffer.shelfCount();
            var head = buffer.shelves[i][self.byteOffset()..];
            head.len = @min(head.len, self.len);

            const body = if (i < j) buffer.shelves[i + 1 .. j] else &.{};
            return segments.SegmentsWriter.init(
                head,
                body,
                .limited(self.len),
            );
        }

        pub fn fromParts(index: Index, slice_len: usize) Slice {
            return init(index, slice_len);
        }

        fn init(index: Index, slice_len: usize) Slice {
            return .{ .index = index, .len = slice_len };
        }
    };

    pub const AppendResult = struct {
        slice: Slice,
        view: SegmentedView,
    };

    shelves: [max_supported_shelves][]u8 = mem.zeroes([max_supported_shelves][]u8),
    shelf_count: u8 = 0,
    latch: CapacityLatch = .{},

    pub const empty: Self = .{};

    pub fn deinit(self: *Self, alloc: Allocator) void {
        const count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const shelf = self.shelves[i];
            if (shelf.len != 0) alloc.free(shelf);
            self.shelves[i] = (@constCast(&[_]u8{}))[0..0];
        }
        self.* = undefined;
    }

    pub fn append(
        self: *Self,
        alloc: Allocator,
        bytes: []const u8,
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!Slice {
        if (bytes.len == 0) {
            _ = try self.reserveRange(alloc, 0);
            const index = Index.init(0, 0);
            const slice = Slice.init(index, 0);
            return slice;
        }

        const start = try self.reserveRange(alloc, bytes.len);
        const start_loc = Self.locateIndex(start);

        var remaining = bytes.len;
        var written: usize = 0;
        var shelf_index = start_loc.shelf_index;
        var shelf_offset = start_loc.offset;

        while (remaining != 0) {
            const available_shelves = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
            std.debug.assert(shelf_index < available_shelves);
            const shelf = self.shelves[shelf_index];
            const shelf_available = shelf.len - shelf_offset;
            const take = @min(remaining, shelf_available);
            @memcpy(shelf[shelf_offset .. shelf_offset + take], bytes[written .. written + take]);

            remaining -= take;
            written += take;
            shelf_index += 1;
            shelf_offset = 0;
        }

        const index = Index.init(start_loc.shelf_index, start_loc.offset);
        const slice = Slice.init(index, bytes.len);
        return slice;
    }

    pub fn len(self: *const Self) usize {
        return self.latch.len.load(.acquire);
    }

    pub fn occupiedBytes(self: *const Self) usize {
        return self.len();
    }

    pub fn capacityBytes(self: *const Self) usize {
        return self.latch.cap.load(.acquire);
    }

    pub fn shelfCount(self: *const Self) usize {
        return @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
    }

    pub fn isEmpty(self: *const Self) bool {
        return self.len() == 0;
    }

    pub fn debugStats(self: *const Self) DebugStats {
        return .{
            .shelf_count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire)),
            .length = self.len(),
            .occupied_bytes = self.occupiedBytes(),
            .capacity_bytes = self.capacityBytes(),
        };
    }

    fn reserveRange(
        self: *Self,
        alloc: Allocator,
        reserve_len: usize,
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!usize {
        if (reserve_len == 0) {
            return self.latch.len.load(.acquire);
        }

        var reservation = try self.latch.reservation(reserve_len);
        while (true) {
            try self.ensureCapacityTo(alloc, reservation.len);
            if (reservation.take()) |base| {
                return base;
            }
            reservation = try self.latch.reservation(reserve_len);
            std.atomic.spinLoopHint();
        }
    }

    fn ensureCapacityTo(
        self: *Self,
        alloc: Allocator,
        target: usize,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        while (true) {
            const snap = self.latch.snapshot();
            if (snap.cap >= target) return;

            if (self.latch.acquireOrWaitFor(target)) |growth_value| {
                var g = growth_value;
                defer g.unlock();

                try self.growUntilCapacityLocked(alloc, target);
                const new_cap = self.totalCapacity();
                std.debug.assert(new_cap >= target);
                std.debug.assert(new_cap >= g.start_cap);
                g.publishTo(new_cap);
            } else {
                return;
            }
        }
    }

    fn growUntilCapacityLocked(
        self: *Self,
        alloc: Allocator,
        target: usize,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        while (self.totalCapacity() < target) {
            try self.growOneShelf(alloc);
        }
    }

    fn growOneShelf(
        self: *Self,
        alloc: Allocator,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        const index = @as(usize, @atomicLoad(u8, &self.shelf_count, .monotonic));
        if (index >= max_supported_shelves) return error.ShelfLimitReached;

        const capacity = shelfCapacity(index);
        if (capacity == 0) return error.ShelfLimitReached;

        const buf = try alloc.alloc(u8, capacity);
        errdefer alloc.free(buf);

        std.debug.assert(self.shelves[index].len == 0);
        self.shelves[index] = buf;
        @atomicStore(u8, &self.shelf_count, @intCast(index + 1), .release);
    }

    pub fn shelfCapacity(index: usize) usize {
        if (index >= max_supported_shelves) return 0;
        var size: usize = base_unit;
        var i: usize = 0;
        while (i < index) : (i += 1) {
            size = std.math.mul(usize, size, 2) catch return 0;
        }
        return size;
    }

    fn shelfStartBytes(shelf_index: usize) usize {
        var total: usize = 0;
        var size: usize = base_unit;
        var i: usize = 0;
        while (i < shelf_index) : (i += 1) {
            total += size;
            size = std.math.mul(usize, size, 2) catch return std.math.maxInt(usize);
        }
        return total;
    }

    fn totalCapacity(self: *const Self) usize {
        const count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        var total: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            total += self.shelves[i].len;
        }
        return total;
    }

    fn locateIndex(index: usize) Location {
        var remaining = index;
        var size: usize = base_unit;
        var shelf_index: usize = 0;
        while (remaining >= size) {
            remaining -= size;
            shelf_index += 1;
            size = std.math.mul(usize, size, 2) catch break;
        }
        return .{ .shelf_index = shelf_index, .offset = remaining };
    }

    fn toConstShelfSlices(slice: []const []u8) []const []const u8 {
        const ptr = @as([*]const []const u8, @ptrCast(slice.ptr));
        return ptr[0..slice.len];
    }

    fn toMutableShelfSlices(slice: [][]u8) [][]u8 {
        const ptr = @as([*][]u8, @ptrCast(slice.ptr));
        return ptr[0..slice.len];
    }
};

fn expectViewEquals(
    expected: []const u8,
    buffer: *SegmentedStringBuffer,
    slice: SegmentedStringBuffer.Slice,
) !void {
    var scratch: [4096]u8 = @splat(0);
    var reader = slice.reader(buffer, &scratch);
    const buf = try reader.reader.allocRemaining(std.testing.allocator, .unlimited);
    defer std.testing.allocator.free(buf);
    try std.testing.expectEqualStrings(expected, buf);
}

test "SegmentedStringBuffer append and view" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first = (try buffer.append(allocator, "hello"));
    const second = (try buffer.append(allocator, "world"));

    try std.testing.expectEqual(@as(usize, 10), buffer.len());
    try expectViewEquals("hello", &buffer, first);
    try expectViewEquals("world", &buffer, second);

    const stats = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 10), stats.length);
}

test "SegmentedStringBuffer spans multiple shelves" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first = try buffer.append(allocator, "a" ** SegmentedStringBuffer.first_shelf_bytes);
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());
    try std.testing.expectEqual(first.length(), SegmentedStringBuffer.first_shelf_bytes);

    const second_len = SegmentedStringBuffer.first_shelf_bytes + 128;
    const payload = try allocator.alloc(u8, second_len);
    defer allocator.free(payload);
    @memset(payload, 'b');

    const second = try buffer.append(allocator, payload);
    try std.testing.expectEqual(@as(usize, 2), buffer.shelfCount());
    try expectViewEquals(payload, &buffer, second);
}

test "SegmentedStringBuffer SliceReader streams" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const text = "The quick brown fox jumps over the lazy dog";
    const res = try buffer.append(allocator, text);

    var reader = res.reader(&buffer, &[_]u8{});

    var collected = std.Io.Writer.Allocating.init(allocator);
    defer collected.deinit();

    const wrote = try reader.reader.streamRemaining(&collected.writer);

    try std.testing.expectEqual(text.len, wrote);
    try std.testing.expectEqualSlices(u8, text, collected.written());
}

test "SegmentedStringBuffer SliceWriter writes within one shelf" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const initial = "........";
    const res = try buffer.append(allocator, initial);

    var writer = res.writer(&buffer);

    try writer.writer().writeAll("ABC");
    try writer.writer().writeAll("DEF");
    try writer.writer().writeAll("GH");

    try expectViewEquals("ABCDEFGH", &buffer, res);
}

test "SegmentedStringBuffer SliceWriter drains across shelves" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const total_len = SegmentedStringBuffer.first_shelf_bytes + 256;

    const placeholder = try allocator.alloc(u8, total_len);
    defer allocator.free(placeholder);
    @memset(placeholder, '.');
    const res = try buffer.append(allocator, placeholder);

    const expected = try allocator.alloc(u8, total_len);
    defer allocator.free(expected);
    for (expected, 0..) |*byte, idx| {
        byte.* = @intCast('a' + (idx % 26));
    }

    var writer = res.writer(&buffer);
    try writer.writer().writeAll(expected);

    try expectViewEquals(expected, &buffer, res);
}

test "SegmentedStringBuffer SliceWriter fails when exceeding slice length" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const initial = "xxxxx";
    const res = try buffer.append(allocator, initial);

    var writer = res.writer(&buffer);
    try writer.writer().writeAll("abcde");
    try std.testing.expectError(error.WriteFailed, writer.writer().writeAll("!"));

    try expectViewEquals("abcde", &buffer, res);
}

test "SegmentedStringBuffer concurrent append" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var buffer = SegmentedStringBuffer.empty;
    defer buffer.deinit(allocator);

    const thread_count = 4;
    const per_thread = 20;
    const total = thread_count * per_thread;

    var slices: [total]SegmentedStringBuffer.Slice = undefined;

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(
            tid: usize,
            buf: *SegmentedStringBuffer,
            alloc: Allocator,
            per: usize,
            slice_storage: []SegmentedStringBuffer.Slice,
        ) !void {
            const start = tid * per;
            var i: usize = 0;
            while (i < per) : (i += 1) {
                var tmp: [32]u8 = undefined;
                const payload = try std.fmt.bufPrint(&tmp, "t{d}-{d}", .{ tid, i });
                const res = try buf.append(alloc, payload);
                slice_storage[start + i] = res;
            }
        }
    };

    try group.spawnMany(
        thread_count,
        Worker.run,
        .{ &buffer, allocator, per_thread, &slices },
    );
    group.wait();

    for (slices, 0..) |slice, idx| {
        var tmp: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(
            &tmp,
            "t{d}-{d}",
            .{ idx / per_thread, idx % per_thread },
        );
        try expectViewEquals(expected, &buffer, slice);
    }
}
