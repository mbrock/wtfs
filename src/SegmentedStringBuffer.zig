const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const test_threads = @import("test_threading.zig");

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
            std.debug.assert(offset <= std.math.maxInt(OffsetInt));
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

        pub fn reader(self: Slice, buffer: *const Self, scratch: []u8) SliceReader {
            return SliceReader.init(buffer, self, scratch);
        }

        pub fn writer(self: Slice, buffer: *Self) SliceWriter {
            return SliceWriter.init(buffer, self);
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

    pub const SliceWriter = struct {
        writer: std.Io.Writer,
        segbuf: *Self,
        slice: Slice,
        current_shelf: usize,
        slice_bytes_written: usize,

        pub fn init(buffer: *Self, slice: Slice) SliceWriter {
            var self = SliceWriter{
                .writer = .{ .vtable = &vtable, .buffer = &.{}, .end = 0 },
                .segbuf = buffer,
                .slice = slice,
                .current_shelf = slice.shelfIndex(),
                .slice_bytes_written = 0,
            };
            self.setupBuffer();
            return self;
        }

        pub fn pos(self: *const SliceWriter) usize {
            return self.slice_bytes_written + self.writer.end;
        }

        fn setupBuffer(self: *SliceWriter) void {
            if (self.slice_bytes_written >= self.slice.length()) {
                self.writer.buffer = &.{};
                self.writer.end = 0;
                return;
            }

            const remaining = self.slice.length() - self.slice_bytes_written;
            const current_global_pos = self.slice.byteOffsetFromStart() + self.slice_bytes_written;
            const location = Self.locateIndex(current_global_pos);

            self.current_shelf = location.shelf_index;
            const shelf = self.segbuf.shelves[self.current_shelf];
            const shelf_start = location.offset;
            const shelf_available = shelf.len - shelf_start;
            const buffer_size = @min(remaining, shelf_available);

            self.writer.buffer = shelf[shelf_start .. shelf_start + buffer_size];
            self.writer.end = 0;
        }

        fn drain(w: *std.Io.Writer, data: []const []const u8, _: usize) std.Io.Writer.Error!usize {
            const self: *SliceWriter = @fieldParentPtr("writer", w);

            self.slice_bytes_written += w.end;
            w.end = 0;

            if (self.slice_bytes_written >= self.slice.length()) {
                return error.WriteFailed;
            }

            self.setupBuffer();

            if (w.buffer.len > 0 and data.len > 0) {
                const first_slice = data[0];
                const to_copy = @min(first_slice.len, w.buffer.len);
                @memcpy(w.buffer[0..to_copy], first_slice[0..to_copy]);
                w.end = to_copy;
                return to_copy;
            }

            return 0;
        }

        fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {}

        fn rebase(w: *std.Io.Writer, preserve: usize, minimum_len: usize) std.Io.Writer.Error!void {
            const self: *SliceWriter = @fieldParentPtr("writer", w);
            const current_pos = self.slice_bytes_written + w.end;
            const needed_total = preserve + minimum_len;
            const would_exceed_token = current_pos + needed_total > self.slice.length();

            if (!would_exceed_token) {
                return std.Io.Writer.defaultRebase(w, preserve, minimum_len);
            }

            return error.WriteFailed;
        }

        const vtable: std.Io.Writer.VTable = .{
            .drain = drain,
            .flush = flush,
            .rebase = rebase,
        };
    };

    pub const SliceReader = struct {
        reader: std.Io.Reader,
        buffer: *const Self,
        slice: Slice,
        pos: usize,

        pub fn init(buffer: *const Self, slice: Slice, scratch: []u8) SliceReader {
            return .{
                .reader = .{ .vtable = &vtable, .buffer = scratch, .seek = 0, .end = 0 },
                .buffer = buffer,
                .slice = slice,
                .pos = 0,
            };
        }

        fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *SliceReader = @fieldParentPtr("reader", r);
            if (self.pos >= self.slice.length()) return error.EndOfStream;

            const limited = self.slice.limit(self.pos, limit);
            if (limited.length() == 0) return 0;

            const seg_view = self.buffer.view(limited);
            var total: usize = 0;

            if (seg_view.head.len > 0) {
                total += try w.writeVec(&[_][]const u8{seg_view.head});
            }

            if (seg_view.body.len > 0) {
                total += try w.writeVec(seg_view.body);
            }

            if (seg_view.tail.len > 0) {
                total += try w.writeVec(&[_][]const u8{seg_view.tail});
            }

            self.pos += total;
            return total;
        }

        const vtable: std.Io.Reader.VTable = .{ .stream = stream };
    };

    shelves: [max_supported_shelves][]u8 = mem.zeroes([max_supported_shelves][]u8),
    shelf_count: u8 = 0,
    len_counter: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    grow_mutex: std.Thread.Mutex = .{},

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
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!AppendResult {
        if (bytes.len == 0) {
            _ = try self.reserveRange(alloc, 0);
            const index = Index.init(0, 0);
            const slice = Slice.init(index, 0);
            return .{ .slice = slice, .view = SegmentedView.empty() };
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
        return .{ .slice = slice, .view = self.view(slice) };
    }

    pub fn view(self: *const Self, slice: Slice) SegmentedView {
        if (slice.length() == 0) return SegmentedView.empty();

        const start_global = slice.byteOffsetFromStart();
        const start_loc = Self.locateIndex(start_global);
        const available = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        std.debug.assert(start_loc.shelf_index < available);

        const first_shelf = self.shelves[start_loc.shelf_index];
        const first_available = first_shelf.len - start_loc.offset;
        const total_len = slice.length();

        if (total_len <= first_available) {
            return .{
                .head = first_shelf[start_loc.offset .. start_loc.offset + total_len],
                .body = SegmentedView.emptyBody(),
                .tail = &[_]u8{},
            };
        }

        var remaining = total_len - first_available;
        var shelf_index = start_loc.shelf_index + 1;

        while (true) {
            std.debug.assert(shelf_index < available);
            const shelf = self.shelves[shelf_index];
            if (remaining < shelf.len) {
                const body_slice = if (shelf_index > start_loc.shelf_index + 1)
                    toConstShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index])
                else
                    SegmentedView.emptyBody();

                return .{
                    .head = first_shelf[start_loc.offset..],
                    .body = body_slice,
                    .tail = shelf[0..remaining],
                };
            }

            if (remaining == shelf.len) {
                const body_slice = toConstShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index + 1]);
                return .{
                    .head = first_shelf[start_loc.offset..],
                    .body = body_slice,
                    .tail = &[_]u8{},
                };
            }

            remaining -= shelf.len;
            shelf_index += 1;
        }
    }

    pub fn viewMutable(self: *Self, slice: Slice) MutableSegmentedView {
        if (slice.length() == 0) return MutableSegmentedView.empty();

        const start_global = slice.byteOffsetFromStart();
        const start_loc = Self.locateIndex(start_global);
        const available = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        std.debug.assert(start_loc.shelf_index < available);

        const first_shelf = self.shelves[start_loc.shelf_index];
        const first_available = first_shelf.len - start_loc.offset;
        const total_len = slice.length();

        if (total_len <= first_available) {
            return .{
                .head = first_shelf[start_loc.offset .. start_loc.offset + total_len],
                .body = MutableSegmentedView.emptyBody(),
                .tail = &[_]u8{},
            };
        }

        var remaining = total_len - first_available;
        var shelf_index = start_loc.shelf_index + 1;

        while (true) {
            std.debug.assert(shelf_index < available);
            const shelf = self.shelves[shelf_index];
            if (remaining < shelf.len) {
                const body_slice = if (shelf_index > start_loc.shelf_index + 1)
                    toMutableShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index])
                else
                    MutableSegmentedView.emptyBody();

                return .{
                    .head = first_shelf[start_loc.offset..],
                    .body = body_slice,
                    .tail = shelf[0..remaining],
                };
            }

            if (remaining == shelf.len) {
                const body_slice = toMutableShelfSlices(self.shelves[start_loc.shelf_index + 1 .. shelf_index + 1]);
                return .{
                    .head = first_shelf[start_loc.offset..],
                    .body = body_slice,
                    .tail = &[_]u8{},
                };
            }

            remaining -= shelf.len;
            shelf_index += 1;
        }
    }

    pub fn len(self: *const Self) usize {
        return self.len_counter.load(.acquire);
    }

    pub fn occupiedBytes(self: *const Self) usize {
        return self.len();
    }

    pub fn capacityBytes(self: *const Self) usize {
        return self.totalCapacity();
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
        while (true) {
            const current = self.len_counter.load(.acquire);
            const end = std.math.add(usize, current, reserve_len) catch return error.Overflow;
            try self.ensureCapacityFor(alloc, end);
            if (self.len_counter.cmpxchgWeak(current, end, .acq_rel, .acquire) == null) {
                return current;
            }
            std.atomic.spinLoopHint();
        }
    }

    fn ensureCapacityFor(
        self: *Self,
        alloc: Allocator,
        required: usize,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        while (true) {
            if (self.totalCapacity() >= required) return;
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

        if (self.totalCapacity() >= required) return;
        try self.growOneShelf(alloc);
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

    fn shelfCapacity(index: usize) usize {
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

fn expectViewEquals(expected: []const u8, view: SegmentedStringBuffer.SegmentedView) !void {
    try std.testing.expectEqual(expected.len, view.totalLength());
    var pos: usize = 0;

    if (view.head.len > 0) {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + view.head.len], view.head);
        pos += view.head.len;
    }

    for (view.body) |segment| {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + segment.len], segment);
        pos += segment.len;
    }

    if (view.tail.len > 0) {
        try std.testing.expectEqualSlices(u8, expected[pos .. pos + view.tail.len], view.tail);
    }
}

test "SegmentedStringBuffer append and view" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first = try buffer.append(allocator, "hello");
    const second = try buffer.append(allocator, "world");

    try std.testing.expectEqual(@as(usize, 10), buffer.len());
    try expectViewEquals("hello", first.view);
    try expectViewEquals("world", buffer.view(second.slice));

    const stats = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 10), stats.length);
}

test "SegmentedStringBuffer spans multiple shelves" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first = try buffer.append(allocator, "a" ** SegmentedStringBuffer.first_shelf_bytes);
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());
    try std.testing.expectEqual(first.slice.length(), SegmentedStringBuffer.first_shelf_bytes);

    const second_len = SegmentedStringBuffer.first_shelf_bytes + 128;
    const payload = try allocator.alloc(u8, second_len);
    defer allocator.free(payload);
    @memset(payload, 'b');

    const second = try buffer.append(allocator, payload);
    try std.testing.expectEqual(@as(usize, 2), buffer.shelfCount());
    try expectViewEquals(payload, buffer.view(second.slice));
}

test "SegmentedStringBuffer SliceReader streams" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const text = "The quick brown fox jumps over the lazy dog";
    const res = try buffer.append(allocator, text);

    var scratch: [16]u8 = undefined;
    var reader = res.slice.reader(&buffer, &scratch);

    var collected = std.ArrayList(u8){};
    defer collected.deinit(allocator);

    var array_writer = collected.writer(allocator);
    var adapter_buffer: [128]u8 = undefined;
    var adapter = array_writer.adaptToNewApi(&adapter_buffer);

    const wrote = try reader.reader.streamRemaining(&adapter.new_interface);
    try adapter.new_interface.flush();
    try std.testing.expect(adapter.err == null);
    try std.testing.expectEqual(text.len, wrote);
    try std.testing.expectEqualSlices(u8, text, collected.items);
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

    const slices = try allocator.alloc(SegmentedStringBuffer.Slice, total);
    defer allocator.free(slices);

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
                slice_storage[start + i] = res.slice;
            }
        }
    };

    try group.spawnMany(thread_count, Worker.run, .{ &buffer, allocator, per_thread, slices });
    group.wait();

    for (slices, 0..) |slice, idx| {
        var tmp: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&tmp, "t{d}-{d}", .{ idx / per_thread, idx % per_thread });
        try expectViewEquals(expected, buffer.view(slice));
    }
}

pub fn main() !void {
    const tabs = @import("TabWriter.zig");

    const shelf_count: u6 = 32;
    const base_shifts = [_]u6{ 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    var stdoutbuf: [4096]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&stdoutbuf);

    var columns = [_]tabs.Column{
        .{ .width = 6, .alignment = .right },
        .{ .width = 8, .alignment = .right },
    };

    for (base_shifts) |shift| {
        var table = tabs.TabWriter.init(&stdout.interface, &columns);

        try table.writeHeader(&[2][]const u8{ "shelf", "size" });
        try table.writeSeparator("─");
        const base_bytes: usize = @as(usize, 1) << shift;

        for (0..shelf_count) |shelf_index| {
            const shelf_bytes = base_bytes << @intCast(shelf_index);
            try table.printRow(
                .{ "{d}", "{Bi: >6.0}" },
                .{ shelf_index, shelf_bytes },
            );
        }

        try table.finish();
        try stdout.interface.writeAll("\n");
    }

    try stdout.interface.flush();
}