const std = @import("std");

/// Minimal segmented writer: treats `io.buffer` as the active segment and
/// walks a tail queue of additional slices. No allocations; all slices point to
/// caller-owned storage.
pub const SegmentsWriter = struct {
    const Self = @This();

    io: std.Io.Writer,
    tail: []const []u8,
    remaining: usize,
    capacity: usize,

    pub fn init(head: []u8, tail: []const []u8, limit: std.Io.Limit) Self {
        const capacity = head.len + std.Io.Limit.countVec(tail).toInt().?;
        return Self{
            .io = .{
                .vtable = &vtable,
                .buffer = head,
                .end = 0,
            },
            .tail = tail,
            .remaining = limit.minInt(capacity),
            .capacity = capacity,
        };
    }

    pub fn writer(self: *Self) *std.Io.Writer {
        return &self.io;
    }

    pub fn remainingCapacity(self: *const Self) usize {
        return self.remaining;
    }

    fn drain(
        w: *std.Io.Writer,
        data: []const []const u8,
        _: usize,
    ) std.Io.Writer.Error!usize {
        var self: *SegmentsWriter = @fieldParentPtr("io", w);

        w.buffer = w.buffer[w.end..];
        w.end = 0;

        if (data[0].len > self.remaining) return error.WriteFailed;
        if (w.buffer.len == 0) {
            if (self.tail.len == 0) return error.WriteFailed;
            w.buffer = self.tail[0];
            w.buffer.len = @min(w.buffer.len, self.remaining);
            self.tail = self.tail[1..];
            w.end = 0;
        }

        const n = @min(w.buffer.len, data[0].len);
        @memcpy(w.buffer[0..n], data[0].ptr);

        w.buffer = w.buffer[n..];
        self.remaining -= n;

        return n;
    }

    const vtable: std.Io.Writer.VTable = .{
        .drain = drain,
    };
};

test "SegmentsWriter writes across multiple segments" {
    var seg0: [4]u8 = undefined;
    var seg1: [2]u8 = undefined;
    var seg2: [3]u8 = undefined;

    var tail = [_][]u8{ seg1[0..], seg2[0..] };
    var writer = SegmentsWriter.init(seg0[0..], tail[0..], .limited(9));

    try writer.writer().writeAll("abcdefghi");

    try std.testing.expectEqualStrings("abcd", seg0[0..]);
    try std.testing.expectEqualStrings("ef", seg1[0..]);
    try std.testing.expectEqualStrings("ghi", seg2[0..]);
    try std.testing.expectEqual(@as(usize, 0), writer.remainingCapacity());
    try std.testing.expectError(error.WriteFailed, writer.writer().writeAll("x"));
}

test "SegmentsWriter respects limit shorter than total" {
    var seg0: [4]u8 = undefined;
    var seg1: [4]u8 = undefined;

    var tail = [_][]u8{seg1[0..]};
    var writer = SegmentsWriter.init(seg0[0..], tail[0..], .limited(6));

    try writer.writer().writeAll("abcdef");
    try std.testing.expectEqualStrings("abcd", seg0[0..]);
    try std.testing.expectEqualSlices(u8, "ef", seg1[0..2]);
    try std.testing.expectEqual(@as(usize, 0), writer.remainingCapacity());
    try std.testing.expectError(error.WriteFailed, writer.writer().writeByte('x'));
}

/// Limited reader over segmented shelves.
///
/// The reader exposes `buffer` through the generic `std.Io.Reader` interface
/// without copying any shelf data. Each call to `stream` performs at most one
/// write: it drains the pending partial shelf with `write`, or (when perfectly
/// aligned) batches contiguous whole shelves into a single `writeVec` call.
pub const SegmentsReader = struct {
    reader: std.Io.Reader,
    tail: []const []const u8,
    seek: usize,
    limit: std.Io.Limit,

    pub fn init(
        buffer: []u8,
        tail: []const []const u8,
        seek: usize,
        limit: std.Io.Limit,
    ) SegmentsReader {
        return .{
            .reader = .{
                .vtable = &vtable,
                .buffer = buffer,
                .end = 0,
                .seek = 0,
            },
            .seek = seek,
            .tail = tail,
            .limit = limit,
        };
    }

    /// Streams data out of the segmented slice while respecting both
    /// the reader's own limit and the caller-provided `limit`. The method
    /// prefers zero-copy `writeVec` over individual writes whenever the
    /// request is aligned to whole shelves, falling back to a single
    /// `write` for partial shelves.
    fn stream(
        r: *std.Io.Reader,
        w: *std.Io.Writer,
        limit: std.Io.Limit,
    ) std.Io.Reader.StreamError!usize {
        var self: *SegmentsReader = @fieldParentPtr("reader", r);

        const request_bytes = try self.beginRequest(limit);

        if (self.seek != 0) {
            return self.emitPartialHead(w, request_bytes);
        }

        const aligned_count = self.countAlignedShelves(request_bytes);
        if (aligned_count == 0) {
            return self.emitHeadPrefix(w, request_bytes);
        }

        return self.emitAlignedShelves(w, aligned_count);
    }

    const vtable: std.Io.Reader.VTable = .{ .stream = stream };

    fn prune(self: *SegmentsReader) void {
        while (self.tail.len != 0) {
            const shelf_len = self.tail[0].len;
            if (self.seek < shelf_len) break;
            self.seek -= shelf_len;
            self.tail = self.tail[1..];
        }
    }

    fn consume(self: *SegmentsReader, amount: usize) void {
        if (amount == 0) return;

        var remaining = amount;
        while (remaining != 0 and self.tail.len != 0) {
            const current = self.tail[0];
            const available = current.len - self.seek;
            if (remaining < available) {
                self.seek += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                self.seek = 0;
                self.tail = self.tail[1..];
            }
        }

        if (self.limit.subtract(amount)) |next| self.limit = next else self.limit = .nothing;
    }

    fn finish(self: *SegmentsReader, amount: usize) usize {
        self.consume(amount);
        self.prune();
        return amount;
    }

    /// Resolve how many bytes the caller may read this turn and ensure the
    /// head shelf is positioned correctly. Returns `error.EndOfStream` when
    /// both limits are exhausted.
    fn beginRequest(self: *SegmentsReader, limit: std.Io.Limit) !usize {
        self.prune();
        if (self.tail.len == 0) return error.EndOfStream;

        const merged = self.limit.min(limit);
        const bytes = merged.minInt(std.math.maxInt(usize));
        if (bytes == 0) return error.EndOfStream;
        return bytes;
    }

    /// Consume a partially-read head shelf using a single `write` call.
    fn emitPartialHead(
        self: *SegmentsReader,
        w: *std.Io.Writer,
        request_bytes: usize,
    ) std.Io.Reader.StreamError!usize {
        const head = self.tail[0];
        const available = head.len - self.seek;
        const take = @min(available, request_bytes);
        if (take == 0) return error.EndOfStream;

        const written = try w.write(head[self.seek .. self.seek + take]);
        return self.finish(written);
    }

    /// Emit the required prefix of the head shelf when there are no fully
    /// aligned shelves to batch.
    fn emitHeadPrefix(
        self: *SegmentsReader,
        w: *std.Io.Writer,
        request_bytes: usize,
    ) std.Io.Reader.StreamError!usize {
        const head = self.tail[0];
        const take = @min(head.len, request_bytes);
        if (take == 0) return error.EndOfStream;

        const written = try w.write(head[0..take]);
        return self.finish(written);
    }

    /// Batch whole shelves using `writeVec`, enabling downstream vectored I/O.
    fn emitAlignedShelves(
        self: *SegmentsReader,
        w: *std.Io.Writer,
        aligned_count: usize,
    ) std.Io.Reader.StreamError!usize {
        const written = try w.writeVec(self.tail[0..aligned_count]);
        return self.finish(written);
    }

    /// Count how many complete shelves fit inside the current request window.
    fn countAlignedShelves(self: *SegmentsReader, request_bytes: usize) usize {
        var idx: usize = 0;
        var total: usize = 0;
        while (idx < self.tail.len) {
            const shelf = self.tail[idx];
            if (shelf.len == 0) {
                idx += 1;
                continue;
            }

            if (total + shelf.len > request_bytes) break;
            total += shelf.len;
            idx += 1;

            if (total == request_bytes) break;
        }

        return idx;
    }
};

test "SegmentsReader returns EndOfStream when limit is empty" {
    var scratch: [4]u8 = undefined;
    const shelf0 = [_]u8{ 'a', 'b', 'c' };
    const tail = [_][]const u8{shelf0[0..]};
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 0, .limited(0));

    var sink_buf: [4]u8 = undefined;
    var sink = std.Io.Writer.fixed(sink_buf[0..]);
    const total = try reader.reader.streamRemaining(&sink);
    try std.testing.expectEqual(@as(usize, 0), total);
    try std.testing.expectEqual(@as(usize, 0), sink.end);
}

test "SegmentsReader streams multiple shelves in one call" {
    var scratch: [8]u8 = undefined;
    const shelf0 = [_]u8{ 'a', 'b', 'c', 'd' };
    const shelf1 = [_]u8{ 'e', 'f', 'g', 'h' };
    const shelf2 = [_]u8{ 'i', 'j' };
    const tail = [_][]const u8{ shelf0[0..], shelf1[0..], shelf2[0..] };
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 0, .limited(10));

    var sink_buf: [12]u8 = undefined;
    var sink = std.Io.Writer.fixed(sink_buf[0..]);
    const total = try reader.reader.streamRemaining(&sink);

    try std.testing.expectEqual(@as(usize, 10), total);
    try std.testing.expectEqualSlices(u8, "abcdefghij", sink_buf[0..total]);
    try std.testing.expectEqual(@as(usize, 0), reader.tail.len);
    try std.testing.expectEqual(@as(usize, 0), reader.seek);
    try std.testing.expectEqual(@as(usize, 0), reader.limit.toInt().?);
}

test "SegmentsReader writeVec stops before partial tail" {
    var scratch: [8]u8 = undefined;
    const shelf0 = [_]u8{ 'a', 'b', 'c', 'd' };
    const shelf1 = [_]u8{ 'e', 'f', 'g', 'h' };
    const shelf2 = [_]u8{ 'i', 'j' };
    const tail = [_][]const u8{ shelf0[0..], shelf1[0..], shelf2[0..] };
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 0, .limited(9));

    var vec_buf: [12]u8 = undefined;
    var vec_sink = std.Io.Writer.fixed(vec_buf[0..]);
    const total = try reader.reader.streamRemaining(&vec_sink);

    try std.testing.expectEqual(@as(usize, 9), total);
    try std.testing.expectEqualSlices(u8, "abcdefghi", vec_buf[0..total]);
    try std.testing.expectEqual(@as(usize, 1), reader.tail.len);
    try std.testing.expectEqual(@as(usize, 1), reader.seek);
    try std.testing.expectEqual(@as(usize, 0), reader.limit.toInt().?);
}

test "SegmentsReader updates seek after partial slice" {
    var scratch: [6]u8 = undefined;
    const shelf0 = [_]u8{ 'a', 'b', 'c', 'd', 'e', 'f' };
    const tail = [_][]const u8{shelf0[0..]};
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 0, .limited(6));

    var sink_buf: [6]u8 = undefined;
    var sink = std.Io.Writer.fixed(sink_buf[0..]);
    const wrote = try reader.reader.stream(&sink, .limited(5));

    try std.testing.expectEqual(@as(usize, 5), wrote);
    try std.testing.expectEqualSlices(u8, "abcde", sink_buf[0..5]);
    try std.testing.expectEqual(@as(usize, 1), reader.limit.toInt().?);
    try std.testing.expectEqual(@as(usize, 5), reader.seek);
    try std.testing.expectEqual(@as(usize, 1), reader.tail.len);
}

test "SegmentsReader respects external limit argument" {
    var scratch: [8]u8 = undefined;
    const shelf0 = [_]u8{ 'x', 'y', 'z', 'q' };
    const shelf1 = [_]u8{ 'r', 's', 't', 'u' };
    const tail = [_][]const u8{ shelf0[0..], shelf1[0..] };
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 0, .unlimited);

    var sink_buf: [8]u8 = undefined;
    var sink = std.Io.Writer.fixed(sink_buf[0..]);
    const wrote = try reader.reader.stream(&sink, .limited(3));

    try std.testing.expectEqual(@as(usize, 3), wrote);
    try std.testing.expectEqualSlices(u8, "xyz", sink_buf[0..3]);
    try std.testing.expect(reader.limit == .unlimited);
    try std.testing.expectEqual(@as(usize, 3), reader.seek);
    try std.testing.expectEqual(@as(usize, 2), reader.tail.len);
}

test "SegmentsReader skips empty shelves and honors seek" {
    var scratch: [8]u8 = undefined;
    const empty = [_]u8{};
    const shelf0 = [_]u8{ 'p', 'q', 'r' };
    const shelf1 = [_]u8{ 's', 't' };
    const tail = [_][]const u8{ empty[0..], shelf0[0..], shelf1[0..] };
    var reader = SegmentsReader.init(scratch[0..], tail[0..], 1, .limited(4));

    var sink_buf: [8]u8 = undefined;
    var sink = std.Io.Writer.fixed(sink_buf[0..]);
    const total = try reader.reader.streamRemaining(&sink);

    try std.testing.expectEqual(@as(usize, 4), total);
    try std.testing.expectEqualSlices(u8, "qrst", sink_buf[0..total]);
    try std.testing.expectEqual(@as(usize, 0), reader.tail.len);
    try std.testing.expectEqual(@as(usize, 0), reader.seek);
    try std.testing.expectEqual(@as(usize, 0), reader.limit.toInt().?);
}
