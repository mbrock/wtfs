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

/// Limited writer into a segmented string buffer slice.
///
/// Uses existing shelf storage without copying the segment list. The
/// writer simply walks `shelves` one slice at a time.
pub const SegmentsReader = struct {
    reader: std.Io.Reader,
    tail: []const []const u8,
    seek: usize,
    limit: std.Io.Limit,

    pub fn init(
        buffer: []u8,
        tail: []const []u8,
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

    fn stream(
        r: *std.Io.Reader,
        w: *std.Io.Writer,
        limit: std.Io.Limit,
    ) std.Io.Reader.StreamError!usize {
        var self: *SegmentsReader = @fieldParentPtr("reader", r);
        var tail = self.tail[0][self.seek..];
        tail.len = self.limit.minInt(tail.len);

        if (tail.len == 0) return error.EndOfStream;

        const n = self.limit.min(limit).minInt(tail.len);

        const written = try w.write(tail[0..n]);
        const remainder = tail.len - written;
        if (remainder == 0) {
            self.tail = self.tail[1..];
            self.seek = 0;
        } else {
            self.seek += written;
        }
        self.limit = self.limit.subtract(written).?;
        return written;
    }

    const vtable: std.Io.Reader.VTable = .{ .stream = stream };
};
