const std = @import("std");
const platform = @import("mac/platform.zig");

pub const AttrGroupMask = platform.AttrGroupMask;

pub const DirContext = packed struct {
    tag: u8 = 1,
    ver: u8 = 0,
    _pad: u16 = 0,
    dir_ix: u64,
    parent: u64,
    baseix: u32,
    flags: u32,
};

pub const RawBatch = packed struct {
    tag: u8 = 2,
    ver: u8 = 0,
    _pad: u16 = 0,
    len: u32,
};

const State = struct {
    fd: std.posix.fd_t,
    mask: AttrGroupMask,
    last_entries: usize = 0,
    last_bytes: usize = 0,
    last_error: ?anyerror = null,
};

const Result = struct {
    entries: usize,
    bytes: usize,
};

const FetchError = error{
    BadAddress,
    BadFileDescriptor,
    BufferTooSmall,
    DeadLock,
    InvalidArgument,
    NotDir,
    PermissionDenied,
    ReadFailed,
    TimedOut,
};

const AttrList = platform.AttrList;
const FsOptMask = platform.FsOptMask;

fn stream(r: *std.Io.Reader, w: *std.Io.Writer, _: std.Io.Limit) std.Io.StreamError!usize {
    const self: *AttrBulkReader = @fieldParentPtr("reader", r);

    const slice = w.writableSliceGreedy(1);
    if (slice.len != 0) {
        const res = getattrlistbulk_into(&self.state, slice) catch |err| return self.handleStreamError(err);
        if (res.entries == 0) return error.EndOfStream;
        self.state.last_entries = res.entries;
        self.state.last_bytes = res.bytes;
        self.state.last_error = null;
        w.advance(res.bytes);
        return res.bytes;
    }

    const available = r.buffer.len - r.end;
    if (available == 0) return 0;
    const dest = r.buffer[r.end..];
    const res = getattrlistbulk_into(&self.state, dest) catch |err| return self.handleStreamError(err);
    if (res.entries == 0) return error.EndOfStream;
    self.state.last_entries = res.entries;
    self.state.last_bytes = res.bytes;
    self.state.last_error = null;
    r.end += res.bytes;
    return 0;
}

fn getattrlistbulk_into(state: *State, buffer: []u8) FetchError!Result {
    var al = AttrList{
        .bitmapcount = platform.ATTR_BIT_MAP_COUNT,
        .reserved = 0,
        .attrs = state.mask,
    };

    const opts = FsOptMask{
        .nofollow = true,
        .report_fullsize = true,
        .pack_invalid_attrs = true,
    };

    while (true) {
        const rc = platform.getattrlistbulk(state.fd, &al, buffer.ptr, buffer.len, opts);
        if (rc < 0) {
            switch (std.posix.errno(rc)) {
                .INTR, .AGAIN => continue,
                .NOTDIR => return error.NotDir,
                .BADF => return error.BadFileDescriptor,
                .ACCES => return error.PermissionDenied,
                .PERM => return error.PermissionDenied,
                .FAULT => return error.BadAddress,
                .RANGE => return error.BufferTooSmall,
                .INVAL => return error.InvalidArgument,
                .IO => return error.ReadFailed,
                .TIMEDOUT => return error.TimedOut,
                .DEADLK => return error.DeadLock,
                .NOENT => unreachable,
                else => |e| std.debug.panic("unexpected errno {t}", .{e}),
            }
        }

        if (rc == 0) return Result{ .entries = 0, .bytes = 0 };

        const entries = @abs(rc);
        var offset: usize = 0;
        var i: usize = 0;
        while (i < entries) : (i += 1) {
            if (offset + 4 > buffer.len) return error.BufferTooSmall;
            const length = std.mem.readIntLittle(u32, buffer[offset .. offset + 4]);
            if (length == 0) return error.ReadFailed;
            const next = offset + length;
            if (next > buffer.len) return error.BufferTooSmall;
            offset = next;
        }

        return Result{ .entries = entries, .bytes = offset };
    }
}

pub const AttrBulkReader = struct {
    reader: std.Io.Reader,
    state: State,

    pub fn init(dirfd: std.posix.fd_t, mask: AttrGroupMask, buffer: []u8) AttrBulkReader {
        return .{
            .reader = .{
                .vtable = &.{ .stream = stream },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .state = .{ .fd = dirfd, .mask = mask },
        };
    }

    fn handleStreamError(self: *AttrBulkReader, err: FetchError) std.Io.StreamError {
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        self.state.last_error = err;
        return error.ReadFailed;
    }

    pub fn readerPtr(self: *AttrBulkReader) *std.Io.Reader {
        return &self.reader;
    }

    pub fn reset(self: *AttrBulkReader) void {
        self.reader.seek = 0;
        self.reader.end = 0;
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        self.state.last_error = null;
    }

    pub fn fillBatch(self: *AttrBulkReader) !bool {
        self.reset();
        self.reader.fillMore() catch |err| switch (err) {
            error.EndOfStream => return false,
            error.ReadFailed => {
                const actual = self.state.last_error orelse error.ReadFailed;
                self.state.last_error = null;
                return actual;
            },
            error.WriteFailed => return error.ReadFailed,
        };

        if (self.state.last_entries == 0) return false;
        return true;
    }

    pub fn lastEntries(self: *const AttrBulkReader) usize {
        return self.state.last_entries;
    }

    pub fn lastBytes(self: *const AttrBulkReader) usize {
        return self.state.last_bytes;
    }

    pub fn batchSlice(self: *AttrBulkReader) []const u8 {
        return self.reader.buffer[0..self.state.last_bytes];
    }
};

pub fn makeAttrBulkReader(
    dirfd: std.posix.fd_t,
    mask: AttrGroupMask,
    buffer: []u8,
) AttrBulkReader {
    return AttrBulkReader.init(dirfd, mask, buffer);
}

pub fn writeDirContext(w: *std.Io.Writer, ctx: DirContext) !void {
    var header: [24]u8 = undefined;
    header[0] = ctx.tag;
    header[1] = ctx.ver;
    header[2] = 0;
    header[3] = 0;
    std.mem.writeIntLittle(u64, header[4..12], ctx.dir_ix);
    std.mem.writeIntLittle(u64, header[12..20], ctx.parent);
    std.mem.writeIntLittle(u32, header[20..24], ctx.baseix);
    try w.writeAll(header[0..]);
    var flags_buf: [4]u8 = undefined;
    std.mem.writeIntLittle(u32, flags_buf[0..], ctx.flags);
    try w.writeAll(flags_buf[0..]);
}

pub fn writeRawBatch(w: *std.Io.Writer, payload: []const u8) !void {
    var header: [8]u8 = undefined;
    header[0] = 2;
    header[1] = 0;
    header[2] = 0;
    header[3] = 0;
    const len = std.math.cast(u32, payload.len) orelse return error.PayloadTooLarge;
    std.mem.writeIntLittle(u32, header[4..8], len);
    try w.writeAll(header[0..]);
    try w.writeAll(payload);
}

pub const Error = error{PayloadTooLarge};
