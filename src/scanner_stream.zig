const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.target.os.tag != .macos) {
        @compileError("scanner_stream is only supported on macOS");
    }
}

const mac = @import("wtfs").mac;

const posix = std.posix;

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

pub const AttrBulkReader = struct {
    interface: std.Io.Reader,
    state: State,

    const State = struct {
        fd: std.posix.fd_t,
        mask: mac.AttrGroupMask,
        options: mac.FsOptMask,
        last_entries: usize = 0,
        last_bytes: usize = 0,
        last_error: ?anyerror = null,
    };

    pub fn init(
        dirfd: std.posix.fd_t,
        mask: mac.AttrGroupMask,
        buffer: []u8,
    ) AttrBulkReader {
        return .{
            .interface = .{
                .vtable = &.{ .stream = stream },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .state = .{
                .fd = dirfd,
                .mask = mask,
                .options = .{
                    .nofollow = true,
                    .report_fullsize = true,
                    .pack_invalid_attrs = true,
                },
            },
        };
    }

    pub fn fillBatch(self: *AttrBulkReader) !bool {
        self.interface.seek = 0;
        self.interface.end = 0;
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        self.state.last_error = null;

        while (true) {
            self.interface.fillMore() catch |err| switch (err) {
                error.EndOfStream => return false,
                error.ReadFailed => {
                    const stored = self.state.last_error orelse return error.ReadFailed;
                    self.state.last_error = null;
                    return stored;
                },
            };

            if (self.state.last_bytes != 0) {
                self.interface.seek = 0;
                return true;
            }
        }
    }

    pub fn lastEntryCount(self: *const AttrBulkReader) usize {
        return self.state.last_entries;
    }

    pub fn lastBatchBytes(self: *const AttrBulkReader) usize {
        return self.state.last_bytes;
    }

    pub fn batchSlice(self: *const AttrBulkReader) []const u8 {
        return self.interface.buffer[0..self.state.last_bytes];
    }

    pub fn resetBuffer(self: *AttrBulkReader) void {
        self.interface.seek = 0;
        self.interface.end = 0;
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        self.state.last_error = null;
    }

    pub fn flags(self: *const AttrBulkReader) u32 {
        return @bitCast(self.state.options);
    }
};

pub fn makeAttrBulkReader(
    dirfd: std.posix.fd_t,
    mask: mac.AttrGroupMask,
    buffer: []u8,
) AttrBulkReader {
    return AttrBulkReader.init(dirfd, mask, buffer);
}

pub fn writeDirContext(w: *std.Io.Writer, ctx: DirContext) !void {
    try w.writeAll(std.mem.asBytes(&ctx));
}

fn stream(
    io_reader: *std.Io.Reader,
    io_writer: *std.Io.Writer,
    limit: std.Io.Limit,
) std.Io.Reader.StreamError!usize {
    const reader: *AttrBulkReader = @alignCast(@fieldParentPtr("interface", io_reader));

    const dest = limit.slice(try io_writer.writableSliceGreedy(1));
    if (dest.len != 0) {
        const written = try reader.performRead(dest);
        io_writer.advance(written);
        return written;
    }

    const buffer_tail = io_reader.buffer[io_reader.end..];
    if (buffer_tail.len == 0) return 0;

    const written = reader.performRead(buffer_tail) catch |err| switch (err) {
        error.EndOfStream => return error.EndOfStream,
        error.ReadFailed => return error.ReadFailed,
    };
    io_reader.end += written;
    return 0;
}

fn performRead(
    self: *AttrBulkReader,
    dest: []u8,
) std.Io.Reader.StreamError!usize {
    var al = mac.AttrList{
        .bitmapcount = mac.ATTR_BIT_MAP_COUNT,
        .reserved = 0,
        .attrs = self.state.mask,
    };

    const rc = mac.getattrlistbulk(self.state.fd, &al, dest.ptr, dest.len, self.state.options);
    if (rc < 0) {
        return self.handleError(rc);
    }

    if (rc == 0) {
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        return error.EndOfStream;
    }

    const positive = std.math.absInt(@as(i64, rc)) catch return self.fail(error.ReadFailed);
    const entry_count = std.math.cast(usize, positive) orelse return self.fail(error.ReadFailed);
    const byte_len = computeBatchBytes(dest, entry_count) catch {
        self.state.last_entries = 0;
        self.state.last_bytes = 0;
        return self.fail(error.ReadFailed);
    };

    self.state.last_entries = entry_count;
    self.state.last_bytes = byte_len;
    self.state.last_error = null;
    return byte_len;
}

fn handleError(self: *AttrBulkReader, rc: anytype) std.Io.Reader.StreamError!usize {
    const err = posix.errno(rc);
    switch (err) {
        .INTR, .AGAIN => {
            self.state.last_entries = 0;
            self.state.last_bytes = 0;
            return 0;
        },
        .NOENT => unreachable,
        .NOTDIR => return self.fail(error.NotDir),
        .BADF => return self.fail(error.BadFileDescriptor),
        .ACCES => return self.fail(error.PermissionDenied),
        .FAULT => return self.fail(error.BadAddress),
        .RANGE => return self.fail(error.BufferTooSmall),
        .INVAL => return self.fail(error.InvalidArgument),
        .IO => return self.fail(error.ReadFailed),
        .TIMEDOUT => return self.fail(error.TimedOut),
        .DEADLK => return self.fail(error.DeadLock),
        else => |unexpected| {
            std.debug.panic("unexpected errno {s}", .{@tagName(unexpected)});
        },
    }
}

fn fail(self: *AttrBulkReader, err: anyerror) std.Io.Reader.StreamError!usize {
    self.state.last_entries = 0;
    self.state.last_bytes = 0;
    self.state.last_error = err;
    return error.ReadFailed;
}

fn computeBatchBytes(dest: []const u8, entry_count: usize) error{ReadFailed}!usize {
    var offset: usize = 0;
    var remaining = entry_count;
    while (remaining != 0) : (remaining -= 1) {
        if (offset + @sizeOf(u32) > dest.len) return error.ReadFailed;
        const reclen = std.mem.readIntLittle(u32, dest[offset .. offset + @sizeOf(u32)]);
        const len = std.math.cast(usize, reclen) orelse return error.ReadFailed;
        offset += len;
        if (offset > dest.len) return error.ReadFailed;
    }
    return offset;
}
