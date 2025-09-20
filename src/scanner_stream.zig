const std = @import("std");
const wtfs = @import("wtfs").mac;

const AttrGroupMask = wtfs.AttrGroupMask;
const FsOptMask = wtfs.FsOptMask;
const AttrList = wtfs.AttrList;

pub const default_options = FsOptMask{
    .nofollow = true,
    .report_fullsize = true,
    .pack_invalid_attrs = true,
};

pub const DirContext = packed struct {
    tag: u8 = 1,
    ver: u8 = 0,
    _pad: u16 = 0,
    dir_ix: u64 = 0,
    parent: u64 = 0,
    baseix: u32 = 0,
    flags: u32 = 0,
};

pub const RawBatch = packed struct {
    tag: u8 = 2,
    ver: u8 = 0,
    _pad: u16 = 0,
    len: u32 = 0,
};

pub const AttrBulkReader = struct {
    reader: std.Io.Reader,
    dirfd: std.posix.fd_t,
    mask: AttrGroupMask,
    options: FsOptMask = default_options,
    last_entries: usize = 0,
    last_bytes: usize = 0,
    last_error: ?anyerror = null,

    pub const Error = error{
        NotDir,
        BadFileDescriptor,
        PermissionDenied,
        BadAddress,
        BufferTooSmall,
        InvalidArgument,
        ReadFailed,
        TimedOut,
        DeadLock,
    };

    pub const Batch = struct {
        byte_len: usize,
        entry_count: usize,
    };

    pub fn init(dirfd: std.posix.fd_t, mask: AttrGroupMask, buffer: []u8) AttrBulkReader {
        return .{
            .reader = .{
                .vtable = &.{ .stream = stream },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .dirfd = dirfd,
            .mask = mask,
            .options = default_options,
        };
    }

    pub fn prepare(self: *AttrBulkReader) void {
        self.reader.seek = 0;
        self.reader.end = 0;
        self.last_entries = 0;
        self.last_bytes = 0;
        self.last_error = null;
    }

    pub fn loadNext(self: *AttrBulkReader) Error!?Batch {
        self.prepare();

        std.Io.Reader.fillMore(&self.reader) catch |err| switch (err) {
            error.EndOfStream => return null,
            error.ReadFailed => return self.last_error orelse error.ReadFailed,
        };

        if (self.last_entries == 0 or self.last_bytes == 0) {
            return error.ReadFailed;
        }

        return Batch{
            .byte_len = self.last_bytes,
            .entry_count = self.last_entries,
        };
    }

    pub fn bytes(self: *const AttrBulkReader) []const u8 {
        return self.reader.buffer[0..self.last_bytes];
    }

    fn selfFromReader(r: *std.Io.Reader) *AttrBulkReader {
        return @as(*AttrBulkReader, @fieldParentPtr("reader", r));
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.StreamError!usize {
        const self = selfFromReader(r);
        const dest_full = limit.slice(try w.writableSliceGreedy(1));
        if (dest_full.len == 0) return 0;

        while (true) {
            const rc = wtfs.getattrlistbulk(self.dirfd, &AttrList{
                .bitmapcount = wtfs.ATTR_BIT_MAP_COUNT,
                .reserved = 0,
                .attrs = self.mask,
            }, dest_full.ptr, dest_full.len, self.options);

            if (rc < 0) {
                switch (std.posix.errno(rc)) {
                    .INTR, .AGAIN => continue,
                    .NOENT => unreachable,
                    .NOTDIR => {
                        self.last_error = error.NotDir;
                        return error.ReadFailed;
                    },
                    .BADF => {
                        self.last_error = error.BadFileDescriptor;
                        return error.ReadFailed;
                    },
                    .ACCES => {
                        self.last_error = error.PermissionDenied;
                        return error.ReadFailed;
                    },
                    .FAULT => {
                        self.last_error = error.BadAddress;
                        return error.ReadFailed;
                    },
                    .RANGE => {
                        self.last_error = error.BufferTooSmall;
                        return error.ReadFailed;
                    },
                    .INVAL => {
                        self.last_error = error.InvalidArgument;
                        return error.ReadFailed;
                    },
                    .IO => {
                        self.last_error = error.ReadFailed;
                        return error.ReadFailed;
                    },
                    .TIMEDOUT => {
                        self.last_error = error.TimedOut;
                        return error.ReadFailed;
                    },
                    .DEADLK => {
                        self.last_error = error.DeadLock;
                        return error.ReadFailed;
                    },
                    else => |e| std.debug.panic("unexpected errno {t}", .{e}),
                }
            }

            if (rc == 0) return error.EndOfStream;

            const entry_count = @as(usize, @intCast(@abs(rc)));
            const batch_len = batchLength(dest_full, entry_count) catch {
                self.last_error = error.ReadFailed;
                return error.ReadFailed;
            };

            self.last_entries = entry_count;
            self.last_bytes = batch_len;
            self.last_error = null;
            w.advance(batch_len);
            return batch_len;
        }
    }

    fn batchLength(buf: []const u8, entry_count: usize) Error!usize {
        var offset: usize = 0;
        var i: usize = 0;
        while (i < entry_count) : (i += 1) {
            if (offset + 4 > buf.len) return error.ReadFailed;
            const reclen = std.mem.readIntLittle(u32, buf[offset .. offset + 4]);
            const end = offset + @as(usize, reclen);
            if (end > buf.len) return error.ReadFailed;
            if (reclen == 0) return error.ReadFailed;
            offset = end;
        }
        return offset;
    }
};

pub fn makeAttrBulkReader(dirfd: std.posix.fd_t, mask: AttrGroupMask, buffer: []u8) AttrBulkReader {
    return AttrBulkReader.init(dirfd, mask, buffer);
}

pub fn writeDirContext(w: *std.Io.Writer, ctx: DirContext) !void {
    var buf: [@sizeOf(DirContext)]u8 = undefined;
    buf[0] = ctx.tag;
    buf[1] = ctx.ver;
    std.mem.writeIntLittle(u16, buf[2..4], ctx._pad);
    std.mem.writeIntLittle(u64, buf[4..12], ctx.dir_ix);
    std.mem.writeIntLittle(u64, buf[12..20], ctx.parent);
    std.mem.writeIntLittle(u32, buf[20..24], ctx.baseix);
    std.mem.writeIntLittle(u32, buf[24..28], ctx.flags);
    try w.writeAll(&buf);
}

pub fn writeRawBatch(w: *std.Io.Writer, payload: []const u8) (PayloadTooLarge || std.Io.Writer.Error)!void {
    if (payload.len > std.math.maxInt(u32)) return error.PayloadTooLarge;

    var header: [@sizeOf(RawBatch)]u8 = undefined;
    header[0] = 2;
    header[1] = 0;
    std.mem.writeIntLittle(u16, header[2..4], 0);
    std.mem.writeIntLittle(u32, header[4..8], @intCast(payload.len));

    try w.writeAll(&header);
    try w.writeAll(payload);
}

pub const PayloadTooLarge = error{PayloadTooLarge};
