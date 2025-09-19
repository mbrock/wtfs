const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

// ======== Darwin constants we need (from <sys/attr.h> / <sys/vnode.h>) ========

const ATTR_BIT_MAP_COUNT: u16 = 5;

const CommonAttrMask = packed struct(u32) {
    name: bool = false,
    pad0: u1 = 0,
    fsid: bool = false,
    obj_type: bool = false,
    pad1: u21 = 0,
    file_id: bool = false,
    pad2: u5 = 0,
    returned_attrs: bool = true,
};

const DirAttrMask = packed struct(u32) {
    linkcount: bool = false,
    entrycount: bool = false,
    mountstatus: bool = false,
    allocsize: bool = false,
    ioblocksize: bool = false,
    datalength: bool = false,
    pad0: u26 = 0,
};

const FileAttrMask = packed struct(u32) {
    linkcount: bool = false,
    totalsize: bool = false,
    allocsize: bool = false,
    pad0: u29 = 0,
};

const AttrGroupMask = packed struct(u160) {
    common: CommonAttrMask = .{},
    vol: u32 = 0,
    dir: DirAttrMask = .{},
    file: FileAttrMask = .{},
    fork: u32 = 0,
};

const FsOptMask = packed struct(u32) {
    nofollow: bool = false,
    pad0: u1 = 0,
    report_fullsize: bool = false,
    pack_invalid_attrs: bool = false,
    pad1: u28 = 0,
};

inline fn maskValue(mask: anytype) u32 {
    return @as(u32, @bitCast(mask));
}

// vnode (object) types (from <sys/vnode.h>)
const VNON: u32 = 0;
const VREG: u32 = 1;
const VDIR: u32 = 2;
const VBLK: u32 = 3;
const VCHR: u32 = 4;
const VLNK: u32 = 5;
const VSOCK: u32 = 6;
const VFIFO: u32 = 7;
const VBAD: u32 = 8;
// (others exist; we only need the basics)

const AttrList = packed struct {
    bitmapcount: u16,
    reserved: u16,
    attrs: AttrGroupMask,
};

const AttributeSet = packed struct {
    attrs: AttrGroupMask,
};

const AttrRef = packed struct {
    off: i32,
    len: u32,
};

const Fsid = packed struct { id0: i32, id1: i32 };

fn DirPayloadFor(mask: DirAttrMask) type {
    return packed struct {
        linkcount: if (mask.linkcount) u32 else void,
        entrycount: if (mask.entrycount) u32 else void,
        mountstatus: if (mask.mountstatus) u32 else void,
        allocsize: if (mask.allocsize) u64 else void,
        ioblocksize: if (mask.ioblocksize) u32 else void,
        datalength: if (mask.datalength) u64 else void,
    };
}

fn FilePayloadFor(mask: FileAttrMask) type {
    return packed struct {
        linkcount: if (mask.linkcount) u32 else void,
        totalsize: if (mask.totalsize) u64 else void,
        allocsize: if (mask.allocsize) u64 else void,
    };
}

/// Generates a packed struct type for the payload returned by getattrlistbulk
/// based on the requested attributes in the mask
pub fn PayloadFor(mask: AttrGroupMask) type {
    return packed struct {
        len: u32,
        returned: AttributeSet,
        name_ref: if (mask.common.name) AttrRef else void,
        fsid: if (mask.common.fsid) Fsid else void,
        objtype: if (mask.common.obj_type) u32 else void,
        fileid: if (mask.common.file_id) u64 else void,
    };
}

extern "c" fn getattrlistbulk(
    dirfd: std.posix.fd_t,
    alist: *const AttrList,
    attrbuf: *anyopaque,
    buflen: usize,
    options: FsOptMask,
) c_int;

/// Represents the type of a file system object
pub const Kind = enum { file, dir, symlink, other };

/// Generates a struct type representing a directory entry with fields
/// populated based on the requested attributes in the mask
pub fn EntryFor(mask: AttrGroupMask) type {
    return struct {
        name: if (mask.common.name) [:0]const u8 else void,
        kind: if (mask.common.obj_type) Kind else void,
        fsid: if (mask.common.fsid) Fsid else void,
        fileid: if (mask.common.file_id) u64 else void,
        details: union(enum) {
            dir: struct {
                linkcount: if (mask.dir.linkcount) u32 else void,
                entrycount: if (mask.dir.entrycount) u32 else void,
                mountstatus: if (mask.dir.mountstatus) u32 else void,
                allocsize: if (mask.dir.allocsize) u64 else void,
                ioblocksize: if (mask.dir.ioblocksize) u32 else void,
                datalength: if (mask.dir.datalength) u64 else void,
            },
            file: struct {
                linkcount: if (mask.file.linkcount) u32 else void,
                totalsize: if (mask.file.totalsize) u64 else void,
                allocsize: if (mask.file.allocsize) u64 else void,
            },
            other: void,
        },
    };
}

/// Creates a directory scanner type that uses getattrlistbulk to efficiently
/// iterate over directory entries with the specified attributes
pub fn DirScanner(mask: AttrGroupMask) type {
    if (builtin.target.os.tag == .macos) {
        return struct {
            pub const Payload = PayloadFor(mask);
            pub const Entry = EntryFor(mask);

            fd: std.posix.fd_t,
            reader: std.io.Reader,
            buf: []u8,
            n: usize = 0,

            /// Initialize a new scanner with a directory file descriptor and buffer
            /// The buffer will be used for storing the bulk attribute results
            pub fn init(fd: std.posix.fd_t, buf: []u8) @This() {
                return .{
                    .fd = fd,
                    .reader = std.io.Reader.fixed(buf),
                    .buf = buf,
                };
            }

            fn pump(self: *@This()) !void {
                const opts_mask = FsOptMask{
                    .nofollow = true,
                    .report_fullsize = true,
                    .pack_invalid_attrs = true,
                };

                var al = AttrList{
                    .bitmapcount = ATTR_BIT_MAP_COUNT,
                    .reserved = 0,
                    .attrs = mask,
                };

                const n = getattrlistbulk(self.fd, &al, self.buf.ptr, self.buf.len, opts_mask);
                if (n < 0) {
                    switch (posix.errno(n)) {
                        .INTR, .AGAIN => {},
                        .NOENT => return error.FileNotFound,
                        .NOTDIR => return error.NotDir,
                        .BADF => return error.BadFileDescriptor,
                        .ACCES => return error.PermissionDenied,
                        .FAULT => return error.BadAddress,
                        .RANGE => return error.BufferTooSmall,
                        .INVAL => return error.InvalidArgument,
                        .IO => return error.ReadFailed,
                        .TIMEDOUT => return error.TimedOut,
                        else => |e| std.debug.panic("unexpected errno {t}", .{e}),
                    }
                }

                if (n == 0) return;

                self.n = @abs(n);

                self.reader = std.io.Reader.fixed(self.buf);
            }

            /// Get the next directory entry, or null if no more entries
            /// Automatically fetches more entries from the kernel when needed
            pub fn next(self: *@This()) !?Entry {
                if (self.n == 0) {
                    try self.pump();
                    if (self.n == 0) return null;
                }

                const reclen = try self.reader.peekInt(u32, .little);
                const recbuf = try self.reader.peek(reclen);
                var rec = std.io.Reader.fixed(recbuf);
                try self.reader.discardAll(@as(usize, reclen));
                self.n -= 1;

                // Take a pointer, so we can find the name slice by pointer arithmetic.
                // If we use takeStruct, we wouldn't have the buffer address of name_ref.
                // We can't use takeStructPointer because @SizeOf(Payload) is aligned
                // even though it's packed, so we would advance too far.
                // So we peekStructPointer and then advance with takeStruct, which works.

                const payload = try rec.peekStructPointer(Payload);
                _ = try rec.takeStruct(Payload, .little);

                const namerefptr = @as([*]u8, @ptrCast(&payload.name_ref));
                const namestart = if (payload.name_ref.off < 0)
                    namerefptr - @abs(payload.name_ref.off)
                else
                    namerefptr + @abs(payload.name_ref.off);
                const name = namestart[0 .. payload.name_ref.len - 1 :0];

                var entry = Entry{
                    .name = name,
                    .kind = switch (payload.objtype) {
                        VDIR => .dir,
                        VREG => .file,
                        VLNK => .symlink,
                        else => .other,
                    },
                    .fsid = payload.fsid,
                    .fileid = payload.fileid,
                    .details = undefined,
                };

                switch (payload.objtype) {
                    VDIR => {
                        const dir = try rec.takeStruct(DirPayloadFor(mask.dir), .little);
                        entry.details = .{
                            .dir = .{
                                .linkcount = dir.linkcount,
                                .entrycount = dir.entrycount,
                                .mountstatus = dir.mountstatus,
                                .allocsize = dir.allocsize,
                                .ioblocksize = dir.ioblocksize,
                                .datalength = dir.datalength,
                            },
                        };
                    },
                    VREG => {
                        const file = try rec.takeStruct(FilePayloadFor(mask.file), .little);
                        entry.details = .{
                            .file = .{
                                .linkcount = file.linkcount,
                                .totalsize = file.totalsize,
                                .allocsize = file.allocsize,
                            },
                        };
                    },
                    else => {},
                }

                return entry;
            }
        };
    } else {
        return struct {
            comptime {
                if (!(mask.common.name and mask.common.obj_type and mask.dir.datalength and mask.file.totalsize)) {
                    @compileError("fallback DirScanner only supports the mask used by main.zig");
                }
                if (mask.common.fsid or mask.common.file_id or mask.dir.linkcount or mask.dir.entrycount or mask.dir.mountstatus or mask.dir.allocsize or mask.dir.ioblocksize or mask.file.linkcount or mask.file.allocsize) {
                    @compileError("fallback DirScanner only supports the mask used by main.zig");
                }
            }

            pub const Payload = PayloadFor(mask);
            pub const Entry = struct {
                name: [:0]const u8,
                kind: Kind,
                details: union(enum) {
                    dir: struct { datalength: u64 },
                    file: struct { totalsize: u64 },
                    other: void,
                },
            };

            dir: std.fs.Dir,
            iterator: std.fs.Dir.Iterator,
            buf: []u8,

            /// Initialize a new scanner with a directory file descriptor and buffer.
            pub fn init(fd: std.posix.fd_t, buf: []u8) @This() {
                var scanner = @This(){
                    .dir = .{ .fd = fd },
                    .iterator = undefined,
                    .buf = buf,
                };
                scanner.iterator = scanner.dir.iterateAssumeFirstIteration();
                return scanner;
            }

            /// Get the next directory entry, or null if no more entries.
            pub fn next(self: *@This()) !?Entry {
                while (try self.iterator.next()) |dir_entry| {
                    const name = dir_entry.name;
                    if (name.len + 1 > self.buf.len) {
                        return error.BufferTooSmall;
                    }

                    const dest = self.buf[0 .. name.len + 1];
                    std.mem.copyForwards(u8, dest[0..name.len], name);
                    dest[name.len] = 0;

                    const entry_kind: Kind = switch (dir_entry.kind) {
                        .directory => .dir,
                        .file => .file,
                        .sym_link => .symlink,
                        else => .other,
                    };

                    var entry = Entry{
                        .name = dest[0..name.len :0],
                        .kind = entry_kind,
                        .details = .{ .other = {} },
                    };

                    switch (entry_kind) {
                        .dir => {
                            const stat = try self.dir.statFile(name);
                            entry.details = .{ .dir = .{ .datalength = stat.size } };
                        },
                        .file => {
                            const stat = try self.dir.statFile(name);
                            entry.details = .{ .file = .{ .totalsize = stat.size } };
                        },
                        else => {},
                    }

                    return entry;
                }

                return null;
            }
        };
    }
}
