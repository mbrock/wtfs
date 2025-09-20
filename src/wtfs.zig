const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

// ======== Darwin constants we need (from <sys/attr.h> / <sys/vnode.h>) ========

const ATTR_BIT_MAP_COUNT: u16 = 5;

const CommonAttrMask = packed struct(u32) {
    name: bool = false,
    devid: bool = false,
    fsid: bool = false,
    objtype: bool = false,
    objtag: bool = false,
    objid: bool = false,
    objpermanentid: bool = false,
    parobjid: bool = false,
    script: bool = false,
    crtime: bool = false,
    modtime: bool = false,
    chgtime: bool = false,
    acctime: bool = false,
    bkuptime: bool = false,
    fndrinfo: bool = false,
    ownerid: bool = false,
    groupid: bool = false,
    accessmask: bool = false,
    flags: bool = false,
    gen_count: bool = false,
    document_id: bool = false,
    useraccess: bool = false,
    extended_security: bool = false,
    uuid: bool = false,
    grpuuid: bool = false,
    fileid: bool = false,
    parentid: bool = false,
    fullpath: bool = false,
    addedtime: bool = false,
    @"error": bool = false,
    data_protect_flags: bool = false,
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
        objtype: if (mask.common.objtype) u32 else void,
        objid: if (mask.common.objid) u64 else void,
    };
}

extern "c" fn getattrlistbulk(
    dirfd: std.posix.fd_t,
    alist: *const AttrList,
    attrbuf: *anyopaque,
    buflen: usize,
    options: FsOptMask,
) c_int;

pub const IoPolicyType = enum(c_int) {
    disk = 0,
    vfs_atime_updates = 2,
    vfs_materialize_dataless_files = 3,
    vfs_statfs_no_data_volume = 4,
    vfs_trigger_resolve = 5,
    vfs_ignore_content_protection = 6,
    vfs_ignore_permissions = 7,
    vfs_skip_mtime_update = 8,
    vfs_allow_low_space_writes = 9,
    vfs_disallow_rw_for_o_evtonly = 10,
};

pub const IoPolicyScope = enum(c_int) {
    process = 0,
    thread = 1,
    darwin_bg = 2,
};

pub extern "c" fn setiopolicy_np(
    iotype: IoPolicyType,
    scope: IoPolicyScope,
    policy: c_int,
) c_int;

pub extern "c" fn getiopolicy_np(
    iotype: IoPolicyType,
    scope: IoPolicyScope,
) c_int;

pub extern "c" fn openat(
    dirfd: std.posix.fd_t,
    path: [*:0]const u8,
    flags: std.posix.O,
    mode: std.posix.mode_t,
) std.posix.fd_t;

pub fn openSubdirectory(
    parent_fd: std.posix.fd_t,
    name: [:0]const u8,
) !std.posix.fd_t {
    while (true) {
        const x = openat(parent_fd, name, .{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        }, 0);

        if (x < 0) {
            switch (posix.errno(x)) {
                .INTR => continue,
                .BADF => return error.BadFileDescriptor,
                .NOTDIR => return error.NotDir,
                .NOENT => return error.FileNotFound,
                .ACCES => return error.AccessDenied,
                .PERM => return error.PermissionDenied,
                .LOOP => return error.TooManySymlinks,
                .NAMETOOLONG => return error.NameTooLong,
                .IO => return error.IOError,
                else => |e| std.debug.panic("unexpected errno {t}", .{e}),
            }
        }

        return x;
    }
}

/// Represents the type of a file system object
pub const Kind = enum { file, dir, symlink, other };

/// Generates a struct type representing a directory entry with fields
/// populated based on the requested attributes in the mask
pub fn EntryFor(mask: AttrGroupMask) type {
    return struct {
        name: if (mask.common.name) [:0]const u8 else void,
        kind: if (mask.common.objtype) Kind else void,
        fsid: if (mask.common.fsid) Fsid else void,
        objid: if (mask.common.objid) u64 else void,
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

            fn refill(self: *@This()) !void {
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
                        .NOENT => unreachable,
                        .NOTDIR => return error.NotDir,
                        .BADF => return error.BadFileDescriptor,
                        .ACCES => return error.PermissionDenied,
                        .FAULT => return error.BadAddress,
                        .RANGE => return error.BufferTooSmall,
                        .INVAL => return error.InvalidArgument,
                        .IO => return error.ReadFailed,
                        .TIMEDOUT => return error.TimedOut,
                        .DEADLK => return error.DeadLock, // iCloud dataless
                        else => |e| std.debug.panic("unexpected errno {t}", .{e}),
                    }
                }

                if (n == 0) return;

                self.n = @abs(n);

                self.reader = std.io.Reader.fixed(self.buf);
            }

            /// Ensure a batch of entries is available. Returns false when no
            /// more entries can be read. This performs the kernel syscall, so
            /// callers should avoid holding contended locks when calling it.
            pub fn fill(self: *@This()) !bool {
                if (self.n == 0) {
                    try self.refill();
                }

                return self.n != 0;
            }

            /// Get the next entry from the current batch or null if the batch
            /// is exhausted. Errors encountered while decoding entry data are
            /// still surfaced here.
            pub fn next(self: *@This()) !?Entry {
                if (self.n == 0) return null;

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
                    .objid = payload.objid,
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
            pub const Payload = PayloadFor(mask);
            pub const Entry = EntryFor(mask);
            const EntryDetails = @FieldType(Entry, "details");
            const DirPayload = @FieldType(EntryDetails, "dir");
            const FilePayload = @FieldType(EntryDetails, "file");
            const stat_has_nlink = @hasField(posix.Stat, "nlink");
            const stat_has_blocks = @hasField(posix.Stat, "blocks");
            const stat_has_blksize = @hasField(posix.Stat, "blksize");

            dir: std.fs.Dir,
            iterator: std.fs.Dir.Iterator,
            buf: []u8,
            pending_entry: ?Entry = null,

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

            fn copyName(self: *@This(), name: []const u8) error{BufferTooSmall}![:0]const u8 {
                if (name.len + 1 > self.buf.len) {
                    return error.BufferTooSmall;
                }

                const dest = self.buf[0 .. name.len + 1];
                std.mem.copyForwards(u8, dest[0..name.len], name);
                dest[name.len] = 0;
                return dest[0..name.len :0];
            }

            fn statAt(self: *@This(), name: [:0]const u8) !posix.Stat {
                return try posix.fstatat(self.dir.fd, name, 0);
            }

            fn fetchNext(self: *@This()) !?Entry {
                const dir_entry = (try self.iterator.next()) orelse return null;

                const raw_name = dir_entry.name;
                const name_z = try self.copyName(raw_name);

                const entry_kind: Kind = switch (dir_entry.kind) {
                    .directory => .dir,
                    .file => .file,
                    .sym_link => .symlink,
                    else => .other,
                };

                var entry: Entry = undefined;
                if (comptime mask.common.name) entry.name = name_z;
                if (comptime mask.common.objtype) entry.kind = entry_kind;
                if (comptime mask.common.fsid) entry.fsid = .{ .id0 = 0, .id1 = 0 };
                if (comptime mask.common.objid) entry.objid = 0;

                const needs_dir_stat = comptime mask.dir.linkcount or mask.dir.allocsize or mask.dir.ioblocksize or mask.dir.datalength;
                const needs_file_stat = comptime mask.file.linkcount or mask.file.totalsize or mask.file.allocsize;

                entry.details = switch (entry_kind) {
                    .dir => blk: {
                        var payload: DirPayload = std.mem.zeroes(DirPayload);
                        if (needs_dir_stat) {
                            const stat = try self.statAt(name_z);
                            if (comptime mask.dir.linkcount) {
                                if (stat_has_nlink) {
                                    payload.linkcount = @intCast(stat.nlink);
                                } else {
                                    payload.linkcount = 0;
                                }
                            }
                            if (comptime mask.dir.allocsize) {
                                if (stat_has_blocks) {
                                    const blocks: u64 = @intCast(stat.blocks);
                                    payload.allocsize = blocks * 512;
                                } else {
                                    payload.allocsize = 0;
                                }
                            }
                            if (comptime mask.dir.ioblocksize) {
                                if (stat_has_blksize) {
                                    payload.ioblocksize = @intCast(stat.blksize);
                                } else {
                                    payload.ioblocksize = 0;
                                }
                            }
                            if (comptime mask.dir.datalength) payload.datalength = @intCast(stat.size);
                        }
                        if (comptime mask.dir.entrycount) payload.entrycount = 0;
                        if (comptime mask.dir.mountstatus) payload.mountstatus = 0;
                        break :blk .{ .dir = payload };
                    },
                    .file => blk: {
                        var payload: FilePayload = std.mem.zeroes(FilePayload);
                        if (needs_file_stat) {
                            const stat = try self.statAt(name_z);
                            if (comptime mask.file.linkcount) {
                                if (stat_has_nlink) {
                                    payload.linkcount = @intCast(stat.nlink);
                                } else {
                                    payload.linkcount = 0;
                                }
                            }
                            if (comptime mask.file.totalsize) payload.totalsize = @intCast(stat.size);
                            if (comptime mask.file.allocsize) {
                                if (stat_has_blocks) {
                                    const blocks: u64 = @intCast(stat.blocks);
                                    payload.allocsize = blocks * 512;
                                } else {
                                    payload.allocsize = @intCast(stat.size);
                                }
                            }
                        }
                        break :blk .{ .file = payload };
                    },
                    else => .{ .other = {} },
                };

                return entry;
            }

            /// Ensure at least one entry is available, returning false when the
            /// iterator is exhausted.
            pub fn fill(self: *@This()) !bool {
                if (self.pending_entry != null) return true;

                const entry = (try self.fetchNext()) orelse return false;
                self.pending_entry = entry;
                return true;
            }

            /// Retrieve the next entry from the current batch (one entry per
            /// fill on non-macOS platforms).
            pub fn next(self: *@This()) !?Entry {
                const entry = self.pending_entry orelse return null;
                self.pending_entry = null;
                return entry;
            }
        };
    }
}
