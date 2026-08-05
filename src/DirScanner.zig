// DirScanner handles per-platform directory iteration. macOS gets the
// getattrlistbulk fast path; other OSes presently fall back to readdir + stat.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const SysDispatcher = @import("SysDispatcher.zig");

// ===== Darwin/macOS System Constants =====
// From <sys/attr.h> and <sys/vnode.h>

pub const ATTR_BIT_MAP_COUNT: u16 = 5;

// Vnode (file system object) types from <sys/vnode.h>
const VNON: u32 = 0; // No type
const VREG: u32 = 1; // Regular file
const VDIR: u32 = 2; // Directory
const VBLK: u32 = 3; // Block device
const VCHR: u32 = 4; // Character device
const VLNK: u32 = 5; // Symbolic link
const VSOCK: u32 = 6; // Socket
const VFIFO: u32 = 7; // FIFO/pipe
const VBAD: u32 = 8; // Bad/invalid

// ===== Attribute Masks =====
// These control which attributes getattrlistbulk will return

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
    returned_attrs: bool = true, // Always request this
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

pub const FsOptMask = packed struct(u32) {
    nofollow: bool = false,
    pad0: u1 = 0,
    report_fullsize: bool = false,
    pack_invalid_attrs: bool = false,
    pad1: u28 = 0,
};

// ===== System Types =====

pub const AttrList = packed struct {
    bitmapcount: u16,
    reserved: u16,
    attrs: AttrGroupMask,
};

pub const AttributeSet = packed struct {
    attrs: AttrGroupMask,
};

const AttrRef = packed struct {
    off: i32,
    len: u32,
};

const Fsid = packed struct { id0: i32, id1: i32 };

// ===== I/O Policy API (macOS-specific) =====

/// Types of I/O policies that can be set
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

/// Scope of I/O policy application
pub const IoPolicyScope = enum(c_int) {
    process = 0,
    thread = 1,
    darwin_bg = 2,
};

/// `std.posix` no longer wraps `dup`, and the scanner needs a second fd so the
/// context can close the original while iteration is still in flight.
pub fn dupFd(fd: std.posix.fd_t) std.posix.UnexpectedError!std.posix.fd_t {
    const rc = std.c.dup(fd);
    if (rc >= 0) return rc;
    return std.posix.unexpectedErrno(std.posix.errno(rc));
}

/// Set an I/O policy for the current process or thread
pub extern "c" fn setiopolicy_np(
    iotype: IoPolicyType,
    scope: IoPolicyScope,
    policy: c_int,
) c_int;

/// Get the current I/O policy for the process or thread
pub extern "c" fn getiopolicy_np(
    iotype: IoPolicyType,
    scope: IoPolicyScope,
) c_int;

// ===== System Calls =====

pub extern "c" fn getattrlistbulk(
    dirfd: std.posix.fd_t,
    alist: *const AttrList,
    attrbuf: *anyopaque,
    buflen: usize,
    options: FsOptMask,
) c_int;

// ===== Public API =====

/// Represents the type of a file system object
pub const Kind = enum { file, dir, symlink, other };

// ===== Type Generation Helpers =====

/// Generate the payload struct for directory attributes
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

/// Generate the payload struct for file attributes
fn FilePayloadFor(mask: FileAttrMask) type {
    return packed struct {
        linkcount: if (mask.linkcount) u32 else void,
        totalsize: if (mask.totalsize) u64 else void,
        allocsize: if (mask.allocsize) u64 else void,
    };
}

/// Generate the payload struct returned by getattrlistbulk
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

/// Generate a directory entry struct with fields based on requested attributes
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

/// Read a packed struct using its exact bit width.
///
/// `Reader.takeStruct` advances by `@sizeOf`, which for a packed struct rounds
/// up to the type's alignment — 48 bytes for a 36-byte `Payload`, for instance.
/// The kernel packs `getattrlistbulk` records with no such padding, so reading
/// by `@sizeOf` would desynchronize everything after the first field group.
fn takePacked(r: *std.Io.Reader, comptime T: type) !T {
    const n = comptime @divExact(@bitSizeOf(T), 8);
    return @bitCast((try r.takeArray(n)).*);
}

// ===== Directory Scanner =====

/// Creates a directory scanner type that efficiently iterates over directory entries
/// On macOS: Uses getattrlistbulk for batched, high-performance scanning
/// On other platforms: Falls back to standard POSIX directory iteration
const DefaultStatProvider = struct {
    pub const ContextType = void;

    pub fn statAt(_: ContextType, dir_fd: std.posix.fd_t, name: [:0]const u8) SysDispatcher.StatError!SysDispatcher.Stat {
        return SysDispatcher.statPath(dir_fd, name);
    }
};

pub const DispatcherStatProvider = struct {
    pub const ContextType = *SysDispatcher.Backend;

    pub fn statAt(dispatcher: ContextType, dir_fd: std.posix.fd_t, name: [:0]const u8) SysDispatcher.StatError!SysDispatcher.Stat {
        return dispatcher.statAt(dir_fd, name);
    }
};

pub fn DirScanner(mask: AttrGroupMask) type {
    return DirScannerWithProvider(mask, DefaultStatProvider);
}

pub fn DirScannerWithProvider(mask: AttrGroupMask, comptime Provider: type) type {
    if (builtin.target.os.tag == .macos) {
        return MacOSDirScanner(mask, Provider);
    } else {
        return PosixDirScanner(mask, Provider);
    }
}

// ===== macOS Implementation =====

fn MacOSDirScanner(mask: AttrGroupMask, comptime Provider: type) type {
    return struct {
        pub const Payload = PayloadFor(mask);
        pub const Entry = EntryFor(mask);
        pub const Mask = mask;

        io: std.Io,
        fd: std.posix.fd_t,
        dir: std.Io.Dir,
        reader: std.Io.Reader,
        buf: []u8,
        provider_ctx: Provider.ContextType,
        n: usize = 0, // Number of entries in current batch

        /// Initialize a new scanner with a directory file descriptor and buffer
        /// The buffer will be used for storing the bulk attribute results
        pub fn init(io: std.Io, fd: std.posix.fd_t, buf: []u8, provider_ctx: Provider.ContextType) @This() {
            return .{
                .io = io,
                .fd = fd,
                .dir = .{ .handle = fd },
                .reader = std.Io.Reader.fixed(buf),
                .buf = buf,
                .provider_ctx = provider_ctx,
            };
        }

        /// Fetch a new batch of entries from the kernel
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

            while (true) {
                const n = getattrlistbulk(self.fd, &al, self.buf.ptr, self.buf.len, opts_mask);
                if (n < 0) {
                    switch (posix.errno(n)) {
                        .INTR, .AGAIN => return error.Interrupted,
                        .NOENT => unreachable,
                        .NOTDIR => return error.NotDir,
                        .BADF => return error.BadFileDescriptor,
                        .ACCES => return error.PermissionDenied,
                        .FAULT => return error.BadAddress,
                        .RANGE => return error.BufferTooSmall,
                        .INVAL => return error.InvalidArgument,
                        .IO => return error.ReadFailed,
                        .TIMEDOUT => return error.TimedOut,
                        .DEADLK => return error.DeadLock, // iCloud dataless file
                        else => |e| std.debug.panic("unexpected errno {t}", .{e}),
                    }
                }

                self.n = @abs(n);
                self.reader = std.Io.Reader.fixed(self.buf);
                return;
            }
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

            // Read record length and prepare reader for this record
            const reclen = try self.reader.peekInt(u32, .little);
            const recbuf = try self.reader.peek(reclen);
            var rec = std.Io.Reader.fixed(recbuf);
            try self.reader.discardAll(@as(usize, reclen));
            self.n -= 1;

            // Parse the payload structure
            const payload = try takePacked(&rec, Payload);

            // An `attrreference_t` offset is relative to the address of the
            // reference itself, which sits at a fixed offset into the record.
            const nameref_offset = comptime @divExact(@bitOffsetOf(Payload, "name_ref"), 8);
            const namerefptr = recbuf.ptr + nameref_offset;
            const namestart = if (payload.name_ref.off < 0)
                namerefptr - @abs(payload.name_ref.off)
            else
                namerefptr + @abs(payload.name_ref.off);
            const name = namestart[0 .. payload.name_ref.len - 1 :0];

            // Build the entry
            var entry = Entry{
                .name = name,
                .kind = vnodeTypeToKind(payload.objtype),
                .fsid = payload.fsid,
                .objid = payload.objid,
                .details = undefined,
            };

            // Parse type-specific attributes
            switch (payload.objtype) {
                VDIR => {
                    const dir = try takePacked(&rec, DirPayloadFor(mask.dir));
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
                    const file = try takePacked(&rec, FilePayloadFor(mask.file));
                    entry.details = .{
                        .file = .{
                            .linkcount = file.linkcount,
                            .totalsize = file.totalsize,
                            .allocsize = file.allocsize,
                        },
                    };
                },
                else => entry.details = .{ .other = {} },
            }

            return entry;
        }

        fn vnodeTypeToKind(vtype: u32) Kind {
            return switch (vtype) {
                VDIR => .dir,
                VREG => .file,
                VLNK => .symlink,
                else => .other,
            };
        }
    };
}

// ===== POSIX Fallback Implementation =====

fn PosixDirScanner(mask: AttrGroupMask, comptime Provider: type) type {
    return struct {
        pub const Payload = PayloadFor(mask);
        pub const Entry = EntryFor(mask);
        const EntryDetails = @FieldType(Entry, "details");
        const DirPayload = @FieldType(EntryDetails, "dir");
        const FilePayload = @FieldType(EntryDetails, "file");

        io: std.Io,
        dir: std.Io.Dir,
        iterator: std.Io.Dir.Iterator,
        buf: []u8,
        provider_ctx: Provider.ContextType,
        pending_entry: ?Entry = null,

        /// Initialize a new scanner with a directory file descriptor and buffer
        pub fn init(io: std.Io, fd: std.posix.fd_t, buf: []u8, provider_ctx: Provider.ContextType) @This() {
            var scanner = @This(){
                .io = io,
                .dir = .{ .handle = fd },
                .iterator = undefined,
                .buf = buf,
                .provider_ctx = provider_ctx,
            };
            scanner.iterator = scanner.dir.iterateAssumeFirstIteration();
            return scanner;
        }

        /// Copy a name into our buffer and null-terminate it
        fn copyName(self: *@This(), name: []const u8) error{BufferTooSmall}![:0]const u8 {
            if (name.len + 1 > self.buf.len) {
                return error.BufferTooSmall;
            }

            const dest = self.buf[0 .. name.len + 1];
            std.mem.copyForwards(u8, dest[0..name.len], name);
            dest[name.len] = 0;
            return dest[0..name.len :0];
        }

        /// Stat a file relative to our directory
        fn statAt(self: *@This(), name: [:0]const u8) !SysDispatcher.Stat {
            return try Provider.statAt(self.provider_ctx, self.dir.handle, name);
        }

        /// Fetch the next directory entry from the iterator
        fn fetchNext(self: *@This()) !?Entry {
            const dir_entry = (try self.iterator.next(self.io)) orelse return null;

            const raw_name = dir_entry.name;
            const name_z = try self.copyName(raw_name);

            const entry_kind: Kind = switch (dir_entry.kind) {
                .directory => .dir,
                .file => .file,
                .sym_link => .symlink,
                else => .other,
            };

            // Build the entry with requested attributes
            var entry: Entry = undefined;
            if (comptime mask.common.name) entry.name = name_z;
            if (comptime mask.common.objtype) entry.kind = entry_kind;
            if (comptime mask.common.fsid) entry.fsid = .{ .id0 = 0, .id1 = 0 };
            if (comptime mask.common.objid) entry.objid = 0;

            // Determine if we need to stat for additional attributes
            const needs_dir_stat = comptime mask.dir.linkcount or mask.dir.allocsize or
                mask.dir.ioblocksize or mask.dir.datalength;
            const needs_file_stat = comptime mask.file.linkcount or mask.file.totalsize or
                mask.file.allocsize;

            // Fill in type-specific details
            entry.details = switch (entry_kind) {
                .dir => blk: {
                    var payload: DirPayload = std.mem.zeroes(DirPayload);
                    if (needs_dir_stat) {
                        const stat = try self.statAt(name_z);
                        if (comptime mask.dir.linkcount) {
                            payload.linkcount = @intCast(stat.nlink);
                        }
                        if (comptime mask.dir.allocsize) {
                            payload.allocsize = stat.blocks * 512;
                        }
                        if (comptime mask.dir.ioblocksize) {
                            payload.ioblocksize = @intCast(stat.blksize);
                        }
                        if (comptime mask.dir.datalength) {
                            payload.datalength = stat.size;
                        }
                    }
                    // These aren't available via standard POSIX APIs
                    if (comptime mask.dir.entrycount) payload.entrycount = 0;
                    if (comptime mask.dir.mountstatus) payload.mountstatus = 0;
                    break :blk .{ .dir = payload };
                },
                .file => blk: {
                    var payload: FilePayload = std.mem.zeroes(FilePayload);
                    if (needs_file_stat) {
                        const stat = try self.statAt(name_z);
                        if (comptime mask.file.linkcount) {
                            payload.linkcount = @intCast(stat.nlink);
                        }
                        if (comptime mask.file.totalsize) {
                            payload.totalsize = stat.size;
                        }
                        if (comptime mask.file.allocsize) {
                            payload.allocsize = stat.blocks * 512;
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

        /// Retrieve the next entry, fetching from the iterator when needed.
        pub fn next(self: *@This()) !?Entry {
            if (self.pending_entry == null) {
                self.pending_entry = try self.fetchNext();
            }

            const entry = self.pending_entry orelse return null;
            self.pending_entry = null;
            return entry;
        }
    };
}

test "POSIX DirScanner iterates entries with requested metadata" {
    if (builtin.target.os.tag == .macos) return error.SkipZigTest;
    if (builtin.target.os.tag == .windows) return error.SkipZigTest;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("subdir");
    try tmp.dir.writeFile(.{ .sub_path = "subdir/nested.txt", .data = "nested" });
    try tmp.dir.writeFile(.{ .sub_path = "file.txt", .data = "abc" });

    const file_stat = try tmp.dir.statFile("file.txt");
    const dir_stat = try tmp.dir.statFile("subdir");

    const mask = AttrGroupMask{
        .common = .{ .name = true, .objtype = true },
        .dir = .{ .datalength = true },
        .file = .{ .totalsize = true },
    };

    const Scanner = DirScanner(mask);
    var name_buf: [256]u8 = undefined;
    var iterable_dir = try tmp.dir.openDir(std.testing.io, ".", .{ .iterate = true });
    const dup_fd = try dupFd(iterable_dir.handle);
    iterable_dir.close(std.testing.io);
    var scanner = Scanner.init(std.testing.io, dup_fd, name_buf[0..], @as(void, {}));
    defer scanner.dir.close(std.testing.io);

    var saw_file = false;
    var saw_dir = false;
    while (true) {
        const entry = (try scanner.next()) orelse break;

        if (std.mem.eql(u8, entry.name, "file.txt")) {
            try std.testing.expect(!saw_file);
            saw_file = true;
            try std.testing.expectEqual(Kind.file, entry.kind);
            try std.testing.expectEqual(@as(u64, file_stat.size), entry.details.file.totalsize);
        } else if (std.mem.eql(u8, entry.name, "subdir")) {
            try std.testing.expect(!saw_dir);
            saw_dir = true;
            try std.testing.expectEqual(Kind.dir, entry.kind);
            try std.testing.expectEqual(@as(u64, dir_stat.size), entry.details.dir.datalength);
        } else {
            // No other entries were created in the temporary directory
            try std.testing.expect(false);
        }
    }

    try std.testing.expect(saw_file);
    try std.testing.expect(saw_dir);
}

test "POSIX DirScanner copyName reports buffer exhaustion" {
    if (builtin.target.os.tag == .macos) return error.SkipZigTest;
    if (builtin.target.os.tag == .windows) return error.SkipZigTest;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const mask = AttrGroupMask{
        .common = .{ .name = true },
    };
    const Scanner = DirScanner(mask);

    var tiny_buf: [3]u8 = undefined;
    var iterable_dir = try tmp.dir.openDir(std.testing.io, ".", .{ .iterate = true });
    const dup_fd = try dupFd(iterable_dir.handle);
    iterable_dir.close(std.testing.io);
    var scanner = Scanner.init(std.testing.io, dup_fd, tiny_buf[0..], @as(void, {}));
    defer scanner.dir.close(std.testing.io);

    try std.testing.expectError(error.BufferTooSmall, scanner.copyName("long"));
    const short_name = try scanner.copyName("ok");
    try std.testing.expectEqualSlices(u8, "ok", std.mem.sliceTo(short_name, 0));
}
