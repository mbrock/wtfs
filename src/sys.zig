//! Platform layer: open directories, list their entries with metadata, and
//! stat single paths. Linux uses getdents64 + statx through raw syscalls (no
//! libc, so the binary is fully static). macOS uses getattrlistbulk, which
//! returns names and attributes for a whole batch of entries per syscall.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const linux = std.os.linux;

const is_linux = builtin.os.tag == .linux;
const is_darwin = builtin.os.tag.isDarwin();

comptime {
    if (!is_linux and !is_darwin) @compileError("wtfs supports Linux and macOS");
}

pub const fd_t = posix.fd_t;

pub const Kind = enum(u8) {
    file,
    dir,
    symlink,
    fifo,
    socket,
    block,
    char,
    unknown,

    pub fn fromMode(mode: u32) Kind {
        return switch (mode & 0o170000) {
            0o100000 => .file,
            0o040000 => .dir,
            0o120000 => .symlink,
            0o010000 => .fifo,
            0o140000 => .socket,
            0o060000 => .block,
            0o020000 => .char,
            else => .unknown,
        };
    }
};

/// Metadata for one filesystem object, normalized across platforms.
pub const Stat = struct {
    kind: Kind,
    /// Permission bits (mode & 0o7777).
    perm: u16 = 0,
    nlink: u32 = 0,
    uid: u32 = 0,
    gid: u32 = 0,
    /// Apparent size in bytes.
    size: u64 = 0,
    /// Bytes allocated on disk (st_blocks * 512); what du counts.
    alloc: u64 = 0,
    ino: u64 = 0,
    dev: u64 = 0,
    mtime_ns: i64 = 0,
    /// macOS only: getattrlistbulk reports the entry as a mount point.
    mount_point: bool = false,
};

pub const Error = error{
    AccessDenied,
    FileNotFound,
    NotDir,
    NameTooLong,
    SymLinkLoop,
    SystemResources,
    ProcessFdQuotaExceeded,
    InputOutput,
    Unexpected,
};

fn mapErrno(e: posix.E) Error {
    return switch (e) {
        .ACCES, .PERM => error.AccessDenied,
        .NOENT => error.FileNotFound,
        .NOTDIR => error.NotDir,
        .NAMETOOLONG => error.NameTooLong,
        .LOOP => error.SymLinkLoop,
        .NOMEM => error.SystemResources,
        .MFILE, .NFILE => error.ProcessFdQuotaExceeded,
        .IO => error.InputOutput,
        else => error.Unexpected,
    };
}

/// Open a directory relative to `dir_fd` without following a final symlink.
pub fn openDir(dir_fd: fd_t, path: [*:0]const u8) Error!fd_t {
    const flags: posix.O = .{ .DIRECTORY = true, .NOFOLLOW = true, .CLOEXEC = true, .NONBLOCK = true };
    while (true) {
        const rc = posix.system.openat(dir_fd, path, flags, @as(posix.mode_t, 0));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |e| return mapErrno(e),
        }
    }
}

/// Open the scan root, following symlinks (like `du PATH`).
pub fn openRoot(path: [*:0]const u8) Error!fd_t {
    const flags: posix.O = .{ .DIRECTORY = true, .CLOEXEC = true, .NONBLOCK = true };
    while (true) {
        const rc = posix.system.openat(posix.AT.FDCWD, path, flags, @as(posix.mode_t, 0));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |e| return mapErrno(e),
        }
    }
}

pub fn close(fd: fd_t) void {
    _ = posix.system.close(fd);
}

/// Stat a path relative to `dir_fd`. `follow` follows a final symlink.
pub fn statAt(dir_fd: fd_t, path: [*:0]const u8, follow: bool) Error!Stat {
    if (is_linux) {
        var stx: linux.Statx = undefined;
        const flags: u32 = linux.AT.NO_AUTOMOUNT | linux.AT.STATX_DONT_SYNC |
            (if (follow) @as(u32, 0) else linux.AT.SYMLINK_NOFOLLOW);
        const mask: linux.STATX = .{
            .TYPE = true,
            .MODE = true,
            .NLINK = true,
            .UID = true,
            .GID = true,
            .MTIME = true,
            .INO = true,
            .SIZE = true,
            .BLOCKS = true,
        };
        while (true) {
            const rc = linux.statx(dir_fd, path, flags, mask, &stx);
            switch (linux.errno(rc)) {
                .SUCCESS => break,
                .INTR => continue,
                else => |e| return mapErrno(e),
            }
        }
        return .{
            .kind = Kind.fromMode(stx.mode),
            .perm = @intCast(stx.mode & 0o7777),
            .nlink = stx.nlink,
            .uid = stx.uid,
            .gid = stx.gid,
            .size = stx.size,
            .alloc = stx.blocks * 512,
            .ino = stx.ino,
            .dev = (@as(u64, stx.dev_major) << 32) | stx.dev_minor,
            .mtime_ns = stx.mtime.sec * std.time.ns_per_s + stx.mtime.nsec,
        };
    } else {
        var st: std.c.Stat = undefined;
        const AT_SYMLINK_NOFOLLOW = 0x20;
        while (true) {
            const rc = std.c.fstatat(dir_fd, path, &st, if (follow) 0 else AT_SYMLINK_NOFOLLOW);
            switch (posix.errno(rc)) {
                .SUCCESS => break,
                .INTR => continue,
                else => |e| return mapErrno(e),
            }
        }
        return .{
            .kind = Kind.fromMode(st.mode),
            .perm = @intCast(st.mode & 0o7777),
            .nlink = st.nlink,
            .uid = st.uid,
            .gid = st.gid,
            .size = @intCast(st.size),
            .alloc = @as(u64, @intCast(st.blocks)) * 512,
            .ino = st.ino,
            .dev = @as(u32, @bitCast(st.dev)),
            .mtime_ns = @as(i64, st.mtimespec.sec) * std.time.ns_per_s + st.mtimespec.nsec,
        };
    }
}

/// True for kernel pseudo filesystems (proc, sysfs, cgroup, ...) that hold no
/// disk data and can be enormous or infinite to walk. Linux only.
pub fn isPseudoFs(fd: fd_t) ?[]const u8 {
    if (!is_linux) return null;
    // struct statfs starts with `long f_type` on every 64-bit Linux target.
    var buf: [256]u8 align(8) = undefined;
    const rc = linux.syscall2(.fstatfs, @as(usize, @bitCast(@as(isize, fd))), @intFromPtr(&buf));
    if (linux.errno(rc) != .SUCCESS) return null;
    const magic: u32 = @truncate(std.mem.readInt(u64, buf[0..8], builtin.cpu.arch.endian()));
    return switch (magic) {
        0x9fa0 => "proc",
        0x62656572 => "sysfs",
        0x1cd1 => "devpts",
        0x27e0eb => "cgroup",
        0x63677270 => "cgroup2",
        0x64626720 => "debugfs",
        0x74726163 => "tracefs",
        0x73636673 => "securityfs",
        0x6165676c => "pstore",
        0xcafe4a11 => "bpf",
        0x62656570 => "configfs",
        0x65735543 => "fusectl",
        0x19800202 => "mqueue",
        0x42494e4d => "binfmt_misc",
        0xde5e81e4 => "efivarfs",
        0x6e736673 => "nsfs",
        0xf97cff8c => "selinuxfs",
        0x0187 => "autofs",
        else => null,
    };
}

/// Read a symlink target into `buf`.
pub fn readLinkAt(dir_fd: fd_t, path: [*:0]const u8, buf: []u8) Error![]const u8 {
    if (is_linux) {
        const rc = linux.readlinkat(dir_fd, path, buf.ptr, buf.len);
        switch (linux.errno(rc)) {
            .SUCCESS => return buf[0..rc],
            else => |e| return mapErrno(e),
        }
    }
    return error.Unexpected;
}

pub const Entry = struct {
    name: [:0]const u8,
    /// Metadata, or the error from fetching it.
    stat: Error!Stat,
};

/// Iterates the entries of an open directory with their metadata.
/// `.` and `..` are skipped.
pub const DirReader = if (is_linux) LinuxDirReader else DarwinDirReader;

const LinuxDirReader = struct {
    fd: fd_t,
    buf: []align(8) u8,
    pos: usize = 0,
    end: usize = 0,

    pub fn init(fd: fd_t, buf: []align(8) u8) LinuxDirReader {
        return .{ .fd = fd, .buf = buf };
    }

    pub fn next(self: *LinuxDirReader) Error!?Entry {
        while (true) {
            if (self.pos >= self.end) {
                const rc = linux.getdents64(self.fd, self.buf.ptr, @intCast(self.buf.len));
                switch (linux.errno(rc)) {
                    .SUCCESS => {},
                    .INTR => continue,
                    .NOENT => return null, // directory was removed under us
                    else => |e| return mapErrno(e),
                }
                if (rc == 0) return null;
                self.pos = 0;
                self.end = rc;
            }
            const d: *align(1) const linux.dirent64 = @ptrCast(&self.buf[self.pos]);
            const reclen = d.reclen;
            const name_ptr: [*:0]const u8 = @ptrCast(&self.buf[self.pos + @offsetOf(linux.dirent64, "name")]);
            self.pos += reclen;
            const name = std.mem.span(name_ptr);
            if (isDotOrDotDot(name)) continue;
            return .{ .name = name, .stat = statAt(self.fd, name, false) };
        }
    }
};

fn isDotOrDotDot(name: []const u8) bool {
    return (name.len == 1 and name[0] == '.') or (name.len == 2 and name[0] == '.' and name[1] == '.');
}

// ===== macOS: getattrlistbulk =====

const darwin = struct {
    const AttrList = extern struct {
        bitmapcount: u16 = 5,
        reserved: u16 = 0,
        commonattr: u32,
        volattr: u32 = 0,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32 = 0,
    };

    const CMN_NAME = 0x00000001;
    const CMN_DEVID = 0x00000002;
    const CMN_OBJTYPE = 0x00000008;
    const CMN_MODTIME = 0x00000400;
    const CMN_OWNERID = 0x00008000;
    const CMN_GRPID = 0x00010000;
    const CMN_ACCESSMASK = 0x00020000;
    const CMN_FILEID = 0x02000000;
    const CMN_RETURNED_ATTRS = 0x80000000;

    const DIR_LINKCOUNT = 0x01;
    const DIR_MOUNTSTATUS = 0x04;
    const DIR_ALLOCSIZE = 0x08;
    const DIR_DATALENGTH = 0x20;
    const DIR_MNTSTATUS_MNTPOINT = 0x01;

    const FILE_LINKCOUNT = 0x01;
    const FILE_TOTALSIZE = 0x02;
    const FILE_ALLOCSIZE = 0x04;

    const FSOPT_NOFOLLOW = 0x01;
    const FSOPT_REPORT_FULLSIZE = 0x04;
    const FSOPT_PACK_INVAL_ATTRS = 0x08;

    const VREG = 1;
    const VDIR = 2;
    const VBLK = 3;
    const VCHR = 4;
    const VLNK = 5;
    const VSOCK = 6;
    const VFIFO = 7;

    extern "c" fn getattrlistbulk(dirfd: c_int, alist: *const AttrList, attrbuf: *anyopaque, size: usize, options: u64) c_int;
};

const DarwinDirReader = struct {
    fd: fd_t,
    buf: []align(8) u8,
    pos: usize = 0,
    remaining: usize = 0,

    const request: darwin.AttrList = .{
        .commonattr = darwin.CMN_RETURNED_ATTRS | darwin.CMN_NAME | darwin.CMN_DEVID |
            darwin.CMN_OBJTYPE | darwin.CMN_MODTIME | darwin.CMN_OWNERID | darwin.CMN_GRPID |
            darwin.CMN_ACCESSMASK | darwin.CMN_FILEID,
        .dirattr = darwin.DIR_LINKCOUNT | darwin.DIR_MOUNTSTATUS | darwin.DIR_ALLOCSIZE | darwin.DIR_DATALENGTH,
        .fileattr = darwin.FILE_LINKCOUNT | darwin.FILE_TOTALSIZE | darwin.FILE_ALLOCSIZE,
    };

    pub fn init(fd: fd_t, buf: []align(8) u8) DarwinDirReader {
        return .{ .fd = fd, .buf = buf };
    }

    pub fn next(self: *DarwinDirReader) Error!?Entry {
        while (true) {
            if (self.remaining == 0) {
                const opts = darwin.FSOPT_NOFOLLOW | darwin.FSOPT_REPORT_FULLSIZE | darwin.FSOPT_PACK_INVAL_ATTRS;
                const rc = darwin.getattrlistbulk(self.fd, &request, self.buf.ptr, self.buf.len, opts);
                if (rc < 0) switch (posix.errno(rc)) {
                    .INTR, .AGAIN => continue,
                    // EDEADLK: dataless (iCloud) directory we refused to materialize.
                    .DEADLK => return error.AccessDenied,
                    else => |e| return mapErrno(e),
                };
                if (rc == 0) return null;
                self.remaining = @intCast(rc);
                self.pos = 0;
            }
            self.remaining -= 1;
            const rec = self.buf[self.pos..];
            const reclen = std.mem.readInt(u32, rec[0..4], .little);
            self.pos += reclen;
            const entry = parse(rec[0..reclen]);
            if (isDotOrDotDot(entry.name)) continue;
            return entry;
        }
    }

    /// Attributes are packed in bit order: all requested common attributes,
    /// then directory attributes for directories or file attributes for
    /// everything else, each 4-byte aligned and unpadded.
    fn parse(rec: []const u8) Entry {
        var r: Cursor = .{ .b = rec, .i = 4 };
        const returned_common = r.u32_();
        _ = r.u32_(); // vol
        const returned_dir = r.u32_();
        const returned_file = r.u32_();
        _ = r.u32_(); // fork

        const name_ref_at = r.i;
        const name_off = r.i32_();
        const name_len = r.u32_();
        const name_start: usize = @intCast(@as(i64, @intCast(name_ref_at)) + name_off);
        const name: [:0]const u8 = rec[name_start .. name_start + name_len - 1 :0];

        const devid = r.u32_();
        const objtype = r.u32_();
        const mtime_sec = r.i64_();
        const mtime_nsec = r.i64_();
        const uid = r.u32_();
        const gid = r.u32_();
        const access = r.u32_();
        const fileid = r.u64_();

        var st: Stat = .{
            .kind = switch (objtype) {
                darwin.VREG => .file,
                darwin.VDIR => .dir,
                darwin.VLNK => .symlink,
                darwin.VFIFO => .fifo,
                darwin.VSOCK => .socket,
                darwin.VBLK => .block,
                darwin.VCHR => .char,
                else => .unknown,
            },
            .perm = @truncate(access & 0o7777),
            .uid = uid,
            .gid = gid,
            .ino = if (returned_common & darwin.CMN_FILEID != 0) fileid else 0,
            .dev = devid,
            .mtime_ns = mtime_sec * std.time.ns_per_s + mtime_nsec,
        };
        if (objtype == darwin.VDIR) {
            const linkcount = r.u32_();
            const mountstatus = r.u32_();
            const allocsize = r.u64_();
            const datalength = r.u64_();
            st.nlink = linkcount;
            st.mount_point = mountstatus & darwin.DIR_MNTSTATUS_MNTPOINT != 0;
            if (returned_dir & darwin.DIR_ALLOCSIZE != 0) st.alloc = allocsize;
            if (returned_dir & darwin.DIR_DATALENGTH != 0) st.size = datalength;
        } else {
            const linkcount = r.u32_();
            const totalsize = r.u64_();
            const allocsize = r.u64_();
            if (returned_file & darwin.FILE_LINKCOUNT != 0) st.nlink = linkcount;
            if (returned_file & darwin.FILE_TOTALSIZE != 0) st.size = totalsize;
            if (returned_file & darwin.FILE_ALLOCSIZE != 0) st.alloc = allocsize;
        }
        return .{ .name = name, .stat = st };
    }

    const Cursor = struct {
        b: []const u8,
        i: usize,
        fn u32_(c: *Cursor) u32 {
            defer c.i += 4;
            return std.mem.readInt(u32, c.b[c.i..][0..4], .little);
        }
        fn i32_(c: *Cursor) i32 {
            return @bitCast(c.u32_());
        }
        fn u64_(c: *Cursor) u64 {
            defer c.i += 8;
            return std.mem.readInt(u64, c.b[c.i..][0..8], .little);
        }
        fn i64_(c: *Cursor) i64 {
            return @bitCast(c.u64_());
        }
    };
};

/// macOS: never download iCloud "dataless" file contents while scanning.
pub fn preventDatalessMaterialization() void {
    if (!is_darwin) return;
    const IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES = 3;
    const IOPOL_SCOPE_PROCESS = 0;
    const IOPOL_MATERIALIZE_DATALESS_FILES_OFF = 1;
    _ = setiopolicy_np(IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES, IOPOL_SCOPE_PROCESS, IOPOL_MATERIALIZE_DATALESS_FILES_OFF);
}

extern "c" fn setiopolicy_np(iotype: c_int, scope: c_int, policy: c_int) c_int;
