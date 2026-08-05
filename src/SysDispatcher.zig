const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

pub const Config = struct {
    allocator: std.mem.Allocator,
    entries: ?u16 = null,
};

/// Placeholder type retained for compatibility with previous backends.
pub const StatRequest = void;

pub const Backend = SyncBackend;

/// The subset of file metadata the scanner needs, normalized across platforms.
///
/// `std.Io.File.Stat` deliberately omits the allocated-block count, but that is
/// exactly what makes the totals agree with `du`, so we go under it.
pub const Stat = struct {
    nlink: u64,
    /// Apparent size in bytes.
    size: u64,
    /// Blocks of 512 bytes actually allocated. Differs from `size` for sparse,
    /// compressed, and inline-data files.
    blocks: u64,
    /// Preferred I/O block size.
    blksize: u64,
};

pub const StatError = error{
    AccessDenied,
    FileNotFound,
    SymLinkLoop,
    NameTooLong,
    SystemResources,
} || posix.UnexpectedError;

/// `std.posix` no longer wraps `fstatat`, and on Linux `std.posix.Stat` is
/// `void` because the kernel interface there is `statx`. Both platforms link
/// libc for us, so call through to the appropriate one directly.
pub fn statPath(dir_fd: posix.fd_t, name: [:0]const u8) StatError!Stat {
    if (builtin.os.tag == .linux) {
        var stx: std.os.linux.Statx = undefined;
        const want: std.os.linux.STATX = .{ .NLINK = true, .SIZE = true, .BLOCKS = true };
        const rc = std.c.statx(dir_fd, name.ptr, 0, want, &stx);
        try checkErrno(posix.errno(rc));
        return .{
            .nlink = stx.nlink,
            .size = stx.size,
            .blocks = stx.blocks,
            .blksize = stx.blksize,
        };
    }

    const fstatat_sym = if (posix.lfs64_abi) posix.system.fstatat64 else posix.system.fstatat;
    var st: posix.system.Stat = undefined;
    try checkErrno(posix.errno(fstatat_sym(dir_fd, name.ptr, &st, 0)));
    return .{
        .nlink = @intCast(st.nlink),
        .size = @intCast(st.size),
        .blocks = @intCast(st.blocks),
        .blksize = @intCast(st.blksize),
    };
}

fn checkErrno(err: posix.E) StatError!void {
    return switch (err) {
        .SUCCESS => {},
        .ACCES, .PERM => error.AccessDenied,
        .NOENT, .NOTDIR => error.FileNotFound,
        .LOOP => error.SymLinkLoop,
        .NAMETOOLONG => error.NameTooLong,
        .NOMEM => error.SystemResources,
        else => posix.unexpectedErrno(err),
    };
}

const SyncBackend = struct {
    pub fn init(_: *SyncBackend, config: Config) !void {
        _ = config;
    }

    pub fn deinit(_: *SyncBackend) void {}

    pub fn openDirectory(
        _: *SyncBackend,
        parent_fd: posix.fd_t,
        name: [:0]const u8,
    ) posix.OpenError!posix.fd_t {
        return posix.openat(parent_fd, name, directoryFlags(), 0);
    }

    pub fn statAt(
        _: *SyncBackend,
        dir_fd: posix.fd_t,
        name: [:0]const u8,
    ) StatError!Stat {
        return statPath(dir_fd, name);
    }

    pub fn directoryFlags() posix.O {
        return posix.O{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        };
    }
};
