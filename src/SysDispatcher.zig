// Unified syscall dispatcher. On non-Linux targets it simply forwards to
// `posix.*`; on Linux it spins up an io_uring instance so callers can submit
// open/stat operations without knowing the mechanics.  Future work: move
// directory reads here too so the scanner can treat stat + readdir uniformly.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const use_linux = builtin.target.os.tag == .linux;
const linux = std.os.linux;

pub const Config = struct {
    allocator: std.mem.Allocator,
    entries: ?u16 = null,
};

const Backend = switch (builtin.target.os.tag) {
    .linux => LinuxBackend,
    else => SyncBackend,
};

pub const Dispatcher = struct {
    backend: Backend,

    pub fn init(config: Config) !Dispatcher {
        return Dispatcher{
            .backend = try Backend.init(config),
        };
    }

    pub fn deinit(self: *Dispatcher) void {
        self.backend.deinit();
    }

    pub fn openDirectory(self: *Dispatcher, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
        return self.backend.openDirectory(parent_fd, name);
    }

    pub fn statAt(self: *Dispatcher, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
        return self.backend.statAt(dir_fd, name);
    }
};

const SyncBackend = struct {
    pub fn init(config: Config) !SyncBackend {
        _ = config;
        return .{};
    }

    pub fn deinit(_: *SyncBackend) void {}

    pub fn openDirectory(_: *SyncBackend, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
        return posix.openat(parent_fd, name, directoryFlags(), 0);
    }

    pub fn statAt(_: *SyncBackend, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
        return posix.fstatat(dir_fd, name, 0);
    }

    pub fn directoryFlags() posix.O {
        return posix.O{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        };
    }
};

const LinuxBackend = if (use_linux) struct {
    ring: linux.IoUring,

    pub fn init(config: Config) !LinuxBackend {
        const entries = config.entries orelse 128;
        return LinuxBackend{
            .ring = try linux.IoUring.init(entries, 0),
        };
    }

    pub fn deinit(self: *LinuxBackend) void {
        self.ring.deinit();
    }

    pub fn openDirectory(self: *LinuxBackend, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
        var sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => return posix.openat(parent_fd, name, SyncBackend.directoryFlags(), 0),
        };
        const dir_fd: linux.fd_t = @intCast(parent_fd);
        sqe.prep_openat(dir_fd, name.ptr, directoryLinuxFlags(), 0);
        sqe.user_data = 0;

        _ = self.ring.submit_and_wait(1) catch |err| return mapOpenSubmitError(err, parent_fd, name);
        const cqe = self.ring.copy_cqe() catch |err| return mapOpenSubmitError(err, parent_fd, name);
        return handleOpenResult(cqe.res);
    }

    pub fn statAt(self: *LinuxBackend, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
        var statx_buf = std.mem.zeroes(linux.Statx);

        var sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => return posix.fstatat(dir_fd, name, 0),
        };

        const fd: linux.fd_t = @intCast(dir_fd);
        sqe.prep_statx(fd, name.ptr, 0, linux.STATX_BASIC_STATS, &statx_buf);
        sqe.user_data = 0;

        _ = self.ring.submit_and_wait(1) catch |err| return mapStatSubmitError(err, dir_fd, name);

        const cqe = self.ring.copy_cqe() catch |err| return mapStatSubmitError(err, dir_fd, name);
        if (cqe.res < 0) {
            const errno_linux: linux.E = @enumFromInt(@as(u32, @intCast(-cqe.res)));
            const errno_posix: posix.E = @enumFromInt(@intFromEnum(errno_linux));
            return mapStatError(errno_posix);
        }

        return convertStatx(statx_buf);
    }

    fn directoryLinuxFlags() linux.O {
        var flags = linux.O{
            .DIRECTORY = true,
            .NOFOLLOW = true,
        };
        flags.NONBLOCK = true;
        return flags;
    }

    fn handleOpenResult(res: i32) posix.OpenError!posix.fd_t {
        if (res >= 0) {
            return @as(posix.fd_t, @intCast(res));
        }

        const code: u32 = @intCast(-res);
        const errno_linux: linux.E = @enumFromInt(code);
        const errno_posix: posix.E = @enumFromInt(@intFromEnum(errno_linux));
        return mapOpenError(errno_posix);
    }
} else struct {};

fn mapOpenSubmitError(err: anyerror, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
    switch (err) {
        error.SystemResources => return error.SystemResources,
        error.PermissionDenied => return error.PermissionDenied,
        else => {}, // Other submit failures are likely ENOSYS/OPNOTSUPP; fall back.
    }
    return posix.openat(parent_fd, name, SyncBackend.directoryFlags(), 0);
}

fn mapStatSubmitError(err: anyerror, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
    switch (err) {
        error.SystemResources => return error.SystemResources,
        error.PermissionDenied => return error.PermissionDenied,
        else => {}, // From here on we eagerly fall back to fstatat.
    }
    return posix.fstatat(dir_fd, name, 0);
}

fn mapOpenError(err: posix.E) posix.OpenError {
    return switch (err) {
        .FAULT => unreachable,
        .BADF => unreachable,
        .INTR => unreachable,
        .INVAL => error.BadPathName,
        .ACCES => error.AccessDenied,
        .FBIG, .OVERFLOW => error.FileTooBig,
        .ISDIR => error.IsDir,
        .LOOP => error.SymLinkLoop,
        .MFILE => error.ProcessFdQuotaExceeded,
        .NAMETOOLONG => error.NameTooLong,
        .NFILE => error.SystemFdQuotaExceeded,
        .NODEV => error.NoDevice,
        .NOENT => error.FileNotFound,
        .SRCH => error.ProcessNotFound,
        .NOMEM => error.SystemResources,
        .NOSPC => error.NoSpaceLeft,
        .NOTDIR => error.NotDir,
        .PERM => error.PermissionDenied,
        .EXIST => error.PathAlreadyExists,
        .BUSY => error.DeviceBusy,
        .OPNOTSUPP => error.FileLocksNotSupported,
        .AGAIN => error.WouldBlock,
        .TXTBSY => error.FileBusy,
        .NXIO => error.NoDevice,
        .ILSEQ => if (builtin.target.os.tag == .wasi)
            error.InvalidUtf8
        else
            posix.unexpectedErrno(err),
        else => posix.unexpectedErrno(err),
    };
}

fn mapStatError(err: posix.E) posix.FStatAtError {
    return switch (err) {
        .ACCES => error.AccessDenied,
        .PERM => error.PermissionDenied,
        .NAMETOOLONG => error.NameTooLong,
        .LOOP => error.SymLinkLoop,
        .NOENT => error.FileNotFound,
        .NOTDIR => error.FileNotFound,
        .NOMEM => error.SystemResources,
        .ILSEQ => if (builtin.target.os.tag == .wasi)
            error.InvalidUtf8
        else
            posix.unexpectedErrno(err),
        else => posix.unexpectedErrno(err),
    };
}

fn convertStatx(statx_buf: linux.Statx) posix.Stat {
    var stat = std.mem.zeroes(posix.Stat);
    stat.mode = @as(@TypeOf(stat.mode), @intCast(statx_buf.mode));
    stat.uid = statx_buf.uid;
    stat.gid = statx_buf.gid;
    stat.ino = statx_buf.ino;
    stat.size = @as(@TypeOf(stat.size), @intCast(statx_buf.size));
    stat.blocks = @as(@TypeOf(stat.blocks), @intCast(statx_buf.blocks));
    stat.blksize = @as(@TypeOf(stat.blksize), @intCast(statx_buf.blksize));
    stat.nlink = @as(@TypeOf(stat.nlink), @intCast(statx_buf.nlink));
    stat.atim = statxTimestampToTimespec(statx_buf.atime);
    stat.mtim = statxTimestampToTimespec(statx_buf.mtime);
    stat.ctim = statxTimestampToTimespec(statx_buf.ctime);
    return stat;
}

fn statxTimestampToTimespec(ts: linux.statx_timestamp) linux.timespec {
    const Timespec = linux.timespec;
    const SecT = @TypeOf((Timespec{ .sec = 0, .nsec = 0 }).sec);
    const NsecT = @TypeOf((Timespec{ .sec = 0, .nsec = 0 }).nsec);
    return Timespec{
        .sec = @as(SecT, ts.sec),
        .nsec = @as(NsecT, @intCast(ts.nsec)),
    };
}
