// Unified syscall dispatcher. On non-Linux targets it simply forwards to
// `posix.*`; on Linux it spins up an io_uring instance so callers can submit
// open/stat operations without knowing the mechanics.  Future work: move
// directory reads here too so the scanner can treat stat + readdir uniformly.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const use_linux = builtin.target.os.tag == .linux;
const linux = std.os.linux;
const AutoHashMap = std.AutoHashMapUnmanaged;
const AtomicU64 = std.atomic.Value(u64);

pub const Config = struct {
    allocator: std.mem.Allocator,
    entries: ?u16 = null,
};

pub const StatRequest = if (use_linux) struct {
    token: u64 = 0,
    statx_result: linux.Statx = undefined,
    fallback: ?posix.Stat = null,
} else struct {};

pub const Backend = switch (builtin.target.os.tag) {
    .linux => LinuxBackend,
    else => SyncBackend,
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
    ring: linux.IoUring = undefined,
    allocator: std.mem.Allocator,
    pending: AutoHashMap(u64, linux.io_uring_cqe) = .empty,
    next_user_data: AtomicU64,
    pending_submit: u32 = 0,

    pub fn init(self: *LinuxBackend, config: Config) !void {
        const entries = config.entries orelse 128;

        self.ring = try linux.IoUring.init(entries, 0);
        self.allocator = config.allocator;
        self.pending = .empty;
        self.next_user_data = AtomicU64.init(1);

        errdefer self.ring.deinit();

        try self.pending.ensureTotalCapacity(config.allocator, entries);
    }

    pub fn deinit(self: *LinuxBackend) void {
        self.pending.deinit(self.allocator);
        self.ring.deinit();
    }

    pub fn openDirectory(self: *LinuxBackend, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
        var sqe = self.tryAcquireSqe() catch |err| {
            return mapOpenSubmitError(err, parent_fd, name);
        };
        const token = self.reserveToken();
        const dir_fd: linux.fd_t = @intCast(parent_fd);
        sqe.prep_openat(dir_fd, name.ptr, directoryLinuxFlags(), 0);
        sqe.user_data = token;

        self.submitQueued() catch |err| {
            return mapOpenSubmitError(err, parent_fd, name);
        };

        const cqe = self.awaitCompletion(token, 0) catch |err| return mapOpenSubmitError(err, parent_fd, name);

        if (cqe.res < 0) {
            const errno_linux: linux.E = @enumFromInt(@as(u32, @intCast(-cqe.res)));
            const errno_posix: posix.E = @enumFromInt(@intFromEnum(errno_linux));
            if (errno_posix == .BADF) {
                return error.FileNotFound;
            }
        }

        return handleOpenResult(cqe.res);
    }

    pub fn statAt(self: *LinuxBackend, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
        var request = StatRequest{};
        try self.queueStat(dir_fd, name, &request);

        self.submitQueued() catch |err| {
            return mapStatSubmitError(err, dir_fd, name);
        };

        return self.awaitStat(&request, dir_fd, name);
    }

    fn reserveToken(self: *LinuxBackend) u64 {
        return self.next_user_data.fetchAdd(1, .acq_rel);
    }

    fn tryAcquireSqe(self: *LinuxBackend) !*linux.io_uring_sqe {
        const sqe = self.ring.get_sqe() catch |err| {
            return err;
        };
        self.pending_submit += 1;
        return sqe;
    }

    pub fn submitQueued(self: *LinuxBackend) !void {
        if (self.pending_submit == 0) return;
        _ = try self.ring.submit();
        self.pending_submit = 0;
    }

    pub fn queueStat(
        self: *LinuxBackend,
        dir_fd: posix.fd_t,
        name: [:0]const u8,
        request: *StatRequest,
    ) posix.FStatAtError!void {
        request.fallback = null;

        const sqe = self.tryAcquireSqe() catch |err| switch (err) {
            error.SubmissionQueueFull => blk: {
                self.submitQueued() catch {};
                break :blk (self.tryAcquireSqe() catch |retry_err| switch (retry_err) {
                    error.SubmissionQueueFull => null,
                });
            },
        };

        if (sqe) |sqe_ptr| {
            request.statx_result = std.mem.zeroes(linux.Statx);
            const token = self.reserveToken();
            request.token = token;
            const fd: linux.fd_t = @intCast(dir_fd);
            sqe_ptr.prep_statx(fd, name.ptr, 0, linux.STATX_BASIC_STATS, &request.statx_result);
            sqe_ptr.user_data = token;
            return;
        }

        const stat = posix.fstatat(dir_fd, name, 0);
        request.token = 0;
        request.fallback = stat catch |err| return err;
    }

    pub fn awaitStat(
        self: *LinuxBackend,
        request: *StatRequest,
        dir_fd: posix.fd_t,
        name: [:0]const u8,
    ) posix.FStatAtError!posix.Stat {
        if (request.fallback) |stat| {
            request.fallback = null;
            return stat;
        }

        self.submitQueued() catch |err| {
            return mapStatSubmitError(err, dir_fd, name);
        };

        const cqe = self.awaitCompletion(request.token, 0) catch |err| {
            return mapStatSubmitError(err, dir_fd, name);
        };

        if (cqe.res < 0) {
            const errno_linux: linux.E = @enumFromInt(@as(u32, @intCast(-cqe.res)));
            const errno_posix: posix.E = @enumFromInt(@intFromEnum(errno_linux));
            if (errno_posix == .BADF) {
                return error.FileNotFound;
            }
            return mapStatError(errno_posix);
        }

        return convertStatx(request.statx_result);
    }

    fn awaitCompletion(self: *LinuxBackend, token: u64, wait_hint: u32) !linux.io_uring_cqe {
        if (self.takeCachedCompletion(token)) |cached| return cached;

        var buffer: [8]linux.io_uring_cqe = undefined;

        while (true) {
            if (try self.drainIntoCache(buffer[0..], 0, token)) |found| {
                return found;
            }

            if (self.takeCachedCompletion(token)) |cached| return cached;

            if (try self.drainIntoCache(buffer[0..], @max(1, wait_hint), token)) |found_wait| {
                return found_wait;
            }

            if (self.takeCachedCompletion(token)) |cached_wait| return cached_wait;
        }
    }

    fn drainIntoCache(
        self: *LinuxBackend,
        buffer: []linux.io_uring_cqe,
        wait_nr: u32,
        token: u64,
    ) !?linux.io_uring_cqe {
        if (buffer.len == 0) return null;

        const count = try self.fetchCompletions(buffer, wait_nr);
        if (count == 0) return null;

        return try self.storeCompletions(buffer[0..count], token, wait_nr);
    }

    fn fetchCompletions(
        self: *LinuxBackend,
        buffer: []linux.io_uring_cqe,
        wait_nr: u32,
    ) !usize {
        const got = try self.ring.copy_cqes(buffer, wait_nr);
        return @as(usize, @intCast(got));
    }

    fn storeCompletions(
        self: *LinuxBackend,
        cqes: []const linux.io_uring_cqe,
        token: u64,
        wait_nr: u32,
    ) !?linux.io_uring_cqe {
        _ = wait_nr;
        var result: ?linux.io_uring_cqe = null;

        for (cqes) |cqe| {
            if (cqe.user_data == token) {
                result = cqe;
            } else {
                self.pending.putAssumeCapacity(cqe.user_data, cqe);
            }
        }

        return result;
    }

    fn takeCachedCompletion(self: *LinuxBackend, token: u64) ?linux.io_uring_cqe {
        const entry = self.pending.fetchRemove(token);

        if (entry) |hit| return hit.value;
        return null;
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
