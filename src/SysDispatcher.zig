// Unified syscall dispatcher. On non-Linux targets it simply forwards to
// `posix.*`; on Linux it spins up an io_uring instance so callers can submit
// open/stat operations without knowing the mechanics.  Future work: move
// directory reads here too so the scanner can treat stat + readdir uniformly.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const use_linux = builtin.target.os.tag == .linux;
const linux = std.os.linux;
const Io = std.Io;
const mem = std.mem;
const AutoHashMap = std.AutoHashMap;
const AtomicU64 = std.atomic.Value(u64);

pub const Config = struct {
    allocator: std.mem.Allocator,
    entries: ?u16 = null,
    trace: ?*TraceSink = null,
};

pub const TraceSink = struct {
    mutex: std.Thread.Mutex = .{},
    writer: *Io.Writer,

    pub fn init(writer: *Io.Writer) TraceSink {
        return .{ .writer = writer };
    }

    fn line(self: *TraceSink, comptime fmt_string: []const u8, args: anytype) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.writer.print(fmt_string, args) catch return;
        self.writer.writeByte('\n') catch return;
    }

    pub fn logSubmitOpen(self: *TraceSink, token: u64, parent_fd: posix.fd_t, name: []const u8) void {
        self.line("submit kind=open token={d} parent_fd={d} name=\"{s}\"", .{ token, parent_fd, name });
    }

    pub fn logSubmitStat(self: *TraceSink, token: u64, dir_fd: posix.fd_t, name: []const u8) void {
        self.line("submit kind=stat token={d} dir_fd={d} name=\"{s}\"", .{ token, dir_fd, name });
    }

    pub fn logFallback(self: *TraceSink, kind: []const u8, reason: []const u8) void {
        self.line("fallback kind={s} reason={s}", .{ kind, reason });
    }

    pub fn logDrain(
        self: *TraceSink,
        token: u64,
        wait_nr: u32,
        total: usize,
        matched: bool,
        others: usize,
        sample: []const u64,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.writer.print(
            "drain token={d} wait={d} count={d} matched={s}",
            .{ token, wait_nr, total, if (matched) "true" else "false" },
        ) catch return;
        if (others > 0) {
            self.writer.print(" others_total={d} sample=", .{others}) catch return;
            self.writer.writeByte('[') catch return;
            for (sample, 0..) |value, idx| {
                if (idx != 0) self.writer.writeByte(',') catch return;
                self.writer.print("{d}", .{value}) catch return;
            }
            self.writer.writeByte(']') catch return;
        }
        self.writer.writeByte('\n') catch return;
    }

    pub fn logCacheHit(self: *TraceSink, token: u64, res: i32) void {
        self.line("cache_hit token={d} res={d}", .{ token, res });
    }

    pub fn logCompletion(self: *TraceSink, kind: []const u8, token: u64, res: i32) void {
        self.line("completion kind={s} token={d} res={d}", .{ kind, token, res });
    }
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
    allocator: std.mem.Allocator,
    sq_mutex: std.Thread.Mutex = .{},
    cq_mutex: std.Thread.Mutex = .{},
    pending_mutex: std.Thread.Mutex = .{},
    pending: AutoHashMap(u64, linux.io_uring_cqe),
    next_user_data: AtomicU64,
    trace: ?*TraceSink,

    pub fn init(config: Config) !LinuxBackend {
        const entries = config.entries orelse 128;
        var backend = LinuxBackend{
            .ring = try linux.IoUring.init(entries, 0),
            .allocator = config.allocator,
            .pending = AutoHashMap(u64, linux.io_uring_cqe).init(config.allocator),
            .next_user_data = AtomicU64.init(1),
            .trace = config.trace,
        };
        errdefer backend.ring.deinit();

        try backend.pending.ensureTotalCapacity(entries);
        return backend;
    }

    pub fn deinit(self: *LinuxBackend) void {
        self.pending.deinit();
        self.ring.deinit();
    }

    pub fn openDirectory(self: *LinuxBackend, parent_fd: posix.fd_t, name: [:0]const u8) posix.OpenError!posix.fd_t {
        self.sq_mutex.lock();
        var sqe = self.ring.get_sqe() catch |err| {
            self.sq_mutex.unlock();
            if (self.trace) |trace| {
                trace.logFallback("open", "sq_full");
            }
            return switch (err) {
                error.SubmissionQueueFull => posix.openat(parent_fd, name, SyncBackend.directoryFlags(), 0),
            };
        };
        const token = self.reserveToken();
        const dir_fd: linux.fd_t = @intCast(parent_fd);
        sqe.prep_openat(dir_fd, name.ptr, directoryLinuxFlags(), 0);
        sqe.user_data = token;

        if (self.trace) |trace| {
            trace.logSubmitOpen(token, parent_fd, mem.sliceTo(name, 0));
        }

        _ = self.ring.submit_and_wait(1) catch |err| {
            self.sq_mutex.unlock();
            if (self.trace) |trace| {
                trace.logFallback("open", @errorName(err));
            }
            return mapOpenSubmitError(err, parent_fd, name);
        };
        self.sq_mutex.unlock();

        const cqe = self.awaitCompletion(token, 0) catch |err| return mapOpenSubmitError(err, parent_fd, name);
        if (self.trace) |trace| {
            trace.logCompletion("open", token, cqe.res);
        }
        return handleOpenResult(cqe.res);
    }

    pub fn statAt(self: *LinuxBackend, dir_fd: posix.fd_t, name: [:0]const u8) posix.FStatAtError!posix.Stat {
        var statx_buf = std.mem.zeroes(linux.Statx);

        self.sq_mutex.lock();
        var sqe = self.ring.get_sqe() catch |err| {
            self.sq_mutex.unlock();
            if (self.trace) |trace| {
                trace.logFallback("stat", "sq_full");
            }
            return switch (err) {
                error.SubmissionQueueFull => posix.fstatat(dir_fd, name, 0),
            };
        };

        const token = self.reserveToken();
        const fd: linux.fd_t = @intCast(dir_fd);
        sqe.prep_statx(fd, name.ptr, 0, linux.STATX_BASIC_STATS, &statx_buf);
        sqe.user_data = token;

        if (self.trace) |trace| {
            trace.logSubmitStat(token, dir_fd, mem.sliceTo(name, 0));
        }

        _ = self.ring.submit_and_wait(1) catch |err| {
            self.sq_mutex.unlock();
            if (self.trace) |trace| {
                trace.logFallback("stat", @errorName(err));
            }
            return mapStatSubmitError(err, dir_fd, name);
        };
        self.sq_mutex.unlock();

        const cqe = self.awaitCompletion(token, 0) catch |err| return mapStatSubmitError(err, dir_fd, name);
        if (self.trace) |trace| {
            trace.logCompletion("stat", token, cqe.res);
        }
        if (cqe.res < 0) {
            const errno_linux: linux.E = @enumFromInt(@as(u32, @intCast(-cqe.res)));
            const errno_posix: posix.E = @enumFromInt(@intFromEnum(errno_linux));
            return mapStatError(errno_posix);
        }

        return convertStatx(statx_buf);
    }

    fn reserveToken(self: *LinuxBackend) u64 {
        return self.next_user_data.fetchAdd(1, .acq_rel);
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
        self.cq_mutex.lock();
        defer self.cq_mutex.unlock();
        const got = try self.ring.copy_cqes(buffer, wait_nr);
        return @as(usize, @intCast(got));
    }

    fn storeCompletions(
        self: *LinuxBackend,
        cqes: []const linux.io_uring_cqe,
        token: u64,
        wait_nr: u32,
    ) !?linux.io_uring_cqe {
        var result: ?linux.io_uring_cqe = null;
        var other_count: usize = 0;
        var sample_buf: [4]u64 = undefined;
        var sample_len: usize = 0;

        self.pending_mutex.lock();
        for (cqes) |cqe| {
            if (cqe.user_data == token) {
                result = cqe;
            } else {
                self.pending.putAssumeCapacity(cqe.user_data, cqe);
                other_count += 1;
                if (sample_len < sample_buf.len) {
                    sample_buf[sample_len] = cqe.user_data;
                    sample_len += 1;
                }
            }
        }
        self.pending_mutex.unlock();

        if (self.trace) |trace| {
            trace.logDrain(token, wait_nr, cqes.len, result != null, other_count, sample_buf[0..sample_len]);
        }

        return result;
    }

    fn takeCachedCompletion(self: *LinuxBackend, token: u64) ?linux.io_uring_cqe {
        self.pending_mutex.lock();
        const entry = self.pending.fetchRemove(token);
        self.pending_mutex.unlock();

        if (entry) |hit| {
            if (self.trace) |trace| {
                trace.logCacheHit(token, hit.value.res);
            }
            return hit.value;
        }
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
