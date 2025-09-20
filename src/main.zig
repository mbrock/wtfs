const std = @import("std");
const builtin = @import("builtin");
const wtfs = @import("wtfs").mac;
const strpool = @import("wtfs").strpool;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

/// Scanner configured to read names, object types, and file sizes.
const Scanner = wtfs.DirScanner(.{
    .common = .{
        .name = true,
        .objtype = true,
    },
    .dir = .{
        .mountstatus = true,
    },
    .file = .{
        .totalsize = false,
        .allocsize = true,
    },
});

const AtomicUsize = std.atomic.Value(usize);
const AtomicU16 = std.atomic.Value(u16);
const invalid_fd: std.posix.fd_t = -1;

const Stats = struct {
    directories_started: AtomicUsize = AtomicUsize.init(0),
    directories_completed: AtomicUsize = AtomicUsize.init(0),
    directories_scheduled: AtomicUsize = AtomicUsize.init(0),
    directories_discovered: AtomicUsize = AtomicUsize.init(0),
    files_discovered: AtomicUsize = AtomicUsize.init(0),
    symlinks_discovered: AtomicUsize = AtomicUsize.init(0),
    other_discovered: AtomicUsize = AtomicUsize.init(0),
    scanner_batches: AtomicUsize = AtomicUsize.init(0),
    scanner_entries: AtomicUsize = AtomicUsize.init(0),
    scanner_max_batch: AtomicUsize = AtomicUsize.init(0),
    scanner_errors: AtomicUsize = AtomicUsize.init(0),

    fn init() Stats {
        return .{};
    }
};

const DirectoryNode = struct {
    parent: u32,
    basename: u32,
    fd: std.posix.fd_t = invalid_fd,
    fdrefcount: AtomicU16,
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};
const DirectoryStore = std.MultiArrayList(DirectoryNode);

const TaskQueue = struct {
    progress: *std.Progress.Node,
    high_watermark: std.atomic.Value(usize) = .init(0),
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    items: std.ArrayList(usize) = .{},
    done: bool = false,

    fn deinit(self: *TaskQueue, allocator: std.mem.Allocator) void {
        self.items.deinit(allocator);
    }

    fn push(self: *TaskQueue, allocator: std.mem.Allocator, value: usize) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const current_len = self.items.items.len;
        self.progress.setCompletedItems(current_len + 1);
        _ = self.high_watermark.fetchMax(current_len + 1, .acq_rel);

        try self.items.append(allocator, value);
        self.cond.signal();
    }

    fn pop(self: *TaskQueue) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.items.items.len == 0 and !self.done) {
            self.cond.wait(&self.mutex);
        }

        if (self.items.items.len == 0) {
            return null;
        }

        self.progress.setCompletedItems(self.items.items.len - 1);
        return self.items.pop();
    }

    fn close(self: *TaskQueue) void {
        self.mutex.lock();
        self.done = true;
        self.mutex.unlock();
        self.cond.broadcast();
    }
};

const Context = struct {
    allocator: std.mem.Allocator,
    directories: *DirectoryStore,
    pool: *std.Thread.Pool,
    wait_group: *std.Thread.WaitGroup,
    progress_node: std.Progress.Node,
    errprogress: std.Progress.Node,
    skip_hidden: bool,
    inaccessible_dirs: AtomicUsize = AtomicUsize.init(0),
    directories_mutex: std.Thread.Mutex = .{},
    task_queue: TaskQueue,
    outstanding: AtomicUsize = AtomicUsize.init(0),

    namelock: std.Thread.Mutex = .{},
    namedata: *std.ArrayList(u8),
    idxset: *strpool.IndexSet,
    stats: Stats,

    fn getNode(self: *Context, index: usize) DirectoryNode {
        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();
        return self.directories.get(index);
    }

    fn internPath(self: *Context, path: []const u8) !u32 {
        self.namelock.lock();
        defer self.namelock.unlock();

        return strpool.intern(self.idxset, self.allocator, self.namedata, path);
    }

    fn setTotals(self: *Context, index: usize, size: u64, files: usize) void {
        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();
        var slices = self.directories.slice();
        slices.items(.total_size)[index] = size;
        slices.items(.total_files)[index] = files;
        slices.items(.total_dirs)[index] = 1;
    }

    fn markInaccessible(self: *Context, index: usize) void {
        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();
        var slices = self.directories.slice();
        const fdrefs = slices.items(.fdrefcount);
        const ref_ptr = &fdrefs[index];
        const current_fd = slices.items(.fd)[index];
        if (current_fd != invalid_fd) {
            std.posix.close(current_fd);
            slices.items(.fd)[index] = invalid_fd;
        }
        ref_ptr.store(0, .release);
        slices.items(.inaccessible)[index] = true;
        slices.items(.total_size)[index] = 0;
        slices.items(.total_files)[index] = 0;
        slices.items(.total_dirs)[index] = 1;
    }

    fn addRoot(self: *Context, path: []const u8, dir: ?std.fs.Dir, inaccessible: bool) !usize {
        var dir_copy = dir;
        errdefer if (dir_copy) |*d| d.close();

        const name_copy = try self.internPath(path);

        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();

        const index = try self.directories.addOne(self.allocator);
        var slices = self.directories.slice();
        slices.items(.parent)[index] = 0;
        slices.items(.basename)[index] = name_copy;
        slices.items(.fd)[index] = if (dir_copy) |d| d.fd else invalid_fd;
        slices.items(.total_size)[index] = 0;
        slices.items(.total_files)[index] = 0;
        slices.items(.total_dirs)[index] = 1;
        slices.items(.inaccessible)[index] = inaccessible;
        slices.items(.fdrefcount)[index] = AtomicU16.init(0);

        return index;
    }

    /// Caller must hold directories_mutex.
    fn addChild(self: *Context, parent_index: usize, name: []const u8) !usize {
        const name_copy = try self.internPath(name);

        const index = try self.directories.addOne(self.allocator);
        var slices = self.directories.slice();
        slices.items(.parent)[index] = @intCast(parent_index);
        slices.items(.basename)[index] = name_copy;
        slices.items(.fd)[index] = invalid_fd;
        slices.items(.total_size)[index] = 0;
        slices.items(.total_files)[index] = 0;
        slices.items(.total_dirs)[index] = 1;
        slices.items(.inaccessible)[index] = false;
        slices.items(.fdrefcount)[index] = AtomicU16.init(0);

        return index;
    }

    fn setDirectoryFd(self: *Context, index: usize, fd: std.posix.fd_t) void {
        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();
        var slices = self.directories.slice();
        slices.items(.fd)[index] = fd;
    }

    /// Caller must hold directories_mutex.
    fn closeDirectory(self: *Context, index: usize) void {
        var slices = self.directories.slice();
        const fd = slices.items(.fd)[index];
        if (fd != invalid_fd) {
            std.posix.close(fd);
            slices.items(.fd)[index] = invalid_fd;
        }
    }

    /// Caller must hold directories_mutex.
    fn fdRefAdd(self: *Context, index: usize, value: u16, comptime order: std.builtin.AtomicOrder) u16 {
        var slices = self.directories.slice();
        return slices.items(.fdrefcount)[index].fetchAdd(value, order);
    }

    /// Caller must hold directories_mutex.
    fn fdRefSub(self: *Context, index: usize, value: u16, comptime order: std.builtin.AtomicOrder) u16 {
        var slices = self.directories.slice();
        return slices.items(.fdrefcount)[index].fetchSub(value, order);
    }

    fn scheduleDirectory(self: *Context, index: usize) !void {
        _ = self.outstanding.fetchAdd(1, .acq_rel);
        errdefer _ = self.outstanding.fetchSub(1, .acq_rel);
        try self.task_queue.push(self.allocator, index);
        _ = self.stats.directories_scheduled.fetchAdd(1, .monotonic);
    }

    fn buildPathOwned(self: *Context, allocator: std.mem.Allocator, index: usize) ![]u8 {
        var buffer = std.ArrayList(u8){};
        defer buffer.deinit(allocator);
        try self.appendPathRecursive(&buffer, index);
        return buffer.toOwnedSlice(allocator);
    }

    fn appendPathRecursive(self: *Context, buffer: *std.ArrayList(u8), index: usize) !void {
        const node = self.getNode(index);
        const basename = node.basename;
        if (index != 0) {
            try self.appendPathRecursive(buffer, @intCast(node.parent));
            try buffer.append(self.allocator, '/');
        }
        try buffer.appendSlice(
            self.allocator,
            std.mem.sliceTo(self.namedata.items[basename..], 0),
        );
    }
};

fn directoryWorker(ctx: *Context) void {
    var path_buffer = std.ArrayList(u8){};
    defer path_buffer.deinit(ctx.allocator);

    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    var errprogress = ctx.errprogress;
    //    var speedometer = ctx.progress_node.start("kHz", 0);
    var marquee = ctx.progress_node.start("...", 0);
    defer errprogress.end();
    //    defer speedometer.end();
    defer marquee.end();

    var timer = std.time.Timer.start() catch unreachable;

    var namebuf: [std.fs.max_name_bytes]u8 = undefined;
    //    var progbuf: [256]u8 = undefined;
    // var mytotalsize: u64 = 0;

    var i: usize = 0;
    while (true) {
        const maybe_index = ctx.task_queue.pop();
        if (maybe_index == null) break;
        const index = maybe_index.?;

        if (@mod(i, 100) == 0) {
            const basename = takename(ctx, ctx.getNode(index).basename, &namebuf);

            const dt_ns = timer.lap();
            const speed = i * 1_000_000_000 / @max(dt_ns, 1);
            w.print("{d: >5} Hz  {s}", .{ speed, basename }) catch unreachable;
            marquee.setName(w.buffered());
            w.end = 0;
            //            speedometer.setCompletedItems(speed / 1000);
            i = 1;
        }
        i += 1;

        //        progress.increaseEstimatedTotalItems(1);

        _ = processDirectory(ctx, &ctx.progress_node, &errprogress, index) catch unreachable;
        //        progress.setCompletedItems(n);

        if (ctx.outstanding.fetchSub(1, .acq_rel) == 1) {
            ctx.task_queue.close();
        }
    }
}

fn takename(ctx: *Context, idx: u32, buffer: []u8) [:0]const u8 {
    ctx.namelock.lock();
    defer ctx.namelock.unlock();

    const slice = std.mem.sliceTo(ctx.namedata.items[idx..], 0);
    @memcpy(buffer[0..slice.len], slice);
    buffer[slice.len] = 0;

    return buffer[0..slice.len :0];
}

fn processDirectory(
    ctx: *Context,
    progress: *std.Progress.Node,
    errprogress: *std.Progress.Node,
    index: usize,
) !usize {
    _ = ctx.stats.directories_started.fetchAdd(1, .monotonic);
    defer _ = ctx.stats.directories_completed.fetchAdd(1, .monotonic);

    var buf: [1024 * 1024]u8 = undefined;
    var namebuf: [std.fs.max_name_bytes]u8 = undefined;

    const node = ctx.getNode(index);
    const basename = takename(ctx, node.basename, &namebuf);

    // var scanprogress = progress.start(basename, 0);
    // defer scanprogress.end();

    var dir_fd: std.posix.fd_t = undefined;
    if (index != 0) {
        const parent_index: usize = @intCast(node.parent);
        const parent = ctx.getNode(parent_index);
        const opened = wtfs.openSubdirectory(parent.fd, basename);

        ctx.directories_mutex.lock();
        const refcnt = ctx.fdRefSub(parent_index, 1, .seq_cst);
        if (refcnt == 1) {
            ctx.closeDirectory(parent_index);
        }
        ctx.directories_mutex.unlock();

        if (opened) |fd| {
            dir_fd = fd;
            ctx.setDirectoryFd(index, fd);
        } else |err| {
            switch (err) {
                error.PermissionDenied, error.AccessDenied => {
                    _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                    ctx.markInaccessible(index);
                    return 0;
                },
                else => {
                    errprogress.setName(@errorName(err));
                    errprogress.completeOne();
                    return err;
                },
            }
        }
    } else {
        const opened = std.posix.openat(node.fd, basename, .{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        }, 0) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                progress.completeOne();
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            },
            else => {
                errprogress.setName(@errorName(err));
                errprogress.completeOne();
                return err;
            },
        };
        if (node.fd != invalid_fd) {
            std.posix.close(node.fd);
        }
        dir_fd = opened;
        ctx.setDirectoryFd(index, opened);
    }

    var scanner = Scanner.init(dir_fd, &buf);
    var total_size: u64 = 0;
    var total_files: usize = 0;

    ctx.directories_mutex.lock();
    _ = ctx.fdRefAdd(index, 1, .acq_rel);
    ctx.directories_mutex.unlock();

    while (true) {
        const has_batch = scanner.fill() catch |err| {
            _ = ctx.stats.scanner_errors.fetchAdd(1, .monotonic);
            errprogress.completeOne();
            if (err == error.DeadLock) {
                errprogress.setName("dataless file");
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            } else {
                errprogress.setName(@errorName(err));
                return err;
            }
        };
        if (!has_batch) break;

        ctx.directories_mutex.lock();
        var lock_released = false;
        defer if (!lock_released) ctx.directories_mutex.unlock();

        var batch_entries: usize = 0;
        var batch_dirs: usize = 0;
        var batch_files: usize = 0;
        var batch_symlinks: usize = 0;
        var batch_other: usize = 0;

        while (true) {
            const maybe_entry = scanner.next() catch |err| {
                ctx.directories_mutex.unlock();
                lock_released = true;
                _ = ctx.stats.scanner_errors.fetchAdd(1, .monotonic);
                errprogress.completeOne();
                if (err == error.DeadLock) {
                    errprogress.setName("dataless file");
                    _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                    ctx.markInaccessible(index);
                    return 0;
                } else {
                    errprogress.setName(@errorName(err));
                    return err;
                }
            };

            const entry = maybe_entry orelse break;
            batch_entries += 1;
            const name = std.mem.sliceTo(entry.name, 0);
            if (name.len == 0) continue;
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

            switch (entry.kind) {
                .dir => {
                    if (ctx.skip_hidden and name[0] == '.') continue;
                    _ = ctx.fdRefAdd(index, 1, .acq_rel);
                    const child_index = try ctx.addChild(index, name);
                    try ctx.scheduleDirectory(child_index);
                    batch_dirs += 1;
                },
                .file => {
                    const file = entry.details.file;
                    total_size += file.allocsize;
                    total_files += 1;
                    batch_files += 1;
                },
                .symlink => batch_symlinks += 1,
                .other => batch_other += 1,
            }
        }

        ctx.directories_mutex.unlock();
        lock_released = true;

        _ = ctx.stats.scanner_batches.fetchAdd(1, .monotonic);
        _ = ctx.stats.scanner_entries.fetchAdd(batch_entries, .monotonic);
        _ = ctx.stats.directories_discovered.fetchAdd(batch_dirs, .monotonic);
        _ = ctx.stats.files_discovered.fetchAdd(batch_files, .monotonic);
        _ = ctx.stats.symlinks_discovered.fetchAdd(batch_symlinks, .monotonic);
        _ = ctx.stats.other_discovered.fetchAdd(batch_other, .monotonic);
        _ = ctx.stats.scanner_max_batch.fetchMax(batch_entries, .acq_rel);
    }

    ctx.directories_mutex.lock();
    const refcnt = ctx.fdRefSub(index, 1, .acq_rel);
    if (refcnt == 1) {
        ctx.closeDirectory(index);
    }
    ctx.directories_mutex.unlock();

    ctx.setTotals(index, total_size, total_files);

    return total_size;
}

pub fn main() !void {
    const gpa = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const exe_name = if (args.len > 0)
        std.mem.sliceTo(args[0], 0)
    else
        "wtfs";

    var skip_hidden = false;
    var root_arg: ?[]const u8 = null;

    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        const arg = std.mem.sliceTo(args[arg_index], 0);
        if (std.mem.eql(u8, arg, "--skip-hidden")) {
            skip_hidden = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--help")) {
            try stdout.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        if (root_arg != null) {
            try stdout.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        root_arg = arg;
    }

    const root = root_arg orelse ".";

    var directories = DirectoryStore{};
    defer directories.deinit(allocator);

    var namedata = std.ArrayList(u8).empty;
    defer namedata.deinit(allocator);

    var idxset = strpool.IndexSet.empty;
    defer idxset.deinit(allocator);

    // forbid on-demand file content loading; don't pull in data from iCloud
    if (builtin.target.os.tag == .macos) {
        switch (wtfs.setiopolicy_np(.vfs_materialize_dataless_files, .process, 1)) {
            0 => {},
            else => |rc| {
                std.debug.panic("setiopolicy_np: {t}", .{std.posix.errno(rc)});
            },
        }
    }

    var progress_root = std.Progress.start(.{ .root_name = "Scanning" });
    errdefer progress_root.end();

    var queue_progress = progress_root.start("Work queue", 0);
    errdefer queue_progress.end();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
    });
    defer pool.deinit();

    var wait_group: std.Thread.WaitGroup = .{};

    var ctx = Context{
        .allocator = allocator,
        .directories = &directories,
        .pool = &pool,
        .wait_group = &wait_group,
        .progress_node = queue_progress,
        .errprogress = progress_root.start("errors", 0),
        .skip_hidden = skip_hidden,
        .namedata = &namedata,
        .idxset = &idxset,
        .task_queue = .{
            .progress = &queue_progress,
        },
        .stats = Stats.init(),
    };

    defer ctx.task_queue.deinit(allocator);

    progress_root.setEstimatedTotalItems(1);

    var root_inaccessible = false;
    const root_dir: ?std.fs.Dir = blk: {
        const dir_result = std.fs.cwd().openDir(root, .{ .iterate = true }) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                root_inaccessible = true;
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                break :blk null;
            },
            else => return err,
        };
        break :blk dir_result;
    };

    const root_index = try ctx.addRoot(".", root_dir, root_inaccessible);

    var total_timer = std.time.Timer.start() catch unreachable;

    if (root_inaccessible) {
        ctx.markInaccessible(root_index);
        progress_root.completeOne();
        ctx.task_queue.close();
    } else {
        try ctx.scheduleDirectory(root_index);
    }

    const worker_count = if (builtin.single_threaded) 1 else pool.threads.len;
    var i: usize = 0;
    while (i < worker_count) : (i += 1) {
        pool.spawnWg(&wait_group, directoryWorker, .{&ctx});
    }

    wait_group.wait();

    const elapsed_ns = total_timer.read();

    const dir_count = ctx.directories.len;
    progress_root.setName("Summarizing");
    progress_root.setEstimatedTotalItems(dir_count);
    ctx.directories_mutex.lock();
    var slices = ctx.directories.slice();
    var idx = dir_count;
    while (idx > 0) {
        idx -= 1;
        if (idx != 0) {
            const parent_index: usize = @intCast(slices.items(.parent)[idx]);
            slices.items(.total_size)[parent_index] += slices.items(.total_size)[idx];
            slices.items(.total_files)[parent_index] += slices.items(.total_files)[idx];
            slices.items(.total_dirs)[parent_index] += slices.items(.total_dirs)[idx];
        }
        progress_root.completeOne();
    }

    const directories_including_root = slices.items(.total_dirs)[0];
    const files_total = slices.items(.total_files)[0];
    const bytes_total = slices.items(.total_size)[0];
    ctx.directories_mutex.unlock();
    const inaccessible_dirs = ctx.inaccessible_dirs.load(.acquire);

    const SummaryEntry = struct {
        index: usize,
        path: []u8,
    };

    var top_level_entries = std.ArrayList(SummaryEntry){};
    progress_root.setName("Building paths");
    progress_root.setEstimatedTotalItems(dir_count);
    var depth_index: usize = 0;
    while (depth_index < dir_count) : (depth_index += 1) {
        if (depth_index == 0) {
            progress_root.completeOne();
            continue;
        }
        const node = ctx.getNode(depth_index);
        if (node.parent == 0) {
            const path_copy = try ctx.buildPathOwned(allocator, depth_index);
            try top_level_entries.append(allocator, .{ .index = depth_index, .path = path_copy });
        }
        progress_root.completeOne();
    }

    progress_root.end();

    const SortContext = struct {
        ctx: *Context,
        pub fn lessThan(self: @This(), lhs: SummaryEntry, rhs: SummaryEntry) bool {
            const lhs_node = self.ctx.getNode(lhs.index);
            const rhs_node = self.ctx.getNode(rhs.index);
            if (lhs_node.total_size == rhs_node.total_size) {
                return std.mem.lessThan(u8, lhs.path, rhs.path);
            }
            return lhs_node.total_size > rhs_node.total_size;
        }
    };

    if (top_level_entries.items.len > 1) {
        std.sort.heap(
            SummaryEntry,
            top_level_entries.items,
            SortContext{ .ctx = &ctx },
            SortContext.lessThan,
        );
    }

    try stdout.print("{s}: {d} dirs, {d} files, {Bi: <.1} total\n", .{
        root,
        directories_including_root,
        files_total,
        bytes_total,
    });

    if (inaccessible_dirs > 0) {
        try stdout.print("  Inaccessible directories: {d}\n", .{inaccessible_dirs});
    }

    const stats_ptr = &ctx.stats;
    const dirs_started = stats_ptr.directories_started.load(.acquire);
    const dirs_completed = stats_ptr.directories_completed.load(.acquire);
    const dirs_scheduled = stats_ptr.directories_scheduled.load(.acquire);
    const dirs_discovered = stats_ptr.directories_discovered.load(.acquire);
    const files_discovered = stats_ptr.files_discovered.load(.acquire);
    const symlinks_discovered = stats_ptr.symlinks_discovered.load(.acquire);
    const other_discovered = stats_ptr.other_discovered.load(.acquire);
    const batches = stats_ptr.scanner_batches.load(.acquire);
    const batch_entries = stats_ptr.scanner_entries.load(.acquire);
    const max_batch = stats_ptr.scanner_max_batch.load(.acquire);
    const scanner_errors = stats_ptr.scanner_errors.load(.acquire);
    const queue_high = ctx.task_queue.high_watermark.load(.acquire);

    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
    const dirs_per_sec = if (elapsed_ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(dirs_completed)) / elapsed_s;

    const avg_batch_entries = if (batches == 0)
        0.0
    else
        @as(f64, @floatFromInt(batch_entries)) / @as(f64, @floatFromInt(batches));

    try stdout.print("  Duration: {d:.2}s  ({d:.1} dirs/s)\n", .{ elapsed_s, dirs_per_sec });
    try stdout.print(
        "  Queue peak: {d} tasks  |  Scheduled dirs: {d}  discovered: {d}  started/completed: {d}/{d}\n",
        .{ queue_high, dirs_scheduled, dirs_discovered, dirs_started, dirs_completed },
    );
    try stdout.print(
        "  Batches: {d}  avg entries/batch: {d:.1}  max: {d}\n",
        .{ batches, avg_batch_entries, max_batch },
    );
    try stdout.print(
        "  Entries seen: files {d}, symlinks {d}, other {d}\n",
        .{ files_discovered, symlinks_discovered, other_discovered },
    );
    if (scanner_errors > 0) {
        try stdout.print("  Scanner errors: {d}\n", .{scanner_errors});
    }

    try stdout.print("\n", .{});

    if (top_level_entries.items.len > 0) {
        try stdout.print("Top-level directories by total size:\n\n", .{});
        const max_show = @min(top_level_entries.items.len, 10);
        var display_index: usize = 0;
        while (display_index < max_show) : (display_index += 1) {
            const entry = top_level_entries.items[display_index];
            const node = ctx.getNode(entry.index);
            const marker = if (node.inaccessible) " (inaccessible)" else "";
            try stdout.print(
                "{d: >9.1}MiB  {s: <32}{s}  {d: >6} files\n",
                .{
                    @as(f64, @floatFromInt(node.total_size)) / 1024 / 1024,
                    entry.path,
                    marker,
                    node.total_files,
                },
            );
        }
    }

    try stdout.flush();
}
