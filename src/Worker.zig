const std = @import("std");
const Context = @import("Context.zig");
const wtfs = @import("wtfs").mac;

const Worker = @This();

// ===== Configuration =====

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

// ===== Types =====

const DirectoryError = error{
    PermissionDenied,
    AccessDenied,
    DeadLock,
};

const ScanMetrics = struct {
    total_size: u64 = 0,
    total_files: usize = 0,
    batch_dirs: usize = 0,
    batch_files: usize = 0,
    batch_symlinks: usize = 0,
    batch_other: usize = 0,
};

// ===== Worker Instance Fields =====

/// Reference to the shared context containing all shared state
ctx: *Context,

/// Local allocator reference for convenience
allocator: std.mem.Allocator,

/// Root progress node for thread pool
progress: std.Progress.Node,
/// Separate global error progress node
errprogress: std.Progress.Node,

/// Buffers for various operations
path_buffer: std.ArrayList(u8),
progress_buffer: [256]u8,
progress_writer: std.Io.Writer,
namebuf: [std.fs.max_name_bytes]u8,
scan_buffer: [1024 * 1024]u8,

/// Performance tracking
timer: std.time.Timer,
items_processed: usize,

// ===== Main Worker Loop =====

const dance_chars = "abcdefghijklmnopqrstuvwxyz";

/// Main worker thread that processes directories from the task queue
pub fn directoryWorker(ctx: *Context) void {
    var worker = Worker{
        .ctx = ctx,
        .allocator = ctx.allocator,
        .progress = ctx.progress_node,
        .errprogress = ctx.errprogress,
        .path_buffer = std.ArrayList(u8){},
        .progress_buffer = undefined,
        .progress_writer = undefined,
        .namebuf = undefined,
        .scan_buffer = undefined,
        .timer = std.time.Timer.start() catch unreachable,
        .items_processed = 0,
    };
    defer worker.path_buffer.deinit(worker.allocator);

    worker.progress_writer = std.Io.Writer.fixed(&worker.progress_buffer);

    while (worker.ctx.task_queue.pop()) |index| {
        worker.processTask(index);
        worker.items_processed += 1;

        // Mark task complete and check if we're done
        if (worker.ctx.outstanding.fetchSub(1, .acq_rel) == 1) {
            worker.ctx.task_queue.close();
        }
    }
}

fn processTask(self: *Worker, index: usize) void {
    _ = self.processDirectory(index) catch |err| {
        std.debug.panic("error {t}", .{err});
    };
}

// ===== Name Management =====

/// Extract a name from the shared name pool (thread-safe)
fn extractName(self: *Worker, idx: u32) [:0]const u8 {
    self.ctx.namelock.lock();
    defer self.ctx.namelock.unlock();

    const slice = std.mem.sliceTo(self.ctx.namedata.items[idx..], 0);
    @memcpy(self.namebuf[0..slice.len], slice);
    self.namebuf[slice.len] = 0;

    return self.namebuf[0..slice.len :0];
}

// ===== Directory Processing =====

/// Process a single directory, scanning its contents and scheduling subdirectories
fn processDirectory(self: *Worker, index: usize) !usize {
    _ = self.ctx.stats.directories_started.fetchAdd(1, .monotonic);
    defer _ = self.ctx.stats.directories_completed.fetchAdd(1, .monotonic);

    const dir_fd = try self.openDirectory(index);
    if (dir_fd == Context.invalid_fd) return 0; // Directory was inaccessible

    var metrics = ScanMetrics{};
    try self.scanDirectory(index, dir_fd, &metrics);

    // Store the totals for this directory
    self.ctx.setTotals(index, metrics.total_size, metrics.total_files);

    return metrics.total_size;
}

// ===== Directory Opening =====

fn openDirectory(self: *Worker, index: usize) !std.posix.fd_t {
    // const prevname = self.marquee.getName();
    // self.marquee.setName("prep");
    // defer self.marquee.setName(&prevname);

    const node = self.ctx.getNode(index);
    const basename = self.extractName(node.basename);

    if (index != 0) {
        return try self.openChildDirectory(index, node, basename);
    } else {
        return try self.openRootDirectory(index, node, basename);
    }
}

fn openChildDirectory(
    self: *Worker,
    index: usize,
    node: Context.DirectoryNode,
    basename: [:0]const u8,
) !std.posix.fd_t {
    //    const prevname = self.marquee.getName();
    //    self.marquee.setName("opendir");
    //    defer self.marquee.setName(&prevname);
    const parent_index: usize = @intCast(node.parent);
    const parent = self.ctx.getNode(parent_index);

    // Try to open the subdirectory using parent's fd
    const opened = wtfs.openSubdirectory(parent.fd, basename);

    // We've successfully used the parent fd for openat(), release our reference
    self.ctx.releaseParentFdAfterOpen(parent_index);

    if (opened) |fd| {
        self.ctx.setDirectoryFd(index, fd);
        return fd;
    } else |err| {
        return self.handleOpenError(index, err);
    }
}

fn openRootDirectory(
    self: *Worker,
    index: usize,
    node: Context.DirectoryNode,
    basename: [:0]const u8,
) !std.posix.fd_t {
    const opened = std.posix.openat(node.fd, basename, .{
        .NONBLOCK = true,
        .DIRECTORY = true,
        .NOFOLLOW = true,
    }, 0) catch |err| switch (err) {
        error.PermissionDenied, error.AccessDenied => {
            self.progress.completeOne();
            _ = self.ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
            self.ctx.markInaccessible(index);
            return Context.invalid_fd;
        },
        else => {
            self.errprogress.setName(@errorName(err));
            self.errprogress.completeOne();
            return err;
        },
    };

    // Close old fd if present
    if (node.fd != Context.invalid_fd) {
        std.posix.close(node.fd);
    }

    self.ctx.setDirectoryFd(index, opened);
    return opened;
}

// ===== Directory Scanning =====

fn scanDirectory(
    self: *Worker,
    index: usize,
    dir_fd: std.posix.fd_t,
    metrics: *ScanMetrics,
) !void {
    var scanner = Scanner.init(dir_fd, &self.scan_buffer);

    self.ctx.retainParentFd(index);
    defer self.ctx.releaseParentFd(index);

    const node = self.progress.start("scan", 0);
    defer node.end();

    while (true) {
        if (scanner.n == 0) self.refill(&scanner, node) catch |err| {
            return self.handleScanError(index, err);
        };
        if (scanner.n == 0) break;
        node.increaseEstimatedTotalItems(scanner.n);

        try self.processBatch(index, &scanner, metrics, node);
    }
}

/// Fetch a new batch of entries from the kernel
fn refill(self: *Worker, scanner: *Scanner, node: std.Progress.Node) !void {
    const opts_mask = wtfs.FsOptMask{
        .nofollow = true,
        .report_fullsize = true,
        .pack_invalid_attrs = true,
    };

    var al = wtfs.AttrList{
        .bitmapcount = wtfs.ATTR_BIT_MAP_COUNT,
        .reserved = 0,
        .attrs = Scanner.Mask,
    };

    var i: usize = 1;

    while (true) {
        self.progress_writer = std.Io.Writer.fixed(&self.progress_buffer);
        self.progress_writer.print("fill #{d:<4} {d}K", .{ i, scanner.buf.len / 1024 }) catch unreachable;
        self.progress_writer = undefined;

        const n = wtfs.getattrlistbulk(scanner.fd, &al, scanner.buf.ptr, scanner.buf.len, opts_mask);
        node.completeOne();

        i += 1;

        if (n < 0) {
            switch (std.posix.errno(n)) {
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
                .DEADLK => return error.DeadLock, // iCloud dataless file
                else => |e| std.debug.panic("unexpected errno {t}", .{e}),
            }
        }

        scanner.n = @abs(n);
        scanner.reader = std.io.Reader.fixed(scanner.buf);

        return;
    }
}

fn processBatch(
    self: *Worker,
    index: usize,
    scanner: *Scanner,
    metrics: *ScanMetrics,
    node: std.Progress.Node,
) !void {
    self.ctx.directories_mutex.lock();

    var lock_released = false;
    defer if (!lock_released) self.ctx.directories_mutex.unlock();

    var batch_entries: usize = 0;
    var batch_metrics = ScanMetrics{};

    while (true) {
        const entrynode = node.start("entry", 0);

        const maybe_entry = scanner.next() catch |err| {
            self.ctx.directories_mutex.unlock();
            lock_released = true;
            return self.handleScanError(index, err);
        };

        const entry = maybe_entry orelse {
            entrynode.end();
            break;
        };
        batch_entries += 1;

        try self.processEntry(index, entry, metrics, &batch_metrics);
        entrynode.end();
    }

    self.ctx.directories_mutex.unlock();
    lock_released = true;

    self.updateStatistics(batch_entries, &batch_metrics);
}

fn processEntry(
    self: *Worker,
    index: usize,
    entry: Scanner.Entry,
    metrics: *ScanMetrics,
    batch_metrics: *ScanMetrics,
) !void {
    const name = std.mem.sliceTo(entry.name, 0);

    // Skip empty, current, and parent directory entries
    if (name.len == 0) return;
    if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) return;

    switch (entry.kind) {
        .dir => {
            if (self.ctx.skip_hidden and name[0] == '.') return;
            // Keep parent fd open until child task can openat() from it
            // (We already hold the mutex in processBatch, so use the locked version)
            self.ctx.retainParentFdLocked(index);
            const child_index = try self.addChild(index, name);
            try self.scheduleDirectory(child_index);
            batch_metrics.batch_dirs += 1;
        },
        .file => {
            const file = entry.details.file;
            metrics.total_size += file.allocsize;
            metrics.total_files += 1;
            batch_metrics.batch_files += 1;

            if (file.allocsize >= self.ctx.large_file_threshold) {
                try self.ctx.recordLargeFileLocked(index, name, file.allocsize);
            }
        },
        .symlink => batch_metrics.batch_symlinks += 1,
        .other => batch_metrics.batch_other += 1,
    }
}

/// Caller must hold directories_mutex.
pub fn addChild(self: *Worker, parent_index: usize, name: []const u8) !usize {
    const name_copy = try self.ctx.internPath(name);
    const index = try self.ctx.directories.addOne(self.allocator);
    var slices = self.ctx.directories.slice();
    slices.items(.parent)[index] = @intCast(parent_index);
    slices.items(.basename)[index] = name_copy;
    slices.items(.fd)[index] = Context.invalid_fd;
    slices.items(.total_size)[index] = 0;
    slices.items(.total_files)[index] = 0;
    slices.items(.total_dirs)[index] = 1;
    slices.items(.inaccessible)[index] = false;
    slices.items(.fdrefcount)[index] = .init(0);

    return index;
}

pub fn scheduleDirectory(self: *Worker, index: usize) !void {
    _ = self.ctx.outstanding.fetchAdd(1, .acq_rel);
    errdefer _ = self.ctx.outstanding.fetchSub(1, .acq_rel);
    try self.ctx.task_queue.push(self.allocator, index);
    _ = self.ctx.stats.directories_scheduled.fetchAdd(1, .monotonic);
}

fn updateStatistics(self: *Worker, batch_entries: usize, batch_metrics: *const ScanMetrics) void {
    _ = self.ctx.stats.scanner_batches.fetchAdd(1, .monotonic);
    _ = self.ctx.stats.scanner_entries.fetchAdd(batch_entries, .monotonic);
    _ = self.ctx.stats.directories_discovered.fetchAdd(batch_metrics.batch_dirs, .monotonic);
    _ = self.ctx.stats.files_discovered.fetchAdd(batch_metrics.batch_files, .monotonic);
    _ = self.ctx.stats.symlinks_discovered.fetchAdd(batch_metrics.batch_symlinks, .monotonic);
    _ = self.ctx.stats.other_discovered.fetchAdd(batch_metrics.batch_other, .monotonic);
    _ = self.ctx.stats.scanner_max_batch.fetchMax(batch_entries, .acq_rel);
}

// ===== Error Handling =====

fn handleOpenError(
    self: *Worker,
    index: usize,
    err: anyerror,
) !std.posix.fd_t {
    switch (err) {
        error.PermissionDenied, error.AccessDenied => {
            _ = self.ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
            self.ctx.markInaccessible(index);
            return Context.invalid_fd;
        },
        else => {
            self.errprogress.setName(@errorName(err));
            self.errprogress.completeOne();
            return err;
        },
    }
}

fn handleScanError(
    self: *Worker,
    index: usize,
    err: anyerror,
) !void {
    _ = self.ctx.stats.scanner_errors.fetchAdd(1, .monotonic);
    self.errprogress.completeOne();

    if (err == error.DeadLock) {
        self.errprogress.setName("dataless file");
        _ = self.ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
        self.ctx.markInaccessible(index);
        return;
    } else {
        self.errprogress.setName(@errorName(err));
        return err;
    }
}
