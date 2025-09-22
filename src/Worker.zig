const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Context = @import("Context.zig");
const dirscan = @import("DirScanner.zig");
const SysDispatcher = @import("SysDispatcher.zig");

const Worker = @This();

// ===== Configuration =====

/// Scanner configured to read names, object types, and file sizes.
/// On Linux the provider points at the dispatcher so statx can run via
/// io_uring, otherwise it collapses to the POSIX defaults.
const Scanner = dirscan.DirScannerWithProvider(.{
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
}, dirscan.DispatcherStatProvider);

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

/// Platform syscall dispatcher used for directory opens and metadata
dispatcher: SysDispatcher.Backend,

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
        .dispatcher = undefined,
        .path_buffer = std.ArrayList(u8).empty,
        .progress_buffer = undefined,
        .progress_writer = undefined,
        .namebuf = undefined,
        .scan_buffer = undefined,
        .timer = std.time.Timer.start() catch |e| {
            std.debug.panic("timer start failed {t}", .{e});
        },
        .items_processed = 0,
    };
    defer worker.path_buffer.deinit(worker.allocator);

    worker.dispatcher.init(.{
        .allocator = worker.allocator,
        .entries = null,
    }) catch |err| {
        std.debug.panic("worker dispatcher init failed: {s}", .{@errorName(err)});
    };

    defer worker.dispatcher.deinit();
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
    const basename = self.extractName(
        self.ctx.directories.ptr(.basename, index).*,
    );

    if (index != 0) {
        return try self.openChildDirectory(index, basename);
    } else {
        return try self.openRootDirectory(index, basename);
    }
}

fn openChildDirectory(
    self: *Worker,
    index: usize,
    basename: [:0]const u8,
) !std.posix.fd_t {
    const parent_index: usize = @intCast(self.ctx.directories.ptr(.parent, index).*);
    const parent = self.ctx.directories.ptr(.fd, parent_index).*;
    const opened = self.dispatcher.openDirectory(parent, basename);

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
    basename: [:0]const u8,
) !std.posix.fd_t {
    const fd = self.ctx.directories.ptr(.fd, index).*;
    const opened = self.dispatcher.openDirectory(fd, basename) catch |err| switch (err) {
        error.PermissionDenied, error.AccessDenied => {
            self.progress.completeOne();
            self.ctx.markInaccessible(index);
            return Context.invalid_fd;
        },
        else => {
            self.errprogress.setName(@errorName(err));
            self.errprogress.completeOne();
            return err;
        },
    };

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
    // Iterate using a duplicate so the context can safely close the original fd.
    const iter_fd = posix.dup(dir_fd) catch |err| {
        return self.handleScanError(index, err);
    };
    var scanner = Scanner.init(iter_fd, &self.scan_buffer, &self.dispatcher);
    defer scanner.dir.close();

    self.ctx.retainParentFd(index);
    defer self.ctx.releaseParentFd(index);

    const node = self.progress.start("scan", 0);
    defer node.end();

    while (true) {
        const has_entry = scanner.fill() catch |err| {
            return self.handleScanError(index, err);
        };
        if (!has_entry) break;

        if (comptime builtin.target.os.tag == .macos) {
            node.completeOne();
            node.increaseEstimatedTotalItems(scanner.n);
        } else {
            node.increaseEstimatedTotalItems(1);
        }

        try self.processBatch(index, &scanner, metrics, node);
    }
}

fn processBatch(
    self: *Worker,
    index: usize,
    scanner: *Scanner,
    metrics: *ScanMetrics,
    node: std.Progress.Node,
) !void {
    var batch_entries: usize = 0;
    var batch_metrics = ScanMetrics{};

    while (true) {
        const entrynode = node.start("entry", 0);
        defer entrynode.end();

        const maybe_entry = scanner.next() catch |err| {
            return self.handleScanError(index, err);
        };

        const entry = maybe_entry orelse {
            break;
        };
        batch_entries += 1;

        try self.processEntry(index, entry, metrics, &batch_metrics);
    }
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
            self.ctx.retainParentFd(index);
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
                try self.ctx.recordLargeFile(index, name, file.allocsize);
            }
        },
        .symlink => batch_metrics.batch_symlinks += 1,
        .other => batch_metrics.batch_other += 1,
    }
}

pub fn addChild(self: *Worker, parent_index: usize, name: []const u8) !usize {
    const name_copy = try self.ctx.internPath(name);
    self.ctx.directories_mutex.lock();
    defer self.ctx.directories_mutex.unlock();
    const index = try self.ctx.directories.addOne(self.allocator);
    self.ctx.directories.ptr(.parent, index).* = @intCast(parent_index);
    self.ctx.directories.ptr(.basename, index).* = name_copy;
    self.ctx.directories.ptr(.fd, index).* = Context.invalid_fd;
    self.ctx.directories.ptr(.total_size, index).* = 0;
    self.ctx.directories.ptr(.total_files, index).* = 0;
    self.ctx.directories.ptr(.total_dirs, index).* = 1;
    self.ctx.directories.ptr(.inaccessible, index).* = false;
    self.ctx.directories.ptr(.fdrefcount, index).* = Context.AtomicU16.init(0);

    return index;
}

pub fn scheduleDirectory(self: *Worker, index: usize) !void {
    _ = self.ctx.outstanding.fetchAdd(1, .acq_rel);
    errdefer _ = self.ctx.outstanding.fetchSub(1, .acq_rel);
    try self.ctx.task_queue.push(self.allocator, index);
}

fn handleOpenError(
    self: *Worker,
    index: usize,
    err: anyerror,
) !std.posix.fd_t {
    switch (err) {
        error.PermissionDenied, error.AccessDenied, error.FileNotFound => {
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
    self.errprogress.completeOne();

    if (err == error.DeadLock) {
        self.errprogress.setName("dataless file");
        self.ctx.markInaccessible(index);
        return;
    } else if (err == error.PermissionDenied or err == error.AccessDenied) {
        self.errprogress.setName("permission denied");
        self.ctx.markInaccessible(index);
        return;
    } else if (err == error.FileNotFound) {
        self.errprogress.setName("file not found");
        self.ctx.markInaccessible(index);
        return;
    } else {
        self.errprogress.setName(@errorName(err));
        return err;
    }
}
