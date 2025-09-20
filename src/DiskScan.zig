const std = @import("std");
const builtin = @import("builtin");
const wtfs = @import("wtfs").mac;
const strpool = @import("wtfs").strpool;

const TaskQueue = @import("TaskQueue.zig");
const Context = @import("Context.zig");
const Worker = @import("Worker.zig");

const Self = @This();

// ===== Types =====

const SummaryEntry = struct {
    index: usize,
    path: []u8,
};

const Summary = struct {
    top_level: std.ArrayList(SummaryEntry),
    heaviest: std.ArrayList(SummaryEntry),

    fn deinit(self: *Summary, allocator: std.mem.Allocator) void {
        freeEntryList(allocator, &self.top_level);
        freeEntryList(allocator, &self.heaviest);
    }

    fn freeEntryList(allocator: std.mem.Allocator, list: *std.ArrayList(SummaryEntry)) void {
        if (list.items.len != 0) {
            for (list.items) |entry| {
                allocator.free(entry.path);
            }
        }
        list.deinit(allocator);
    }
};

const RootOpenResult = struct {
    dir: ?std.fs.Dir,
    inaccessible: bool,
};

const DirectoryTotals = struct {
    directories: u64,
    files: u64,
    bytes: u64,
};

const ScanResults = struct {
    stats: *const Context.Stats,
    elapsed_ns: u64,
    totals: DirectoryTotals,
};

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

// ===== Instance Fields =====

/// Memory allocator for all dynamic allocations during the scan
allocator: std.mem.Allocator,

/// Whether to skip hidden files and directories (starting with '.')
skip_hidden: bool = true,

/// Root directory path to scan
root: []const u8 = ".",

/// Multi-array storage for all discovered directory nodes and their metadata
directories: Context.DirectoryStore = .empty,

/// Pool of null-terminated directory names referenced by basename indices
namedata: std.ArrayList(u8) = .empty,

/// Set tracking unique name indices to avoid duplicates in the name pool
idxset: strpool.IndexSet = .empty,

/// Root progress node for tracking scan progress and reporting to the user
progress_root: std.Progress.Node = undefined,

/// Atomic statistics counters shared by all worker threads during scanning
stats: Context.Stats = Context.Stats.init(),

// ===== Platform Configuration =====

const PlatformConfig = struct {
    fn preventICloudDownload() void {
        if (builtin.target.os.tag == .macos) {
            // Forbid on-demand file content loading; don't pull in data from iCloud
            switch (wtfs.setiopolicy_np(.vfs_materialize_dataless_files, .process, 1)) {
                0 => {},
                else => |rc| {
                    std.debug.panic("setiopolicy_np: {t}", .{std.posix.errno(rc)});
                },
            }
        }
    }
};

// ===== Scanning Infrastructure =====

fn createScanContext(
    self: *Self,
    pool: *std.Thread.Pool,
    wait_group: *std.Thread.WaitGroup,
    queue_progress: *std.Progress.Node,
    task_queue: *TaskQueue,
) Context {
    return Context{
        .allocator = self.allocator,
        .pool = pool,
        .task_queue = task_queue,
        .directories = &self.directories,
        .namedata = &self.namedata,
        .idxset = &self.idxset,
        .wait_group = wait_group,
        .progress_node = queue_progress.*,
        .errprogress = self.progress_root.start("errors", 0),
        .skip_hidden = self.skip_hidden,
        .stats = &self.stats, // Pass pointer to stats
    };
}

fn initializeRootDirectory(self: *Self, ctx: *Context) !void {
    self.progress_root.setEstimatedTotalItems(1);

    const root_result = try self.openRootDirectory(ctx);
    const root_index = try ctx.addRoot(".", root_result.dir, root_result.inaccessible);

    if (root_result.inaccessible) {
        ctx.markInaccessible(root_index);
        self.progress_root.completeOne();
        ctx.task_queue.close();
    } else {
        try ctx.scheduleDirectory(root_index);
    }
}

// Main gathering phase
fn gatherPhase(self: *Self) !void {
    PlatformConfig.preventICloudDownload();

    var queue_progress = self.progress_root.start("Work queue", 0);
    errdefer queue_progress.end();

    var wait_group = std.Thread.WaitGroup{};

    var worker_pool: std.Thread.Pool = undefined;
    try worker_pool.init(.{
        .allocator = self.allocator,
        .stack_size = 16 * 1024 * 1024,
    });
    defer worker_pool.deinit();

    var task_queue = TaskQueue{
        .progress = &queue_progress,
    };

    var ctx = self.createScanContext(
        &worker_pool,
        &wait_group,
        &queue_progress,
        &task_queue,
    );
    defer ctx.task_queue.deinit(self.allocator);

    try self.initializeRootDirectory(&ctx);

    const n_workers = if (builtin.single_threaded) 1 else worker_pool.threads.len;

    for (0..n_workers) |_| {
        worker_pool.spawnWg(&wait_group, Worker.directoryWorker, .{&ctx});
    }

    defer wait_group.wait();
    // Stats are now stored directly in self.stats, no need to return
}

// ===== Directory Operations =====

fn openRootDirectory(self: *Self, ctx: *Context) !RootOpenResult {
    const dir_result = std.fs.cwd().openDir(
        self.root,
        .{ .iterate = true },
    ) catch |err| switch (err) {
        error.PermissionDenied, error.AccessDenied => {
            _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
            return .{ .dir = null, .inaccessible = true };
        },
        else => return err,
    };
    return .{ .dir = dir_result, .inaccessible = false };
}

// ===== Path Management =====
fn writeFullPath(
    self: *Self,
    index: usize,
    writer: *std.Io.Writer,
) !void {
    const basenameidx = self.directories.slice().items(.basename)[index];
    const parent = self.directories.slice().items(.parent)[index];

    if (index != 0) {
        try self.writeFullPath(@intCast(parent), writer);
        try writer.writeByte('/');
    }
    try writer.writeSliceSwap(
        u8,
        std.mem.sliceTo(self.namedata.items[basenameidx..], 0),
    );
}

fn buildPath(self: *Self, index: usize) ![]u8 {
    var writer = try std.Io.Writer.Allocating.initCapacity(self.allocator, 256);
    try self.writeFullPath(index, &writer.writer);
    return try writer.toOwnedSlice();
}

// ===== Statistics Processing =====

const StatsAggregator = struct {
    fn aggregateUp(self: *Self) void {
        var slices = self.directories.slice();
        var idx = self.directories.len;
        while (idx > 0) {
            idx -= 1;
            if (idx != 0) {
                const parent_index: usize = @intCast(slices.items(.parent)[idx]);
                slices.items(.total_size)[parent_index] += slices.items(.total_size)[idx];
                slices.items(.total_files)[parent_index] += slices.items(.total_files)[idx];
                slices.items(.total_dirs)[parent_index] += slices.items(.total_dirs)[idx];
            }
            self.progress_root.completeOne();
        }
    }

    fn extractTotals(self: *Self) DirectoryTotals {
        const slices = self.directories.slice();
        return .{
            .directories = slices.items(.total_dirs)[0],
            .files = slices.items(.total_files)[0],
            .bytes = slices.items(.total_size)[0],
        };
    }
};

// ===== Summary Generation =====

const SummaryBuilder = struct {
    fn buildTopLevelEntries(self: *Self) !std.ArrayList(SummaryEntry) {
        var entries = std.ArrayList(SummaryEntry){};
        errdefer Summary.freeEntryList(self.allocator, &entries);
        self.progress_root.setName("Building paths");
        self.progress_root.setEstimatedTotalItems(self.directories.len);

        var idx: usize = 0;
        while (idx < self.directories.len) : (idx += 1) {
            defer self.progress_root.completeOne();

            if (idx == 0) continue;

            const parent = self.directories.slice().items(.parent)[idx];
            if (parent == 0) {
                const path = try buildPath(self, idx);
                entries.append(self.allocator, .{
                    .index = idx,
                    .path = path,
                }) catch |err| {
                    self.allocator.free(path);
                    return err;
                };
            }
        }

        return entries;
    }

    fn buildHeaviestEntries(self: *Self, max_entries: usize) !std.ArrayList(SummaryEntry) {
        if (max_entries == 0) return std.ArrayList(SummaryEntry){};

        var top_indexes = std.ArrayList(usize){};
        defer top_indexes.deinit(self.allocator);

        const slices = self.directories.slice();
        const sizes = slices.items(.total_size);

        var idx: usize = 1; // Skip root
        while (idx < self.directories.len) : (idx += 1) {
            const size = sizes[idx];
            if (size == 0) continue;
            try SummaryBuilder.insertTopIndex(self, &top_indexes, idx, max_entries, sizes);
        }

        var entries = std.ArrayList(SummaryEntry){};
        errdefer Summary.freeEntryList(self.allocator, &entries);
        try entries.ensureTotalCapacityPrecise(self.allocator, top_indexes.items.len);

        for (top_indexes.items) |directory_index| {
            const path = try buildPath(self, directory_index);

            entries.appendAssumeCapacity(.{
                .index = directory_index,
                .path = path,
            });
        }

        return entries;
    }

    fn insertTopIndex(
        self: *Self,
        list: *std.ArrayList(usize),
        index: usize,
        max_entries: usize,
        sizes: []const u64,
    ) !void {
        const size = sizes[index];
        if (size == 0) return;

        if (list.items.len < max_entries) {
            try list.append(self.allocator, index);
            var pos = list.items.len - 1;
            while (pos > 0 and size > sizes[list.items[pos - 1]]) {
                list.items[pos] = list.items[pos - 1];
                pos -= 1;
            }
            list.items[pos] = index;
            return;
        }

        const smallest_index = list.items[max_entries - 1];
        if (size <= sizes[smallest_index]) return;

        list.items[max_entries - 1] = index;
        var pos: usize = max_entries - 1;
        while (pos > 0 and size > sizes[list.items[pos - 1]]) {
            list.items[pos] = list.items[pos - 1];
            pos -= 1;
        }
        list.items[pos] = index;
    }

    const EntrySorter = struct {
        directories: *Context.DirectoryStore,

        fn bySize(self: @This(), lhs: SummaryEntry, rhs: SummaryEntry) bool {
            const sizes = self.directories.items(.total_size);
            const lhs_size = sizes[lhs.index];
            const rhs_size = sizes[rhs.index];

            if (lhs_size != rhs_size) {
                return lhs_size > rhs_size;
            }
            return std.mem.lessThan(u8, lhs.path, rhs.path);
        }
    };

    fn sortBySize(self: *Self, entries: []SummaryEntry) void {
        if (entries.len > 1) {
            std.sort.heap(
                SummaryEntry,
                entries,
                EntrySorter{ .directories = &self.directories },
                EntrySorter.bySize,
            );
        }
    }
};

// ===== Output Formatting =====

const Reporter = struct {
    fn printHeader(self: *Self, results: ScanResults) !void {
        try stdout.print("{s}: {d} dirs, {d} files, {Bi: <.1} total\n\n", .{
            self.root,
            results.totals.directories,
            results.totals.files,
            results.totals.bytes,
        });
    }

    fn printStatistics(results: ScanResults) !void {
        const stats = results.stats;
        const elapsed_ns = results.elapsed_ns;

        const inaccessible = stats.inaccessible_dirs.load(.acquire);
        if (inaccessible > 0) {
            try stdout.print("  Inaccessible directories: {d}\n", .{inaccessible});
        }

        const metrics = extractMetrics(stats.*, elapsed_ns);

        try stdout.print("  Duration: {d:.2}s  ({d:.1} dirs/s)\n", .{
            metrics.elapsed_s,
            metrics.dirs_per_sec,
        });

        try printQueueMetrics(metrics);
        try printBatchMetrics(metrics);
        try printEntryMetrics(metrics);

        if (metrics.scanner_errors > 0) {
            try stdout.print("  Scanner errors: {d}\n", .{metrics.scanner_errors});
        }
        try stdout.print("\n", .{});
    }

    const Metrics = struct {
        elapsed_s: f64,
        dirs_per_sec: f64,
        avg_batch_entries: f64,
        dirs_started: u64,
        dirs_completed: u64,
        dirs_scheduled: u64,
        dirs_discovered: u64,
        files_discovered: u64,
        symlinks_discovered: u64,
        other_discovered: u64,
        batches: u64,
        batch_entries: u64,
        max_batch: u64,
        scanner_errors: u64,
        queue_high: u64,
    };

    fn extractMetrics(stats: Context.Stats, elapsed_ns: u64) Metrics {
        const dirs_completed = stats.directories_completed.load(.acquire);
        const batches = stats.scanner_batches.load(.acquire);
        const batch_entries = stats.scanner_entries.load(.acquire);

        const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));

        return .{
            .elapsed_s = elapsed_s,
            .dirs_per_sec = if (elapsed_ns == 0) 0.0 else @as(f64, @floatFromInt(dirs_completed)) / elapsed_s,
            .avg_batch_entries = if (batches == 0) 0.0 else @as(f64, @floatFromInt(batch_entries)) / @as(f64, @floatFromInt(batches)),
            .dirs_started = stats.directories_started.load(.acquire),
            .dirs_completed = dirs_completed,
            .dirs_scheduled = stats.directories_scheduled.load(.acquire),
            .dirs_discovered = stats.directories_discovered.load(.acquire),
            .files_discovered = stats.files_discovered.load(.acquire),
            .symlinks_discovered = stats.symlinks_discovered.load(.acquire),
            .other_discovered = stats.other_discovered.load(.acquire),
            .batches = batches,
            .batch_entries = batch_entries,
            .max_batch = stats.scanner_max_batch.load(.acquire),
            .scanner_errors = stats.scanner_errors.load(.acquire),
            .queue_high = stats.high_watermark.load(.acquire),
        };
    }

    fn printQueueMetrics(metrics: Metrics) !void {
        try stdout.print(
            "  Queue peak: {d} tasks\n  Scheduled dirs: {d}  discovered: {d}  started/completed: {d}/{d}\n",
            .{ metrics.queue_high, metrics.dirs_scheduled, metrics.dirs_discovered, metrics.dirs_started, metrics.dirs_completed },
        );
    }

    fn printBatchMetrics(metrics: Metrics) !void {
        try stdout.print(
            "  Batches: {d}  avg entries/batch: {d:.1}  max: {d}\n",
            .{ metrics.batches, metrics.avg_batch_entries, metrics.max_batch },
        );
    }

    fn printEntryMetrics(metrics: Metrics) !void {
        try stdout.print(
            "  Entries seen: files {d}, symlinks {d}, other {d}\n",
            .{ metrics.files_discovered, metrics.symlinks_discovered, metrics.other_discovered },
        );
    }

    fn printTopLevelDirectories(
        self: *Self,
        totals: DirectoryTotals,
        entries: []const SummaryEntry,
    ) !void {
        if (entries.len == 0) return;

        try stdout.print(
            "Top-level directories by total size:\n\n",
            .{},
        );
        try stdout.print(
            "  Size         Share   Path                               Files      Dirs   Avg file   Status\n",
            .{},
        );

        const limit = @min(entries.len, 10);
        for (entries[0..limit]) |entry| {
            try printDirectoryEntry(self, totals, entry);
        }
        try stdout.print("\n", .{});
    }

    fn printHeaviestDirectories(
        self: *Self,
        totals: DirectoryTotals,
        entries: []const SummaryEntry,
    ) !void {
        if (entries.len == 0) return;

        try stdout.print(
            "Heaviest directories in tree:\n\n",
            .{},
        );
        try stdout.print(
            "  Size         Share   Path                               Files      Dirs   Avg file   Status\n",
            .{},
        );

        const limit = @min(entries.len, 15);
        for (entries[0..limit]) |entry| {
            try printDirectoryEntry(self, totals, entry);
        }
        try stdout.print("\n", .{});
    }

    fn printDirectoryEntry(
        self: *Self,
        totals: DirectoryTotals,
        entry: SummaryEntry,
    ) !void {
        const slices = self.directories.slice();
        const inaccessible = slices.items(.inaccessible)[entry.index];
        const size = slices.items(.total_size)[entry.index];
        const files = slices.items(.total_files)[entry.index];
        const dirs_total = slices.items(.total_dirs)[entry.index];
        const dir_count = if (dirs_total == 0) 0 else dirs_total - 1;

        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], size);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], size, totals.bytes);

        var files_buf: [32]u8 = undefined;
        const files_str = try formatCount(files_buf[0..], files);

        var dirs_buf: [32]u8 = undefined;
        const dirs_str = try formatCount(dirs_buf[0..], dir_count);

        var avg_buf: [32]u8 = undefined;
        const avg_str = if (files == 0)
            "-"
        else blk: {
            const files_u64 = std.math.cast(u64, files) orelse std.math.maxInt(u64);
            const avg_bytes = size / @max(files_u64, 1);
            break :blk try formatBytes(avg_buf[0..], avg_bytes);
        };

        const status = if (inaccessible) "partial" else "";

        try stdout.print(
            "  {s:>11}  {s:>6}  {s:<36}  {s:>9}  {s:>8}  {s:>9}  {s}\n",
            .{ size_str, share_str, entry.path, files_str, dirs_str, avg_str, status },
        );
    }

    fn formatPercent(buf: []u8, value: u64, total: u64) ![]const u8 {
        if (total == 0) {
            return try std.fmt.bufPrint(buf, "0.0%", .{});
        }

        const percent = @min(
            100.0,
            (@as(f64, @floatFromInt(value)) / @as(f64, @floatFromInt(total))) * 100.0,
        );
        return try std.fmt.bufPrint(buf, "{d:>5.1}%", .{percent});
    }

    fn formatBytes(buf: []u8, bytes: u64) ![]const u8 {
        const Unit = struct {
            threshold: u64,
            suffix: []const u8,
        };

        const units = [_]Unit{
            .{ .threshold = 1024 * 1024 * 1024 * 1024, .suffix = "TiB" },
            .{ .threshold = 1024 * 1024 * 1024, .suffix = "GiB" },
            .{ .threshold = 1024 * 1024, .suffix = "MiB" },
            .{ .threshold = 1024, .suffix = "KiB" },
        };

        for (units) |unit| {
            if (bytes >= unit.threshold) {
                const value = @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(unit.threshold));
                return try std.fmt.bufPrint(buf, "{d:>7.1} {s}", .{ value, unit.suffix });
            }
        }

        return try std.fmt.bufPrint(buf, "{d} B", .{bytes});
    }

    fn formatCount(buf: []u8, value: usize) ![]const u8 {
        const cast_value = std.math.cast(u64, value) orelse std.math.maxInt(u64);
        const Unit = struct {
            threshold: u64,
            suffix: []const u8,
        };

        const units = [_]Unit{
            .{ .threshold = 1_000_000_000_000, .suffix = "T" },
            .{ .threshold = 1_000_000_000, .suffix = "B" },
            .{ .threshold = 1_000_000, .suffix = "M" },
            .{ .threshold = 1_000, .suffix = "K" },
        };

        for (units) |unit| {
            if (cast_value >= unit.threshold) {
                const scaled = @as(f64, @floatFromInt(cast_value)) / @as(f64, @floatFromInt(unit.threshold));
                const decimals: u8 = if (scaled >= 100.0) 0 else if (scaled >= 10.0) 1 else 2;
                return switch (decimals) {
                    0 => try std.fmt.bufPrint(buf, "{d:.0}{s}", .{ scaled, unit.suffix }),
                    1 => try std.fmt.bufPrint(buf, "{d:.1}{s}", .{ scaled, unit.suffix }),
                    else => try std.fmt.bufPrint(buf, "{d:.2}{s}", .{ scaled, unit.suffix }),
                };
            }
        }

        return try std.fmt.bufPrint(buf, "{d}", .{cast_value});
    }
};

// ===== Main Entry Point =====

fn performScan(self: *Self) !ScanResults {
    var timer = try std.time.Timer.start();
    try self.gatherPhase();
    const elapsed_ns = timer.read();

    StatsAggregator.aggregateUp(self);
    const totals = StatsAggregator.extractTotals(self);

    return .{
        .stats = &self.stats,
        .elapsed_ns = elapsed_ns,
        .totals = totals,
    };
}

fn generateSummary(self: *Self) !Summary {
    var summary = Summary{
        .top_level = std.ArrayList(SummaryEntry){},
        .heaviest = std.ArrayList(SummaryEntry){},
    };
    errdefer summary.deinit(self.allocator);

    summary.top_level = try SummaryBuilder.buildTopLevelEntries(self);
    SummaryBuilder.sortBySize(self, summary.top_level.items);

    summary.heaviest = try SummaryBuilder.buildHeaviestEntries(self, 12);

    return summary;
}

fn reportResults(self: *Self, results: ScanResults, summary: Summary) !void {
    try Reporter.printHeader(self, results);
    try Reporter.printStatistics(results);
    try Reporter.printTopLevelDirectories(self, results.totals, summary.top_level.items);
    try Reporter.printHeaviestDirectories(self, results.totals, summary.heaviest.items);
    try stdout.flush();
}

pub fn run(self: *Self) !void {
    // Initialize progress tracking
    var progress = std.Progress.start(.{ .root_name = "wtfs" });
    errdefer progress.end();
    self.progress_root = progress;

    // Perform the scan
    const results = try self.performScan();

    // Generate summary
    var summary = try self.generateSummary();
    defer summary.deinit(self.allocator);

    // End progress and report
    progress.end();
    try self.reportResults(results, summary);
}
