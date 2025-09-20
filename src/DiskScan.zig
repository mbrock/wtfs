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

allocator: std.mem.Allocator,
skip_hidden: bool = true,
root: []const u8 = ".",
directories: Context.DirectoryStore = .empty,
namedata: std.ArrayList(u8) = .empty,
idxset: strpool.IndexSet = .empty,
progress_root: std.Progress.Node = undefined,
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
        .stats = &self.stats,  // Pass pointer to stats
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
        self.progress_root.setName("Building paths");
        self.progress_root.setEstimatedTotalItems(self.directories.len);

        var idx: usize = 0;
        while (idx < self.directories.len) : (idx += 1) {
            defer self.progress_root.completeOne();

            if (idx == 0) continue;

            const parent = self.directories.slice().items(.parent)[idx];
            if (parent == 0) {
                var writer = try std.Io.Writer.Allocating.initCapacity(self.allocator, 256);
                try self.writeFullPath(idx, &writer.writer);

                try entries.append(self.allocator, .{
                    .index = idx,
                    .path = try writer.toOwnedSlice(),
                });
            }
        }

        return entries;
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

    fn printTopDirectories(self: *Self, entries: []const SummaryEntry) !void {
        if (entries.len == 0) return;

        try stdout.print("Top-level directories by total size:\n\n", .{});

        const max_entries = @min(entries.len, 10);
        for (entries[0..max_entries]) |entry| {
            try printDirectoryEntry(self, entry);
        }
    }

    fn printDirectoryEntry(self: *Self, entry: SummaryEntry) !void {
        const slices = self.directories.slice();
        const inaccessible = slices.items(.inaccessible)[entry.index];
        const size = slices.items(.total_size)[entry.index];
        const files = slices.items(.total_files)[entry.index];
        const marker = if (inaccessible) " (inaccessible)" else "";

        try stdout.print(
            "{d: >9.1}MiB  {s: <32}{s}  {d: >6} files\n",
            .{
                @as(f64, @floatFromInt(size)) / 1024 / 1024,
                entry.path,
                marker,
                files,
            },
        );
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

fn generateSummary(self: *Self) !std.ArrayList(SummaryEntry) {
    const entries = try SummaryBuilder.buildTopLevelEntries(self);
    SummaryBuilder.sortBySize(self, entries.items);
    return entries;
}

fn reportResults(self: *Self, results: ScanResults, entries: []const SummaryEntry) !void {
    try Reporter.printHeader(self, results);
    try Reporter.printStatistics(results);
    try Reporter.printTopDirectories(self, entries);
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
    const entries = try self.generateSummary();

    // End progress and report
    progress.end();
    try self.reportResults(results, entries.items);
}
