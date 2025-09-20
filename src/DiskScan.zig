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

const FileSummaryEntry = struct {
    size: u64,
    path: []u8,
};

const Summary = struct {
    top_level: std.ArrayList(SummaryEntry),
    heaviest: std.ArrayList(SummaryEntry),
    large_files: std.ArrayList(FileSummaryEntry),

    fn deinit(self: *Summary, allocator: std.mem.Allocator) void {
        freeEntryList(allocator, &self.top_level);
        freeEntryList(allocator, &self.heaviest);
        freeFileEntryList(allocator, &self.large_files);
    }

    fn freeEntryList(allocator: std.mem.Allocator, list: *std.ArrayList(SummaryEntry)) void {
        if (list.items.len != 0) {
            for (list.items) |entry| {
                allocator.free(entry.path);
            }
        }
        list.deinit(allocator);
    }

    fn freeFileEntryList(allocator: std.mem.Allocator, list: *std.ArrayList(FileSummaryEntry)) void {
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

/// Threshold in bytes for recording large files
large_file_threshold: u64 = default_large_file_threshold,

/// Collection of large files discovered during the scan
large_files: Context.LargeFileStore = .empty,

pub const BinaryFormatVersion: u16 = 2;

pub const RunOptions = struct {
    /// When true, generate the human readable summary and print it to stdout.
    emit_text_report: bool = true,
    /// Optional binary writer that receives the scan snapshot.
    binary_writer: ?*std.Io.Writer = null,
};

pub const default_large_file_threshold: u64 = 100 * 1024 * 1024;

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
        .large_files = &self.large_files,
        .large_file_threshold = self.large_file_threshold,
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

fn buildFilePath(self: *Self, directory_index: usize, basename_index: u32) ![]u8 {
    const directory_path = try self.buildPath(directory_index);
    defer self.allocator.free(directory_path);

    const file_name = std.mem.sliceTo(self.namedata.items[basename_index..], 0);

    var writer = try std.Io.Writer.Allocating.initCapacity(
        self.allocator,
        directory_path.len + 1 + file_name.len,
    );
    try writer.writer.writeAll(directory_path);
    if (directory_path.len == 0 or directory_path[directory_path.len - 1] != '/') {
        try writer.writer.writeByte('/');
    }
    try writer.writer.writeAll(file_name);

    return try writer.toOwnedSlice();
}

fn directoryName(self: *Self, index: usize) []const u8 {
    const slices = self.directories.slice();
    const base_index = slices.items(.basename)[index];
    return std.mem.sliceTo(self.namedata.items[base_index..], 0);
}

test "path helpers use shared directory data" {
    const allocator = std.testing.allocator;

    var disk_scan = Self{ .allocator = allocator };
    defer disk_scan.directories.deinit(allocator);
    defer disk_scan.namedata.deinit(allocator);
    defer disk_scan.idxset.deinit(allocator);

    const root_name_offset = disk_scan.namedata.items.len;
    try disk_scan.namedata.appendSlice(allocator, "root");
    try disk_scan.namedata.append(allocator, 0);

    const child_name_offset = disk_scan.namedata.items.len;
    try disk_scan.namedata.appendSlice(allocator, "child");
    try disk_scan.namedata.append(allocator, 0);

    const root_index = try disk_scan.directories.addOne(allocator);
    var slices = disk_scan.directories.slice();
    slices.items(.parent)[root_index] = 0;
    slices.items(.basename)[root_index] = @intCast(root_name_offset);

    const child_index = try disk_scan.directories.addOne(allocator);
    slices = disk_scan.directories.slice();
    slices.items(.parent)[child_index] = @intCast(root_index);
    slices.items(.basename)[child_index] = @intCast(child_name_offset);

    try std.testing.expectEqualStrings("root", disk_scan.directoryName(root_index));
    try std.testing.expectEqualStrings("child", disk_scan.directoryName(child_index));

    var root_buffer: [32]u8 = undefined;
    var root_writer = std.Io.Writer.fixed(&root_buffer);
    try disk_scan.writeFullPath(root_index, &root_writer);
    try std.testing.expectEqualStrings("root", root_writer.buffered());

    var child_buffer: [32]u8 = undefined;
    var child_writer = std.Io.Writer.fixed(&child_buffer);
    try disk_scan.writeFullPath(child_index, &child_writer);
    try std.testing.expectEqualStrings("root/child", child_writer.buffered());

    const root_path = try disk_scan.buildPath(root_index);
    defer allocator.free(root_path);
    try std.testing.expectEqualStrings("root", root_path);

    const child_path = try disk_scan.buildPath(child_index);
    defer allocator.free(child_path);
    try std.testing.expectEqualStrings("root/child", child_path);

    const expected_names = [_]u8{ 'r', 'o', 'o', 't', 0, 'c', 'h', 'i', 'l', 'd', 0 };
    try std.testing.expectEqualSlices(u8, &expected_names, disk_scan.namedata.items);
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

    fn buildLargeFileEntries(self: *Self, max_entries: usize) !std.ArrayList(FileSummaryEntry) {
        if (max_entries == 0) return std.ArrayList(FileSummaryEntry){};

        var top_indexes = std.ArrayList(usize){};
        defer top_indexes.deinit(self.allocator);

        const slices = self.large_files.slice();
        const sizes = slices.items(.size);
        const directories = slices.items(.directory_index);
        const basenames = slices.items(.basename);

        for (sizes, 0..) |size, idx| {
            if (size < self.large_file_threshold) continue;
            try SummaryBuilder.insertFileIndex(self, &top_indexes, idx, max_entries, sizes);
        }

        var entries = std.ArrayList(FileSummaryEntry){};
        errdefer Summary.freeFileEntryList(self.allocator, &entries);
        try entries.ensureTotalCapacityPrecise(self.allocator, top_indexes.items.len);

        for (top_indexes.items) |file_index| {
            const path = try buildFilePath(self, directories[file_index], basenames[file_index]);

            entries.appendAssumeCapacity(.{
                .size = sizes[file_index],
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

    fn insertFileIndex(
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
            "  Size         Share   Files      Dirs\n",
            .{},
        );

        const limit = @min(entries.len, 10);
        for (entries[0..limit]) |entry| {
            try printTopLevelEntry(self, totals, entry);
        }
    }

    fn printHeaviestDirectories(
        self: *Self,
        totals: DirectoryTotals,
        entries: []const SummaryEntry,
    ) !void {
        if (entries.len == 0) return;

        try stdout.print("Heaviest directories in tree:\n\n", .{});
        try stdout.print("  Size         Share   Files      Dirs\n", .{});

        const ChildMap = std.AutoHashMap(usize, std.ArrayList(usize));
        var child_map = ChildMap.init(self.allocator);
        defer {
            var iter = child_map.iterator();
            while (iter.next()) |entry| {
                entry.value_ptr.*.deinit(self.allocator);
            }
            child_map.deinit();
        }

        var node_set = std.AutoHashMap(usize, void).init(self.allocator);
        defer node_set.deinit();

        const slices = self.directories.slice();

        for (entries) |entry| {
            var current = entry.index;
            while (true) {
                try node_set.put(current, {});
                if (current == 0) break;

                const parent_index: usize = @intCast(slices.items(.parent)[current]);
                try node_set.put(parent_index, {});

                const gop = try child_map.getOrPut(parent_index);
                if (!gop.found_existing) {
                    gop.value_ptr.* = std.ArrayList(usize){};
                }
                const children_ptr = gop.value_ptr;
                if (std.mem.indexOfScalar(usize, children_ptr.items, current) == null) {
                    try children_ptr.append(self.allocator, current);
                }

                if (parent_index == 0) {
                    current = 0;
                    break;
                }
                current = parent_index;
            }
        }

        try node_set.put(0, {});

        var sort_iter = child_map.iterator();
        while (sort_iter.next()) |entry| {
            const children_ptr = entry.value_ptr;
            const SortCtx = struct {
                directories: *Context.DirectoryStore,

                fn bySize(ctx: @This(), lhs: usize, rhs: usize) bool {
                    const dir_slices = ctx.directories.slice();
                    const sizes = dir_slices.items(.total_size);
                    const lhs_size = sizes[lhs];
                    const rhs_size = sizes[rhs];
                    if (lhs_size != rhs_size) return lhs_size > rhs_size;
                    return lhs < rhs;
                }
            };

            if (children_ptr.items.len > 1) {
                std.sort.heap(
                    usize,
                    children_ptr.items,
                    SortCtx{ .directories = &self.directories },
                    SortCtx.bySize,
                );
            }
        }

        try printHeaviestNode(self, &child_map, &node_set, totals, 0, 0, true);
        try stdout.print("\n", .{});
    }

    fn printHeaviestNode(
        self: *Self,
        child_map: *std.AutoHashMap(usize, std.ArrayList(usize)),
        node_set: *std.AutoHashMap(usize, void),
        totals: DirectoryTotals,
        index: usize,
        depth: usize,
        include_self: bool,
    ) !void {
        if (!node_set.contains(index)) return;

        if (include_self) {
            try printHeaviestLine(self, totals, index, depth);
        }

        if (child_map.getPtr(index)) |children| {
            for (children.items) |child_index| {
                try printHeaviestNode(self, child_map, node_set, totals, child_index, depth + 1, true);
            }
        }
    }

    fn printHeaviestLine(self: *Self, totals: DirectoryTotals, index: usize, depth: usize) !void {
        const slices = self.directories.slice();
        const inaccessible = slices.items(.inaccessible)[index];
        const size = slices.items(.total_size)[index];
        const files = slices.items(.total_files)[index];
        const dirs_total = slices.items(.total_dirs)[index];
        const dir_count = if (dirs_total == 0) 0 else dirs_total - 1;

        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], size);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], size, totals.bytes);

        var files_buf: [32]u8 = undefined;
        const files_str = try formatCount(files_buf[0..], files);

        var dirs_buf: [32]u8 = undefined;
        const dirs_str = try formatCount(dirs_buf[0..], dir_count);

        var indent_buf: [64]u8 = undefined;
        const indent_len = @min(depth * 2, indent_buf.len);
        for (indent_buf[0..indent_len]) |*ch| ch.* = ' ';
        const indent = indent_buf[0..indent_len];

        const name = if (index == 0) "." else directoryName(self, index);
        const status_suffix = if (inaccessible) "  (partial)" else "";

        try stdout.print(
            "  {s:>11}  {s:>6}  files {s:>9}  dirs {s:>8}\n",
            .{ size_str, share_str, files_str, dirs_str },
        );
        try stdout.print("      {s}{s}{s}\n\n", .{ indent, name, status_suffix });
    }

    fn printTopLevelEntry(
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
        const status_suffix = if (inaccessible) "  (partial)" else "";

        try stdout.print(
            "  {s:>11}  {s:>6}  files {s:>9}  dirs {s:>8}\n",
            .{ size_str, share_str, files_str, dirs_str },
        );
        try stdout.print("      {s}{s}\n\n", .{ entry.path, status_suffix });
    }

    fn printLargeFiles(
        self: *Self,
        totals: DirectoryTotals,
        entries: []const FileSummaryEntry,
        threshold: u64,
    ) !void {
        _ = self;
        if (entries.len == 0) return;

        var threshold_buf: [32]u8 = undefined;
        const threshold_str = try formatBytes(threshold_buf[0..], threshold);

        try stdout.print("Largest files (>= {s})\n\n", .{threshold_str});

        for (entries) |entry| {
            var size_buf: [32]u8 = undefined;
            const size_str = try formatBytes(size_buf[0..], entry.size);

            var share_buf: [16]u8 = undefined;
            const share_str = try formatPercent(share_buf[0..], entry.size, totals.bytes);

            try stdout.print("  {s:>11}  {s:>6}\n", .{ size_str, share_str });
            try stdout.print("      {s}\n\n", .{entry.path});
        }
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
    self.large_files.clearRetainingCapacity();

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
        .large_files = std.ArrayList(FileSummaryEntry){},
    };
    errdefer summary.deinit(self.allocator);

    summary.top_level = try SummaryBuilder.buildTopLevelEntries(self);
    SummaryBuilder.sortBySize(self, summary.top_level.items);

    summary.heaviest = try SummaryBuilder.buildHeaviestEntries(self, 12);
    summary.large_files = try SummaryBuilder.buildLargeFileEntries(self, 12);

    return summary;
}

fn reportResults(self: *Self, results: ScanResults, summary: Summary) !void {
    try Reporter.printHeader(self, results);
    try Reporter.printStatistics(results);
    try Reporter.printTopLevelDirectories(self, results.totals, summary.top_level.items);
    try Reporter.printHeaviestDirectories(self, results.totals, summary.heaviest.items);
    try Reporter.printLargeFiles(self, results.totals, summary.large_files.items, self.large_file_threshold);
    try stdout.flush();
}

fn runWithOptions(self: *Self, options: RunOptions) !void {
    // Initialize progress tracking
    var progress = std.Progress.start(.{ .root_name = "wtfs" });
    errdefer progress.end();
    self.progress_root = progress;

    // Perform the scan
    const results = try self.performScan();

    var summary_storage: Summary = undefined;
    var have_summary = false;
    if (options.emit_text_report) {
        summary_storage = try self.generateSummary();
        have_summary = true;
    }

    // End progress and report
    progress.end();

    if (options.binary_writer) |writer| {
        try self.writeBinaryResults(results, writer);
    }

    if (have_summary) {
        defer summary_storage.deinit(self.allocator);
        try self.reportResults(results, summary_storage);
    }
}

pub fn run(self: *Self) !void {
    try self.runWithOptions(.{});
}

pub fn runWithBinaryOutput(self: *Self, writer: *std.Io.Writer, emit_text_report: bool) !void {
    try self.runWithOptions(.{ .binary_writer = writer, .emit_text_report = emit_text_report });
}

fn writeIntLittle(writer: *std.Io.Writer, comptime T: type, value: anytype) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntLittle(T, &buf, @as(T, @intCast(value)));
    try writer.writeSliceSwap(u8, &buf);
}

fn chunkTag(comptime value: []const u8) [4]u8 {
    if (value.len != 4) @compileError("chunk tags must be exactly 4 bytes");
    return .{ value[0], value[1], value[2], value[3] };
}

fn totalSliceLength(slices: []const []const u8) usize {
    var total: usize = 0;
    for (slices) |slice| total += slice.len;
    return total;
}

fn writeChunk(
    writer: *std.Io.Writer,
    sequence: *u32,
    tag: [4]u8,
    payload: []const []const u8,
) !void {
    var seq_buf: [4]u8 = undefined;
    std.mem.writeIntLittle(u32, &seq_buf, sequence.*);
    sequence.* += 1;

    var len_buf: [8]u8 = undefined;
    std.mem.writeIntLittle(u64, &len_buf, totalSliceLength(payload));

    const reserved = [4]u8{ 0, 0, 0, 0 };
    var header = [_][]const u8{
        tag[0..],
        seq_buf[0..],
        reserved[0..],
        len_buf[0..],
    };

    try writer.writeVecAll(header[0..]);
    if (payload.len != 0) {
        try writer.writeVecAll(payload);
    }
}

fn writeInfoChunk(
    self: *Self,
    results: ScanResults,
    writer: *std.Io.Writer,
    sequence: *u32,
) !void {
    _ = self;
    const snapshot = snapshotStats(results.stats);
    var values = [_]u64{
        results.elapsed_ns,
        results.totals.directories,
        results.totals.files,
        results.totals.bytes,
        snapshot.directories_started,
        snapshot.directories_completed,
        snapshot.directories_scheduled,
        snapshot.directories_discovered,
        snapshot.files_discovered,
        snapshot.symlinks_discovered,
        snapshot.other_discovered,
        snapshot.scanner_batches,
        snapshot.scanner_entries,
        snapshot.scanner_max_batch,
        snapshot.scanner_errors,
        snapshot.inaccessible_dirs,
        snapshot.high_watermark,
    };

    inline for (&values) |*value| {
        value.* = std.mem.nativeToLittle(u64, value.*);
    }

    var count_buf: [2]u8 = undefined;
    std.mem.writeIntLittle(u16, &count_buf, @intCast(values.len));

    const payload = [_][]const u8{
        count_buf[0..],
        std.mem.sliceAsBytes(values[0..]),
    };

    try writeChunk(writer, sequence, chunkTag("INFO"), &payload);
}

fn writeNamesChunk(
    self: *Self,
    writer: *std.Io.Writer,
    sequence: *u32,
) !void {
    const offset: u64 = 0;
    const length: u64 = @intCast(self.namedata.items.len);

    var offset_buf: [8]u8 = undefined;
    var length_buf: [8]u8 = undefined;
    std.mem.writeIntLittle(u64, &offset_buf, offset);
    std.mem.writeIntLittle(u64, &length_buf, length);

    const payload = [_][]const u8{
        offset_buf[0..],
        length_buf[0..],
        self.namedata.items,
    };

    try writeChunk(writer, sequence, chunkTag("NAME"), &payload);
}

fn writeDirectoryChunks(
    self: *Self,
    writer: *std.Io.Writer,
    sequence: *u32,
) !void {
    const total = self.directories.len;
    if (total == 0) {
        var offset_buf: [8]u8 = undefined;
        var count_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(u64, &offset_buf, 0);
        std.mem.writeIntLittle(u64, &count_buf, 0);
        const payload = [_][]const u8{ offset_buf[0..], count_buf[0..] };
        try writeChunk(writer, sequence, chunkTag("DIRS"), &payload);
        return;
    }

    const slices = self.directories.slice();
    const parents = slices.items(.parent);
    const basenames = slices.items(.basename);
    const total_sizes = slices.items(.total_size);
    const total_files = slices.items(.total_files);
    const total_dirs = slices.items(.total_dirs);
    const inaccessible = slices.items(.inaccessible);

    const chunk_capacity = 1024;
    var parent_chunk: [chunk_capacity]u32 = undefined;
    var basename_chunk: [chunk_capacity]u32 = undefined;
    var size_chunk: [chunk_capacity]u64 = undefined;
    var files_chunk: [chunk_capacity]u64 = undefined;
    var dirs_chunk: [chunk_capacity]u64 = undefined;
    var inaccessible_chunk: [chunk_capacity]u8 = undefined;

    var offset: usize = 0;
    while (offset < total) {
        const remaining = total - offset;
        const count = @min(chunk_capacity, remaining);

        var i: usize = 0;
        while (i < count) : (i += 1) {
            const idx = offset + i;
            parent_chunk[i] = std.mem.nativeToLittle(u32, parents[idx]);
            basename_chunk[i] = std.mem.nativeToLittle(u32, basenames[idx]);
            size_chunk[i] = std.mem.nativeToLittle(u64, total_sizes[idx]);
            files_chunk[i] = std.mem.nativeToLittle(u64, @as(u64, @intCast(total_files[idx])));
            dirs_chunk[i] = std.mem.nativeToLittle(u64, @as(u64, @intCast(total_dirs[idx])));
            inaccessible_chunk[i] = if (inaccessible[idx]) 1 else 0;
        }

        var offset_buf: [8]u8 = undefined;
        var count_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(u64, &offset_buf, offset);
        std.mem.writeIntLittle(u64, &count_buf, count);

        const payload = [_][]const u8{
            offset_buf[0..],
            count_buf[0..],
            std.mem.sliceAsBytes(parent_chunk[0..count]),
            std.mem.sliceAsBytes(basename_chunk[0..count]),
            std.mem.sliceAsBytes(size_chunk[0..count]),
            std.mem.sliceAsBytes(files_chunk[0..count]),
            std.mem.sliceAsBytes(dirs_chunk[0..count]),
            std.mem.sliceAsBytes(inaccessible_chunk[0..count]),
        };

        try writeChunk(writer, sequence, chunkTag("DIRS"), &payload);
        offset += count;
    }
}

fn writeLargeFileChunks(
    self: *Self,
    writer: *std.Io.Writer,
    sequence: *u32,
) !void {
    const total = self.large_files.len;
    if (total == 0) {
        var offset_buf: [8]u8 = undefined;
        var count_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(u64, &offset_buf, 0);
        std.mem.writeIntLittle(u64, &count_buf, 0);
        const payload = [_][]const u8{ offset_buf[0..], count_buf[0..] };
        try writeChunk(writer, sequence, chunkTag("LARG"), &payload);
        return;
    }

    const slices = self.large_files.slice();
    const directories = slices.items(.directory_index);
    const basenames = slices.items(.basename);
    const sizes = slices.items(.size);

    const chunk_capacity = 1024;
    var dir_chunk: [chunk_capacity]u64 = undefined;
    var basename_chunk: [chunk_capacity]u32 = undefined;
    var size_chunk: [chunk_capacity]u64 = undefined;

    var offset: usize = 0;
    while (offset < total) {
        const remaining = total - offset;
        const count = @min(chunk_capacity, remaining);

        var i: usize = 0;
        while (i < count) : (i += 1) {
            const idx = offset + i;
            dir_chunk[i] = std.mem.nativeToLittle(u64, @as(u64, @intCast(directories[idx])));
            basename_chunk[i] = std.mem.nativeToLittle(u32, basenames[idx]);
            size_chunk[i] = std.mem.nativeToLittle(u64, sizes[idx]);
        }

        var offset_buf: [8]u8 = undefined;
        var count_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(u64, &offset_buf, offset);
        std.mem.writeIntLittle(u64, &count_buf, count);

        const payload = [_][]const u8{
            offset_buf[0..],
            count_buf[0..],
            std.mem.sliceAsBytes(dir_chunk[0..count]),
            std.mem.sliceAsBytes(basename_chunk[0..count]),
            std.mem.sliceAsBytes(size_chunk[0..count]),
        };

        try writeChunk(writer, sequence, chunkTag("LARG"), &payload);
        offset += count;
    }
}

fn snapshotStats(stats: *const Context.Stats) StatsSnapshot {
    return .{
        .directories_started = stats.directories_started.load(.acquire),
        .directories_completed = stats.directories_completed.load(.acquire),
        .directories_scheduled = stats.directories_scheduled.load(.acquire),
        .directories_discovered = stats.directories_discovered.load(.acquire),
        .files_discovered = stats.files_discovered.load(.acquire),
        .symlinks_discovered = stats.symlinks_discovered.load(.acquire),
        .other_discovered = stats.other_discovered.load(.acquire),
        .scanner_batches = stats.scanner_batches.load(.acquire),
        .scanner_entries = stats.scanner_entries.load(.acquire),
        .scanner_max_batch = stats.scanner_max_batch.load(.acquire),
        .scanner_errors = stats.scanner_errors.load(.acquire),
        .inaccessible_dirs = stats.inaccessible_dirs.load(.acquire),
        .high_watermark = stats.high_watermark.load(.acquire),
    };
}

const StatsSnapshot = struct {
    directories_started: usize,
    directories_completed: usize,
    directories_scheduled: usize,
    directories_discovered: usize,
    files_discovered: usize,
    symlinks_discovered: usize,
    other_discovered: usize,
    scanner_batches: usize,
    scanner_entries: usize,
    scanner_max_batch: usize,
    scanner_errors: usize,
    inaccessible_dirs: usize,
    high_watermark: usize,
};

pub fn writeBinaryResults(self: *Self, results: ScanResults, writer: *std.Io.Writer) !void {
    const magic = "WTFS";
    try writer.writeAll(magic);
    try writeIntLittle(writer, u16, BinaryFormatVersion);
    try writeIntLittle(writer, u16, 0);
    var sequence: u32 = 0;
    try self.writeInfoChunk(results, writer, &sequence);
    try self.writeNamesChunk(writer, &sequence);
    try self.writeDirectoryChunks(writer, &sequence);
    try self.writeLargeFileChunks(writer, &sequence);
}

test "binary snapshot writer emits deterministic format" {
    const allocator = std.testing.allocator;

    var disk_scan = Self{ .allocator = allocator };
    defer disk_scan.directories.deinit(allocator);
    defer disk_scan.namedata.deinit(allocator);
    defer disk_scan.idxset.deinit(allocator);
    defer disk_scan.large_files.deinit(allocator);

    const root_offset = disk_scan.namedata.items.len;
    try disk_scan.namedata.appendSlice(allocator, "root");
    try disk_scan.namedata.append(allocator, 0);

    const child_offset = disk_scan.namedata.items.len;
    try disk_scan.namedata.appendSlice(allocator, "child");
    try disk_scan.namedata.append(allocator, 0);

    const file_offset = disk_scan.namedata.items.len;
    try disk_scan.namedata.appendSlice(allocator, "large.bin");
    try disk_scan.namedata.append(allocator, 0);

    const root_index = try disk_scan.directories.addOne(allocator);
    var slices = disk_scan.directories.slice();
    slices.items(.parent)[root_index] = 0;
    slices.items(.basename)[root_index] = @intCast(root_offset);
    slices.items(.total_size)[root_index] = 1024;
    slices.items(.total_files)[root_index] = 3;
    slices.items(.total_dirs)[root_index] = 2;
    slices.items(.inaccessible)[root_index] = false;

    const child_index = try disk_scan.directories.addOne(allocator);
    slices = disk_scan.directories.slice();
    slices.items(.parent)[child_index] = @intCast(root_index);
    slices.items(.basename)[child_index] = @intCast(child_offset);
    slices.items(.total_size)[child_index] = 256;
    slices.items(.total_files)[child_index] = 2;
    slices.items(.total_dirs)[child_index] = 1;
    slices.items(.inaccessible)[child_index] = true;

    try disk_scan.large_files.append(allocator, .{
        .directory_index = child_index,
        .basename = @intCast(file_offset),
        .size = 4096,
    });

    disk_scan.stats.directories_started.store(11, .release);
    disk_scan.stats.directories_completed.store(10, .release);
    disk_scan.stats.directories_scheduled.store(9, .release);
    disk_scan.stats.directories_discovered.store(8, .release);
    disk_scan.stats.files_discovered.store(7, .release);
    disk_scan.stats.symlinks_discovered.store(6, .release);
    disk_scan.stats.other_discovered.store(5, .release);
    disk_scan.stats.scanner_batches.store(4, .release);
    disk_scan.stats.scanner_entries.store(3, .release);
    disk_scan.stats.scanner_max_batch.store(2, .release);
    disk_scan.stats.scanner_errors.store(1, .release);
    disk_scan.stats.inaccessible_dirs.store(12, .release);
    disk_scan.stats.high_watermark.store(13, .release);

    const results = ScanResults{
        .stats = &disk_scan.stats,
        .elapsed_ns = 123,
        .totals = .{
            .directories = 2,
            .files = 3,
            .bytes = 1024,
        },
    };

    var buffer: [1024]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try disk_scan.writeBinaryResults(results, &writer);
    const emitted = writer.buffered();

    var offset: usize = 0;
    const Reader = struct {
        fn readInt(comptime T: type, data: []const u8, cursor: *usize) T {
            const start = cursor.*;
            const end = start + @sizeOf(T);
            cursor.* = end;
            return std.mem.readIntLittle(T, data[start..end]);
        }

        fn readBytes(data: []const u8, cursor: *usize, len: usize) []const u8 {
            const start = cursor.*;
            const end = start + len;
            cursor.* = end;
            return data[start..end];
        }
    };

    const Chunk = struct {
        tag: [4]u8,
        seq: u32,
        reserved: u32,
        payload: []const u8,
    };

    const ChunkReader = struct {
        fn readChunk(data: []const u8, cursor: *usize) Chunk {
            var tag: [4]u8 = undefined;
            var i: usize = 0;
            while (i < 4) : (i += 1) {
                tag[i] = data[cursor.* + i];
            }
            cursor.* += 4;
            const seq = Reader.readInt(u32, data, cursor);
            const reserved = Reader.readInt(u32, data, cursor);
            const length = Reader.readInt(u64, data, cursor);
            const payload = Reader.readBytes(data, cursor, @intCast(length));
            return .{ .tag = tag, .seq = seq, .reserved = reserved, .payload = payload };
        }
    };

    try std.testing.expectEqualSlices(u8, "WTFS", emitted[offset..][0..4]);
    offset += 4;
    try std.testing.expectEqual(BinaryFormatVersion, Reader.readInt(u16, emitted, &offset));
    _ = Reader.readInt(u16, emitted, &offset);

    const info_chunk = ChunkReader.readChunk(emitted, &offset);
    try std.testing.expectEqualSlices(u8, "INFO", info_chunk.tag[0..]);
    try std.testing.expectEqual(@as(u32, 0), info_chunk.seq);
    try std.testing.expectEqual(@as(u32, 0), info_chunk.reserved);
    var info_cursor: usize = 0;
    const info_count = Reader.readInt(u16, info_chunk.payload, &info_cursor);
    const expected_info = [_]u64{
        123,
        2,
        3,
        1024,
        11,
        10,
        9,
        8,
        7,
        6,
        5,
        4,
        3,
        2,
        1,
        12,
        13,
    };
    try std.testing.expectEqual(@as(u16, expected_info.len), info_count);
    for (expected_info) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u64, info_chunk.payload, &info_cursor));
    }
    try std.testing.expectEqual(info_cursor, info_chunk.payload.len);

    const name_chunk = ChunkReader.readChunk(emitted, &offset);
    try std.testing.expectEqualSlices(u8, "NAME", name_chunk.tag[0..]);
    try std.testing.expectEqual(@as(u32, 1), name_chunk.seq);
    try std.testing.expectEqual(@as(u32, 0), name_chunk.reserved);
    var name_cursor: usize = 0;
    try std.testing.expectEqual(@as(u64, 0), Reader.readInt(u64, name_chunk.payload, &name_cursor));
    const name_length = Reader.readInt(u64, name_chunk.payload, &name_cursor);
    try std.testing.expectEqual(@as(u64, disk_scan.namedata.items.len), name_length);
    const name_bytes = Reader.readBytes(name_chunk.payload, &name_cursor, @intCast(name_length));
    try std.testing.expectEqualSlices(u8, disk_scan.namedata.items, name_bytes);
    try std.testing.expectEqual(name_cursor, name_chunk.payload.len);

    const dirs_chunk = ChunkReader.readChunk(emitted, &offset);
    try std.testing.expectEqualSlices(u8, "DIRS", dirs_chunk.tag[0..]);
    try std.testing.expectEqual(@as(u32, 2), dirs_chunk.seq);
    try std.testing.expectEqual(@as(u32, 0), dirs_chunk.reserved);
    var dir_cursor: usize = 0;
    try std.testing.expectEqual(@as(u64, 0), Reader.readInt(u64, dirs_chunk.payload, &dir_cursor));
    const dir_count = Reader.readInt(u64, dirs_chunk.payload, &dir_cursor);
    try std.testing.expectEqual(@as(u64, 2), dir_count);

    const expected_parents = [_]u32{ 0, @intCast(root_index) };
    const expected_basenames = [_]u32{ @intCast(root_offset), @intCast(child_offset) };
    const expected_sizes = [_]u64{ 1024, 256 };
    const expected_files = [_]u64{ 3, 2 };
    const expected_dirs = [_]u64{ 2, 1 };
    const expected_inaccessible = [_]u8{ 0, 1 };

    for (expected_parents) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u32, dirs_chunk.payload, &dir_cursor));
    }
    for (expected_basenames) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u32, dirs_chunk.payload, &dir_cursor));
    }
    for (expected_sizes) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u64, dirs_chunk.payload, &dir_cursor));
    }
    for (expected_files) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u64, dirs_chunk.payload, &dir_cursor));
    }
    for (expected_dirs) |value| {
        try std.testing.expectEqual(value, Reader.readInt(u64, dirs_chunk.payload, &dir_cursor));
    }
    const inaccessible_bytes = Reader.readBytes(dirs_chunk.payload, &dir_cursor, expected_inaccessible.len);
    try std.testing.expectEqualSlices(u8, &expected_inaccessible, inaccessible_bytes);
    try std.testing.expectEqual(dir_cursor, dirs_chunk.payload.len);

    const large_chunk = ChunkReader.readChunk(emitted, &offset);
    try std.testing.expectEqualSlices(u8, "LARG", large_chunk.tag[0..]);
    try std.testing.expectEqual(@as(u32, 3), large_chunk.seq);
    try std.testing.expectEqual(@as(u32, 0), large_chunk.reserved);
    var large_cursor: usize = 0;
    try std.testing.expectEqual(@as(u64, 0), Reader.readInt(u64, large_chunk.payload, &large_cursor));
    const large_count = Reader.readInt(u64, large_chunk.payload, &large_cursor);
    try std.testing.expectEqual(@as(u64, 1), large_count);
    try std.testing.expectEqual(@as(u64, child_index), Reader.readInt(u64, large_chunk.payload, &large_cursor));
    try std.testing.expectEqual(@as(u32, file_offset), Reader.readInt(u32, large_chunk.payload, &large_cursor));
    try std.testing.expectEqual(@as(u64, 4096), Reader.readInt(u64, large_chunk.payload, &large_cursor));
    try std.testing.expectEqual(large_cursor, large_chunk.payload.len);

    try std.testing.expectEqual(offset, emitted.len);
}
