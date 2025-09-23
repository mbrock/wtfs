const std = @import("std");
const builtin = @import("builtin");
const dirscan = @import("DirScanner.zig");
const strpool = @import("pool.zig");

const TaskQueue = @import("TaskQueue.zig");
const Context = @import("Context.zig");
const Worker = @import("Worker.zig");
const Reporter = @import("Report.zig");

comptime {
    _ = std.testing.refAllDecls(@This());
}

const Self = @This();

/// Memory allocator for all dynamic allocations during the scan
allocator: std.mem.Allocator,

/// Whether to skip hidden files and directories (starting with '.')
skip_hidden: bool = true,

/// Root directory path to scan
root: []const u8 = ".",

/// Segmented storage for all discovered directory nodes and their metadata
directories: Context.DirectoryTable = .{},

/// Pool of null-terminated directory names referenced by basename indices
namedata: std.ArrayList(u8) = .empty,

/// Set tracking unique name indices to avoid duplicates in the name pool
idxset: strpool.IndexSet = .empty,

/// Root progress node for tracking scan progress and reporting to the user
progress_root: std.Progress.Node = undefined,

/// Threshold in bytes for recording large files
large_file_threshold: u64 = default_large_file_threshold,

/// Collection of large files discovered during the scan
large_files: std.MultiArrayList(Context.LargeFile) = .empty,

pub const SummaryEntry = struct {
    index: usize,
    path: []u8,
};

pub const FileSummaryEntry = struct {
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

pub const DirectoryTotals = packed struct {
    directories: u64,
    files: u64,
    bytes: u64,
};

pub const ScanResults = struct {
    elapsed_ns: u64,
    totals: DirectoryTotals,
};

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

pub const RunOptions = struct {
    /// When true, generate the human readable summary and print it to stdout.
    emit_text_report: bool = true,
    /// Optional binary writer that receives the scan snapshot.
    binary_writer: ?*std.Io.Writer = null,
};

pub const default_large_file_threshold: u64 = 100 * 1024 * 1024;

const PlatformConfig = struct {
    fn preventICloudDownload() void {
        if (builtin.target.os.tag == .macos) {
            // Forbid on-demand file content loading; don't pull in data from iCloud
            // macOS-only: keep Spotlight/iCloud from paging in file contents.
            // Linux fast paths live in SysDispatcher/DirScanner instead.
            switch (dirscan.setiopolicy_np(.vfs_materialize_dataless_files, .process, 1)) {
                0 => {},
                else => |rc| {
                    std.debug.panic("setiopolicy_np: {t}", .{std.posix.errno(rc)});
                },
            }
        }
    }
};

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
        .large_files = &self.large_files,
        .large_file_threshold = self.large_file_threshold,
    };
}

fn initializeRootDirectory(self: *Self, ctx: *Context) !void {
    self.progress_root.setEstimatedTotalItems(1);

    var rootdir = try std.fs.cwd().openDir(self.root, .{ .iterate = true });
    errdefer rootdir.close();

    const rootidx = try ctx.addRoot(".", rootdir);

    _ = ctx.outstanding.fetchAdd(1, .acq_rel);
    errdefer _ = ctx.outstanding.fetchSub(1, .acq_rel);

    try ctx.task_queue.push(self.allocator, rootidx);
}

fn gatherPhase(self: *Self) !void {
    PlatformConfig.preventICloudDownload();

    var queue_progress = self.progress_root.start("Work queue", 0);
    errdefer queue_progress.end();

    var wait_group = std.Thread.WaitGroup{};

    var worker_pool: std.Thread.Pool = undefined;
    try worker_pool.init(.{
        .allocator = self.allocator,
        .stack_size = 32 * 1024 * 1024,
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
}

fn writeFullPath(
    self: *Self,
    index: usize,
    writer: *std.Io.Writer,
) !void {
    const basename_off: usize = @intCast(self.directories.ptr(.basename, index).*);
    const parent_index = self.directories.ptr(.parent, index).*;

    if (index != 0) {
        try self.writeFullPath(@intCast(parent_index), writer);
        try writer.writeByte('/');
    }
    try writer.writeAll(std.mem.sliceTo(self.namedata.items[basename_off..], 0));
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

pub fn directoryName(
    directories: *Context.DirectoryTable,
    namedata: *const std.ArrayList(u8),
    index: usize,
) []const u8 {
    const base_index: usize = @intCast(directories.ptr(.basename, index).*);
    return std.mem.sliceTo(namedata.items[base_index..], 0);
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
    disk_scan.directories.ptr(.parent, root_index).* = 0;
    disk_scan.directories.ptr(.basename, root_index).* = @intCast(root_name_offset);

    const child_index = try disk_scan.directories.addOne(allocator);
    disk_scan.directories.ptr(.parent, child_index).* = @intCast(root_index);
    disk_scan.directories.ptr(.basename, child_index).* = @intCast(child_name_offset);

    try std.testing.expectEqualStrings("root", directoryName(&disk_scan.directories, &disk_scan.namedata, root_index));
    try std.testing.expectEqualStrings("child", directoryName(&disk_scan.directories, &disk_scan.namedata, child_index));

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

test "threaded disk scan can repeatedly scan populated directory trees" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (builtin.target.os.tag == .macos) return error.SkipZigTest;
    if (builtin.target.os.tag == .windows) return error.SkipZigTest;

    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const top_dir_count: usize = 5;
    const sub_dir_count: usize = 5;
    const files_per_subdir: usize = 5;

    for (0..top_dir_count) |top_index| {
        var dir_buf: [32]u8 = undefined;
        const dir_name = try std.fmt.bufPrint(&dir_buf, "dir{d}", .{top_index});
        try tmp.dir.makePath(dir_name);

        for (0..sub_dir_count) |sub_index| {
            var sub_buf: [64]u8 = undefined;
            const sub_name = try std.fmt.bufPrint(&sub_buf, "{s}/sub{d}", .{ dir_name, sub_index });
            try tmp.dir.makePath(sub_name);

            for (0..files_per_subdir) |file_index| {
                var file_buf: [96]u8 = undefined;
                const file_path = try std.fmt.bufPrint(&file_buf, "{s}/file{d}.txt", .{ sub_name, file_index });

                var contents_buf: [32]u8 = undefined;
                const contents = try std.fmt.bufPrint(&contents_buf, "data-{d}-{d}-{d}", .{ top_index, sub_index, file_index });

                try tmp.dir.writeFile(.{ .sub_path = file_path, .data = contents });
            }
        }
    }

    const root_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(root_path);

    const expected_files = top_dir_count * sub_dir_count * files_per_subdir;
    const expected_directories = 1 + top_dir_count + (top_dir_count * sub_dir_count);

    for (0..20) |_| {
        var disk_scan = Self{ .allocator = allocator, .root = root_path };
        defer {
            disk_scan.directories.deinit(allocator);
            disk_scan.namedata.deinit(allocator);
            disk_scan.idxset.deinit(allocator);
            disk_scan.large_files.deinit(allocator);
        }

        disk_scan.progress_root = std.Progress.Node.none;

        const results = try disk_scan.performScan();

        try std.testing.expectEqual(expected_files, disk_scan.directories.ptr(.total_files, 0).*);
        try std.testing.expectEqual(expected_directories, disk_scan.directories.ptr(.total_dirs, 0).*);
        try std.testing.expectEqual(@as(u64, expected_directories), results.totals.directories);
        try std.testing.expectEqual(@as(u64, expected_files), results.totals.files);
    }
}

// ===== Statistics Processing =====

const StatsAggregator = struct {
    fn aggregateUp(self: *Self) void {
        const len_snapshot = self.directories.len.load(.acquire);
        var idx = len_snapshot;
        while (idx > 0) {
            idx -= 1;
            if (idx != 0) {
                const parent_index: usize = @intCast(self.directories.ptr(.parent, idx).*);
                self.directories.ptr(.total_size, parent_index).* += self.directories.ptr(.total_size, idx).*;
                self.directories.ptr(.total_files, parent_index).* += self.directories.ptr(.total_files, idx).*;
                self.directories.ptr(.total_dirs, parent_index).* += self.directories.ptr(.total_dirs, idx).*;
            }
            self.progress_root.completeOne();
        }
    }

    fn extractTotals(self: *Self) DirectoryTotals {
        return .{
            .directories = @intCast(self.directories.ptr(.total_dirs, 0).*),
            .files = @intCast(self.directories.ptr(.total_files, 0).*),
            .bytes = self.directories.ptr(.total_size, 0).*,
        };
    }
};

// ===== Summary Generation =====

const SummaryBuilder = struct {
    fn buildTopLevelEntries(self: *Self) !std.ArrayList(SummaryEntry) {
        var entries = std.ArrayList(SummaryEntry){};
        errdefer Summary.freeEntryList(self.allocator, &entries);
        self.progress_root.setName("Building paths");
        const len_snapshot = self.directories.len.load(.acquire);
        self.progress_root.setEstimatedTotalItems(len_snapshot);

        var idx: usize = 0;
        while (idx < len_snapshot) : (idx += 1) {
            defer self.progress_root.completeOne();

            if (idx == 0) continue;

            const parent = self.directories.ptr(.parent, idx).*;
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

        const len_snapshot = self.directories.len.load(.acquire);

        var idx: usize = 1; // Skip root
        while (idx < len_snapshot) : (idx += 1) {
            const size = self.directories.ptr(.total_size, idx).*;
            if (size == 0) continue;
            try SummaryBuilder.insertTopIndex(self, &top_indexes, idx, max_entries, size);
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
        size: u64,
    ) !void {
        if (size == 0) return;

        if (list.items.len < max_entries) {
            try list.append(self.allocator, index);
            var pos = list.items.len - 1;
            while (pos > 0 and size > self.directories.ptr(.total_size, list.items[pos - 1]).*) {
                list.items[pos] = list.items[pos - 1];
                pos -= 1;
            }
            list.items[pos] = index;
            return;
        }

        const smallest_index = list.items[max_entries - 1];
        if (size <= self.directories.ptr(.total_size, smallest_index).*) return;

        list.items[max_entries - 1] = index;
        var pos: usize = max_entries - 1;
        while (pos > 0 and size > self.directories.ptr(.total_size, list.items[pos - 1]).*) {
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
        directories: *Context.DirectoryTable,

        fn bySize(self: @This(), lhs: SummaryEntry, rhs: SummaryEntry) bool {
            const lhs_size = self.directories.ptr(.total_size, lhs.index).*;
            const rhs_size = self.directories.ptr(.total_size, rhs.index).*;

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

// ===== Main Entry Point =====

fn performScan(self: *Self) !ScanResults {
    self.large_files.clearRetainingCapacity();

    var timer = try std.time.Timer.start();
    try self.gatherPhase();
    const elapsed_ns = timer.read();

    StatsAggregator.aggregateUp(self);
    const totals = StatsAggregator.extractTotals(self);

    return .{
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
    const report_data = Reporter.ReportData.init(
        &self.directories,
        &self.namedata,
        &self.idxset,
        &self.large_files,
        results.totals,
    );

    var top_level = try Reporter.buildTopLevelSummary(
        self.allocator,
        report_data,
        summary.top_level.items,
        10,
    );
    defer top_level.deinit(self.allocator);

    var heaviest = try Reporter.buildHeaviestSummary(
        self.allocator,
        report_data,
        summary.heaviest.items,
    );
    defer heaviest.deinit(self.allocator);

    try Reporter.printHeader(stdout, self.root, results);
    try Reporter.printTopLevelDirectories(stdout, report_data, top_level);
    try Reporter.printHeaviestDirectories(stdout, report_data, heaviest);
    try Reporter.printLargeFiles(stdout, report_data, summary.large_files.items, self.large_file_threshold);
    try stdout.flush();
}

fn runWithOptions(self: *Self, options: RunOptions) !void {
    // Initialize progress tracking
    var progress = std.Progress.start(.{
        .root_name = "wtfs",
        .refresh_rate_ns = 80 * std.time.ns_per_ms,
    });
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

pub fn writeBinaryResults(
    self: *Self,
    results: ScanResults,
    writer: *std.Io.Writer,
) !void {
    const magic = "wtfsdumpv0.0   \n";
    const large_slices = self.large_files.slice();
    const large_dirs = large_slices.items(.directory_index);
    const large_names = large_slices.items(.basename);
    const large_sizes = large_slices.items(.size);

    try writer.writeAll(magic);
    try writer.writeStruct(results.totals, .little);
    try writer.writeAll(self.namedata.items);

    // const len_snapshot = self.directories.len.load(.acquire);
    // const snapshot = self.directories.slices();

    // var buf32: [4]u8 = undefined;
    // var buf64: [8]u8 = undefined;

    // var idx: usize = 0;
    // while (idx < len_snapshot) : (idx += 1) {
    //     std.mem.writeIntLittle(u32, buf32[0..], self.directories.ptr(.parent, idx).*);
    //     try writer.writeAll(buf32[0..]);
    // }

    // idx = 0;
    // while (idx < len_snapshot) : (idx += 1) {
    //     std.mem.writeIntLittle(u32, buf32[0..], self.directories.ptr(.basename, idx).*);
    //     try writer.writeAll(buf32[0..]);
    // }

    // idx = 0;
    // while (idx < len_snapshot) : (idx += 1) {
    //     std.mem.writeIntLittle(u64, buf64[0..], self.directories.ptr(.total_size, idx).*);
    //     try writer.writeAll(buf64[0..]);
    // }

    // idx = 0;
    // while (idx < len_snapshot) : (idx += 1) {
    //     const value: u64 = @intCast(self.directories.ptr(.total_files, idx).*);
    //     std.mem.writeIntLittle(u64, buf64[0..], value);
    //     try writer.writeAll(buf64[0..]);
    // }

    // idx = 0;
    // while (idx < len_snapshot) : (idx += 1) {
    //     const value: u64 = @intCast(self.directories.ptr(.total_dirs, idx).*);
    //     std.mem.writeIntLittle(u64, buf64[0..], value);
    //     try writer.writeAll(buf64[0..]);
    // }

    try writer.writeSliceEndian(usize, large_dirs, .little);
    try writer.writeSliceEndian(u32, large_names, .little);
    try writer.writeSliceEndian(u64, large_sizes, .little);
    try writer.flush();
}
