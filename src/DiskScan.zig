const std = @import("std");
const builtin = @import("builtin");
const wtfs = @import("wtfs").mac;
const strpool = @import("wtfs").strpool;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

const TaskQueue = @import("TaskQueue.zig");
const Context = @import("Context.zig");
const Worker = @import("Worker.zig");

allocator: std.mem.Allocator,
skip_hidden: bool = true,
root: []const u8 = ".",
directories: Context.DirectoryStore = .empty,
namedata: std.ArrayList(u8) = .empty,
idxset: strpool.IndexSet = .empty,
progress_root: std.Progress.Node = undefined,

fn gatherPhase(self: *@This()) !Context.Stats {
    // forbid on-demand file content loading; don't pull in data from iCloud
    if (builtin.target.os.tag == .macos) {
        switch (wtfs.setiopolicy_np(.vfs_materialize_dataless_files, .process, 1)) {
            0 => {},
            else => |rc| {
                std.debug.panic("setiopolicy_np: {t}", .{std.posix.errno(rc)});
            },
        }
    }

    var queue_progress = self.progress_root.start("Work queue", 0);
    errdefer queue_progress.end();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = self.allocator,
    });
    defer pool.deinit();

    var wait_group: std.Thread.WaitGroup = .{};

    var ctx = Context{
        .allocator = self.allocator,
        .pool = &pool,
        .task_queue = .{ .progress = &queue_progress },
        .directories = &self.directories,
        .namedata = &self.namedata,
        .idxset = &self.idxset,
        .wait_group = &wait_group,
        .progress_node = queue_progress,
        .errprogress = self.progress_root.start("errors", 0),
        .skip_hidden = self.skip_hidden,
        .stats = Context.Stats.init(),
    };

    defer ctx.task_queue.deinit(self.allocator);

    self.progress_root.setEstimatedTotalItems(1);

    var root_inaccessible = false;
    const root_dir: ?std.fs.Dir = blk: {
        const dir_result = std.fs.cwd().openDir(
            self.root,
            .{ .iterate = true },
        ) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                root_inaccessible = true;
                _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
                break :blk null;
            },
            else => return err,
        };
        break :blk dir_result;
    };

    const root_index = try ctx.addRoot(".", root_dir, root_inaccessible);

    if (root_inaccessible) {
        ctx.markInaccessible(root_index);
        self.progress_root.completeOne();
        ctx.task_queue.close();
    } else {
        try ctx.scheduleDirectory(root_index);
    }

    const worker_count = if (builtin.single_threaded) 1 else pool.threads.len;
    var i: usize = 0;
    while (i < worker_count) : (i += 1) {
        pool.spawnWg(&wait_group, Worker.directoryWorker, .{&ctx});
    }

    wait_group.wait();

    return ctx.stats;
}

fn writeFullPath(
    self: *@This(),
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

pub fn run(self: *@This()) !void {
    var progress = std.Progress.start(.{
        .root_name = "wtfs",
    });
    errdefer progress.end();

    self.progress_root = progress;

    var total_timer = try std.time.Timer.start();
    const elapsed_ns = total_timer.read();

    const stats = try self.gatherPhase();

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

    const directories_including_root = slices.items(.total_dirs)[0];
    const files_total = slices.items(.total_files)[0];
    const bytes_total = slices.items(.total_size)[0];

    const SummaryEntry = struct {
        index: usize,
        path: []u8,
    };

    var top_level_entries = std.ArrayList(SummaryEntry){};
    self.progress_root.setName("Building paths");
    self.progress_root.setEstimatedTotalItems(self.directories.len);
    var depth_index: usize = 0;

    while (depth_index < self.directories.len) : (depth_index += 1) {
        if (depth_index == 0) {
            self.progress_root.completeOne();
            continue;
        }
        const parent = self.directories.slice().items(.parent)[depth_index];
        if (parent == 0) {
            var writer = try std.Io.Writer.Allocating.initCapacity(self.allocator, 256);
            try self.writeFullPath(depth_index, &writer.writer);

            try top_level_entries.append(self.allocator, .{
                .index = depth_index,
                .path = try writer.toOwnedSlice(),
            });
        }
        self.progress_root.completeOne();
    }

    const SortContext = struct {
        ctx: *Context.DirectoryStore,
        pub fn lessThan(ds: @This(), lhs: SummaryEntry, rhs: SummaryEntry) bool {
            const sizes = ds.ctx.items(.total_size);
            const lhssize = sizes[lhs.index];
            const rhssize = sizes[rhs.index];
            if (lhssize != rhssize) {
                return lhssize > rhssize;
            } else {
                return std.mem.lessThan(u8, lhs.path, rhs.path);
            }
        }
    };

    if (top_level_entries.items.len > 1) {
        std.sort.heap(
            SummaryEntry,
            top_level_entries.items,
            SortContext{ .ctx = &self.directories },
            SortContext.lessThan,
        );
    }

    progress.end();

    try stdout.print("{s}: {d} dirs, {d} files, {Bi: <.1} total\n\n", .{
        self.root,
        directories_including_root,
        files_total,
        bytes_total,
    });

    const inaccessible_dirs = stats.inaccessible_dirs.load(.acquire);

    if (inaccessible_dirs > 0) {
        try stdout.print("  Inaccessible directories: {d}\n", .{inaccessible_dirs});
    }

    const dirs_started = stats.directories_started.load(.acquire);
    const dirs_completed = stats.directories_completed.load(.acquire);
    const dirs_scheduled = stats.directories_scheduled.load(.acquire);
    const dirs_discovered = stats.directories_discovered.load(.acquire);
    const files_discovered = stats.files_discovered.load(.acquire);
    const symlinks_discovered = stats.symlinks_discovered.load(.acquire);
    const other_discovered = stats.other_discovered.load(.acquire);
    const batches = stats.scanner_batches.load(.acquire);
    const batch_entries = stats.scanner_entries.load(.acquire);
    const max_batch = stats.scanner_max_batch.load(.acquire);
    const scanner_errors = stats.scanner_errors.load(.acquire);
    const queue_high = stats.high_watermark.load(.acquire);

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
        "  Queue peak: {d} tasks\n  Scheduled dirs: {d}  discovered: {d}  started/completed: {d}/{d}\n",
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
            const node = self.directories.slice();
            const inaccessible = node.items(.inaccessible)[entry.index];
            const totalsize = node.items(.total_size)[entry.index];
            const totalfiles = node.items(.total_files)[entry.index];
            const marker = if (inaccessible) " (inaccessible)" else "";
            try stdout.print(
                "{d: >9.1}MiB  {s: <32}{s}  {d: >6} files\n",
                .{
                    @as(f64, @floatFromInt(totalsize)) / 1024 / 1024,
                    entry.path,
                    marker,
                    totalfiles,
                },
            );
        }
    }

    try stdout.flush();
}
