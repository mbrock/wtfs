const std = @import("std");
const wtfs = @import("wtfs").mac;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

const Scanner = wtfs.DirScanner(.{
    .common = .{
        .name = true,
        .obj_type = true,
    },
    .dir = .{
        .datalength = true,
    },
    .file = .{
        .totalsize = true,
    },
});
const AtomicUsize = std.atomic.Value(usize);

const QueueFields = struct {
    path: []u8,
    depth: usize,
    parent: ?usize,
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};

const WorkQueue = std.MultiArrayList(QueueFields);

const ItemSnapshot = struct {
    path: []const u8,
    depth: usize,
};

const Context = struct {
    allocator: std.mem.Allocator,
    queue: *WorkQueue,
    pool: *std.Thread.Pool,
    wait_group: *std.Thread.WaitGroup,
    progress_node: std.Progress.Node,
    skip_hidden: bool,
    inaccessible_dirs: AtomicUsize = AtomicUsize.init(0),
    queue_mutex: std.Thread.Mutex = .{},

    fn snapshot(self: *Context, index: usize) ItemSnapshot {
        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        const slice = self.queue.slice();
        return .{
            .path = slice.items(.path)[index],
            .depth = slice.items(.depth)[index],
        };
    }

    fn setTotals(self: *Context, index: usize, size: u64, files: usize) void {
        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        const slice = self.queue.slice();
        slice.items(.total_size)[index] = size;
        slice.items(.total_files)[index] = files;
        slice.items(.total_dirs)[index] = 1;
    }

    fn markInaccessible(self: *Context, index: usize) void {
        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        const slice = self.queue.slice();
        slice.items(.inaccessible)[index] = true;
        slice.items(.total_size)[index] = 0;
        slice.items(.total_files)[index] = 0;
        slice.items(.total_dirs)[index] = 1;
    }

    fn addDirectory(self: *Context, parent_index: usize, path: []u8) !usize {
        self.queue_mutex.lock();
        defer self.queue_mutex.unlock();

        const slice = self.queue.slice();
        const parent_depth = slice.items(.depth)[parent_index];

        const new_index = try self.queue.addOne(self.allocator);
        self.queue.set(new_index, .{
            .path = path,
            .depth = parent_depth + 1,
            .parent = parent_index,
            .total_size = 0,
            .total_files = 0,
            .total_dirs = 1,
            .inaccessible = false,
        });

        return new_index;
    }
};

fn directoryWorker(ctx: *Context, index: usize) void {
    defer ctx.progress_node.completeOne();
    processDirectory(ctx, index) catch |err| {
        const snap = ctx.snapshot(index);
        std.log.err("failed to process {s}: {s}", .{ snap.path, @errorName(err) });
        _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
        ctx.markInaccessible(index);
    };
}

fn processDirectory(ctx: *Context, index: usize) !void {
    var buf: [16 * 1024]u8 = undefined;
    const snap = ctx.snapshot(index);

    var dir = std.fs.cwd().openDir(snap.path, .{ .iterate = true }) catch |err| switch (err) {
        error.PermissionDenied, error.AccessDenied => {
            _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
            ctx.markInaccessible(index);
            return;
        },
        else => return err,
    };
    defer dir.close();

    var scanner = Scanner.init(dir.fd, &buf);
    var total_size: u64 = 0;
    var total_files: usize = 0;

    while (try scanner.next()) |entry| {
        const name = std.mem.sliceTo(entry.name, 0);
        if (name.len == 0) continue;
        if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

        const child_path = std.fs.path.join(ctx.allocator, &[_][]const u8{ snap.path, name }) catch |join_err| {
            std.log.warn("unable to join path {s}/{s}: {s}", .{ snap.path, name, @errorName(join_err) });
            continue;
        };

        switch (entry.kind) {
            .dir => {
                if (ctx.skip_hidden and name[0] == '.') {
                    ctx.allocator.free(child_path);
                    continue;
                }

                const child_index = ctx.addDirectory(index, child_path) catch |add_err| {
                    std.log.err("unable to enqueue directory {s}: {s}", .{ child_path, @errorName(add_err) });
                    ctx.allocator.free(child_path);
                    continue;
                };

                ctx.progress_node.increaseEstimatedTotalItems(1);
                ctx.pool.spawnWg(ctx.wait_group, directoryWorker, .{ ctx, child_index });
            },
            .file => {
                const file = entry.details.file;
                total_size += file.totalsize;
                total_files += 1;
                ctx.allocator.free(child_path);
            },
            .symlink, .other => {
                ctx.allocator.free(child_path);
            },
        }
    }

    ctx.setTotals(index, total_size, total_files);
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;

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

    var queue = WorkQueue{};
    defer {
        const slice = queue.slice();
        const paths = slice.items(.path);
        for (paths) |p| {
            allocator.free(p);
        }
        queue.deinit(allocator);
    }

    const root_path = try allocator.dupe(u8, root);
    const root_index = try queue.addOne(allocator);
    queue.set(root_index, .{
        .path = root_path,
        .depth = 0,
        .parent = null,
        .total_size = 0,
        .total_files = 0,
        .total_dirs = 1,
        .inaccessible = false,
    });

    var progress_root = std.Progress.start(.{ .root_name = "Scanning" });
    defer progress_root.end();

    var progress_node = progress_root.start("directories", 0);
    defer progress_node.end();
    progress_node.setEstimatedTotalItems(1);

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{ .allocator = allocator });
    defer pool.deinit();

    var wait_group: std.Thread.WaitGroup = .{};

    var ctx = Context{
        .allocator = allocator,
        .queue = &queue,
        .pool = &pool,
        .wait_group = &wait_group,
        .progress_node = progress_node,
        .skip_hidden = skip_hidden,
    };

    pool.spawnWg(&wait_group, directoryWorker, .{ &ctx, root_index });

    wait_group.wait();

    var slice = queue.slice();
    var total_size = slice.items(.total_size);
    var total_files = slice.items(.total_files);
    var total_dirs = slice.items(.total_dirs);
    const parents = slice.items(.parent);

    var idx = queue.len;
    while (idx > 0) {
        idx -= 1;
        if (parents[idx]) |parent_index| {
            total_size[parent_index] += total_size[idx];
            total_files[parent_index] += total_files[idx];
            total_dirs[parent_index] += total_dirs[idx];
        }
    }

    const directories_including_root = total_dirs[0];
    const files_total = total_files[0];
    const bytes_total = total_size[0];
    const inaccessible_dirs = ctx.inaccessible_dirs.load(.acquire);

    var top_level = std.ArrayList(usize){};
    defer top_level.deinit(allocator);

    const depths = slice.items(.depth);
    for (depths, 0..) |depth, idx2| {
        if (depth == 1) {
            try top_level.append(allocator, idx2);
        }
    }

    const paths = slice.items(.path);
    const inaccessible_flags = slice.items(.inaccessible);

    const SortContext = struct {
        totals: []const u64,
        paths: [][]u8,
        pub fn lessThan(state: @This(), lhs: usize, rhs: usize) bool {
            const a_size = state.totals[lhs];
            const b_size = state.totals[rhs];
            if (a_size == b_size) {
                return std.mem.lessThan(u8, state.paths[lhs], state.paths[rhs]);
            }
            return a_size > b_size;
        }
    };

    if (top_level.items.len > 1) {
        std.sort.heap(usize, top_level.items, SortContext{ .totals = total_size, .paths = paths }, SortContext.lessThan);
    }

    try stdout.print("Summary for {s}:\n", .{root});
    try stdout.print("  Directories (incl. root): {d}\n", .{directories_including_root});
    try stdout.print("  Files: {d}\n", .{files_total});
    try stdout.print("  Total size: {Bi}\n", .{bytes_total});
    if (inaccessible_dirs > 0) {
        try stdout.print("  Inaccessible directories: {d}\n", .{inaccessible_dirs});
    }
    if (skip_hidden) {
        try stdout.print("  Hidden entries skipped: yes\n", .{});
    }

    if (top_level.items.len > 0) {
        try stdout.print("\nTop-level directories by total size:\n", .{});
        const max_show = @min(top_level.items.len, 10);
        var i: usize = 0;
        while (i < max_show) : (i += 1) {
            const entry_index = top_level.items[i];
            const marker = if (inaccessible_flags[entry_index]) " (inaccessible)" else "";
            try stdout.print(
                "  {d:>2}. {Bi:>10}  {d:>7} files  {s}{s}\n",
                .{ i + 1, total_size[entry_index], total_files[entry_index], paths[entry_index], marker },
            );
        }
    }

    try stdout.flush();
}
