const std = @import("std");
const builtin = @import("builtin");
const wtfs = @import("wtfs").mac;
const strpool = @import("wtfs").strpool;

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

const DirectoryNode = struct {
    parent: ?usize,
    depth: usize,
    basename: u32,
    dir: ?std.fs.Dir,
    fdrefcount: AtomicUsize,
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};

const DirectoryStore = std.SegmentedList(DirectoryNode, 0);

const TaskQueue = struct {
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    items: std.ArrayListUnmanaged(usize) = .{},
    done: bool = false,

    fn deinit(self: *TaskQueue, allocator: std.mem.Allocator) void {
        self.items.deinit(allocator);
    }

    fn push(self: *TaskQueue, allocator: std.mem.Allocator, value: usize) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
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

        return self.items.pop().?;
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
    skip_hidden: bool,
    inaccessible_dirs: AtomicUsize = AtomicUsize.init(0),
    directories_mutex: std.Thread.Mutex = .{},
    task_queue: TaskQueue = .{},
    outstanding: AtomicUsize = AtomicUsize.init(0),

    namelock: std.Thread.Mutex = .{},
    namedata: *std.ArrayListUnmanaged(u8),
    idxset: *strpool.IndexSet,

    fn getNode(self: *Context, index: usize) *DirectoryNode {
        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();
        return self.directories.at(index);
    }

    fn internPath(self: *Context, path: []const u8) !u32 {
        self.namelock.lock();
        defer self.namelock.unlock();

        return strpool.intern(self.idxset, self.allocator, self.namedata, path);
    }

    fn setTotals(self: *Context, index: usize, size: u64, files: usize) void {
        const node = self.getNode(index);
        node.total_size = size;
        node.total_files = files;
        node.total_dirs = 1;
    }

    fn markInaccessible(self: *Context, index: usize) void {
        const node = self.getNode(index);
        node.inaccessible = true;
        if (node.dir) |*dir| {
            dir.close();
            node.dir = null;
        }
        node.total_size = 0;
        node.total_files = 0;
        node.total_dirs = 1;
    }

    fn addRoot(self: *Context, path: []const u8, dir: ?std.fs.Dir, inaccessible: bool) !usize {
        var dir_copy = dir;
        errdefer if (dir_copy) |*d| d.close();

        const name_copy = try self.internPath(path);

        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();

        const index = self.directories.count();
        const node_ptr = try self.directories.addOne(self.allocator);
        node_ptr.* = .{
            .parent = null,
            .depth = 0,
            .basename = name_copy,
            .dir = dir_copy,
            .total_size = 0,
            .total_files = 0,
            .total_dirs = 1,
            .inaccessible = inaccessible,
            .fdrefcount = AtomicUsize.init(0),
        };

        return index;
    }

    fn addChild(self: *Context, parent_index: usize, name: []const u8) !usize {
        const name_copy = try self.internPath(name);

        self.directories_mutex.lock();
        defer self.directories_mutex.unlock();

        const parent_depth = self.directories.at(parent_index).depth;
        const index = self.directories.count();
        const node_ptr = try self.directories.addOne(self.allocator);
        node_ptr.* = .{
            .parent = parent_index,
            .depth = parent_depth + 1,
            .basename = name_copy,
            .dir = null,
            .total_size = 0,
            .total_files = 0,
            .total_dirs = 1,
            .inaccessible = false,
            .fdrefcount = .init(0),
        };

        return index;
    }

    fn scheduleDirectory(self: *Context, index: usize) !void {
        _ = self.outstanding.fetchAdd(1, .acq_rel);
        errdefer _ = self.outstanding.fetchSub(1, .acq_rel);
        try self.task_queue.push(self.allocator, index);
    }

    fn buildPathInBuffer(self: *Context, index: usize, buffer: *std.ArrayListUnmanaged(u8)) ![]const u8 {
        buffer.clearRetainingCapacity();
        try self.appendPathRecursive(buffer, index);
        return buffer.items;
    }

    fn buildChildPathInBuffer(self: *Context, parent_index: usize, name: []const u8, buffer: *std.ArrayListUnmanaged(u8)) ![]const u8 {
        const base = try self.buildPathInBuffer(parent_index, buffer);
        if (base.len > 0 and name.len > 0 and base[base.len - 1] != '/') {
            try buffer.append(self.allocator, '/');
        }
        try buffer.appendSlice(self.allocator, name);
        return buffer.items;
    }

    fn buildPathOwned(self: *Context, allocator: std.mem.Allocator, index: usize) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8){};
        defer buffer.deinit(allocator);
        try self.appendPathRecursive(&buffer, index);
        return buffer.toOwnedSlice(allocator);
    }

    fn appendPathRecursive(self: *Context, buffer: *std.ArrayListUnmanaged(u8), index: usize) !void {
        const node = self.getNode(index);
        const parent_index = node.parent;
        const basename = node.basename;
        if (parent_index) |parent| {
            try self.appendPathRecursive(buffer, parent);
            try buffer.append(self.allocator, '/');
        }
        try buffer.appendSlice(
            self.allocator,
            std.mem.sliceTo(self.namedata.items[basename..], 0),
        );
    }
};

fn directoryWorker(ctx: *Context) void {
    var path_buffer = std.ArrayListUnmanaged(u8){};
    defer path_buffer.deinit(ctx.allocator);

    while (true) {
        const maybe_index = ctx.task_queue.pop();
        if (maybe_index == null) break;
        const index = maybe_index.?;

        {
            defer ctx.progress_node.completeOne();
            processDirectory(ctx, index, &path_buffer) catch |err| {
                const path = ctx.buildPathInBuffer(index, &path_buffer) catch "<path unavailable>";
                std.log.err("failed to process {s}: {s}", .{ path, @errorName(err) });
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
            };
        }

        if (ctx.outstanding.fetchSub(1, .acq_rel) == 1) {
            ctx.task_queue.close();
        }
    }
}

fn processDirectory(
    ctx: *Context,
    index: usize,
    path_buffer: *std.ArrayList(u8),
) !void {
    var buf: [16 * 1024]u8 = undefined;

    const node = ctx.getNode(index);

    if (node.parent != null) {
        const parent = ctx.getNode(node.parent.?);

        const basename = std.mem.sliceTo(ctx.namedata.items[node.basename..], 0);
        const opened = parent.dir.?.openDir(
            basename,
            .{ .iterate = true },
        );

        const refcnt = parent.fdrefcount.fetchSub(1, .release);
        if (refcnt == 1) {
            _ = parent.fdrefcount.load(.acquire);
            parent.dir.?.close();
        }
        node.dir = opened catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return;
            },
            else => return err,
        };
    } else {
        const basename = std.mem.sliceTo(ctx.namedata.items[node.basename..], 0);
        node.dir = std.fs.cwd().openDir(
            basename,
            .{ .iterate = true },
        ) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return;
            },
            else => return err,
        };
    }

    var scanner = Scanner.init(node.dir.?.fd, &buf);
    var total_size: u64 = 0;
    var total_files: usize = 0;

    node.fdrefcount.store(1, .monotonic);

    while (try scanner.next()) |entry| {
        const name = std.mem.sliceTo(entry.name, 0);
        if (name.len == 0) continue;
        if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

        switch (entry.kind) {
            .dir => {
                if (ctx.skip_hidden and name[0] == '.') continue;

                _ = node.fdrefcount.fetchAdd(1, .monotonic) + 1;

                const child_index = ctx.addChild(index, name) catch |add_err| {
                    const full_path = ctx.buildChildPathInBuffer(index, name, path_buffer) catch "<path unavailable>";
                    std.log.err("unable to record inaccessible directory {s}: {s}", .{ full_path, @errorName(add_err) });
                    continue;
                };

                ctx.progress_node.increaseEstimatedTotalItems(1);
                ctx.scheduleDirectory(child_index) catch |sched_err| {
                    const full_path = ctx.buildPathInBuffer(child_index, path_buffer) catch "<path unavailable>";
                    std.log.err("unable to schedule directory {s}: {s}", .{ full_path, @errorName(sched_err) });
                    _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                    ctx.markInaccessible(child_index);
                    ctx.progress_node.completeOne();
                    continue;
                };
            },
            .file => {
                const file = entry.details.file;
                total_size += file.totalsize;
                total_files += 1;
            },
            .symlink, .other => {},
        }
    }

    if (node.fdrefcount.fetchSub(1, .release) == 1) {
        node.dir.?.close();
    }

    ctx.setTotals(index, total_size, total_files);
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

    var namedata = std.ArrayListUnmanaged(u8).empty;
    defer namedata.deinit(allocator);

    var idxset = strpool.IndexSet.empty;
    defer idxset.deinit(allocator);

    var progress_root = std.Progress.start(.{ .root_name = "Scanning" });
    defer progress_root.end();

    var progress_node = progress_root.start("directories", 0);
    defer progress_node.end();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{ .allocator = allocator });
    defer pool.deinit();

    var wait_group: std.Thread.WaitGroup = .{};

    var ctx = Context{
        .allocator = allocator,
        .directories = &directories,
        .pool = &pool,
        .wait_group = &wait_group,
        .progress_node = progress_node,
        .skip_hidden = skip_hidden,
        .namedata = &namedata,
        .idxset = &idxset,
    };

    defer ctx.task_queue.deinit(allocator);

    progress_node.setEstimatedTotalItems(1);

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

    const root_index = try ctx.addRoot(root, root_dir, root_inaccessible);

    if (root_inaccessible) {
        ctx.markInaccessible(root_index);
        progress_node.completeOne();
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

    const dir_count = ctx.directories.count();
    var idx = dir_count;
    while (idx > 0) {
        idx -= 1;
        const node = ctx.getNode(idx);
        if (node.parent) |parent_index| {
            const parent = ctx.getNode(parent_index);
            parent.total_size += node.total_size;
            parent.total_files += node.total_files;
            parent.total_dirs += node.total_dirs;
        }
    }

    const root_node = ctx.getNode(0);
    const directories_including_root = root_node.total_dirs;
    const files_total = root_node.total_files;
    const bytes_total = root_node.total_size;
    const inaccessible_dirs = ctx.inaccessible_dirs.load(.acquire);

    const SummaryEntry = struct {
        index: usize,
        path: []u8,
    };

    var top_level_entries = std.ArrayListUnmanaged(SummaryEntry){};
    defer {
        for (top_level_entries.items) |entry| {
            allocator.free(entry.path);
        }
        top_level_entries.deinit(allocator);
    }

    var depth_index: usize = 0;
    while (depth_index < dir_count) : (depth_index += 1) {
        const node = ctx.getNode(depth_index);
        if (node.depth == 1) {
            const path_copy = try ctx.buildPathOwned(allocator, depth_index);
            try top_level_entries.append(allocator, .{ .index = depth_index, .path = path_copy });
        }
    }

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
        std.sort.heap(SummaryEntry, top_level_entries.items, SortContext{ .ctx = &ctx }, SortContext.lessThan);
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

    if (top_level_entries.items.len > 0) {
        try stdout.print("\nTop-level directories by total size:\n", .{});
        const max_show = @min(top_level_entries.items.len, 10);
        var display_index: usize = 0;
        while (display_index < max_show) : (display_index += 1) {
            const entry = top_level_entries.items[display_index];
            const node = ctx.getNode(entry.index);
            const marker = if (node.inaccessible) " (inaccessible)" else "";
            try stdout.print(
                "  {d:>2}. {Bi:>10}  {d:>7} files  {s}{s}\n",
                .{ display_index + 1, node.total_size, node.total_files, entry.path, marker },
            );
        }
    }

    try stdout.flush();
}
