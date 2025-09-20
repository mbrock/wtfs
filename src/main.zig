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
    items: std.ArrayList(usize) = .{},
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
    namedata: *std.ArrayList(u8),
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

    fn buildPathInBuffer(self: *Context, index: usize, buffer: *std.ArrayList(u8)) ![]const u8 {
        buffer.clearRetainingCapacity();
        try self.appendPathRecursive(buffer, index);
        return buffer.items;
    }

    fn buildChildPathInBuffer(self: *Context, parent_index: usize, name: []const u8, buffer: *std.ArrayList(u8)) ![]const u8 {
        const base = try self.buildPathInBuffer(parent_index, buffer);
        if (base.len > 0 and name.len > 0 and base[base.len - 1] != '/') {
            try buffer.append(self.allocator, '/');
        }
        try buffer.appendSlice(self.allocator, name);
        return buffer.items;
    }

    fn buildPathOwned(self: *Context, allocator: std.mem.Allocator, index: usize) ![]u8 {
        var buffer = std.ArrayList(u8){};
        defer buffer.deinit(allocator);
        try self.appendPathRecursive(&buffer, index);
        return buffer.toOwnedSlice(allocator);
    }

    fn appendPathRecursive(self: *Context, buffer: *std.ArrayList(u8), index: usize) !void {
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
    var path_buffer = std.ArrayList(u8){};
    defer path_buffer.deinit(ctx.allocator);

    var progress = ctx.progress_node.start("worker", 0);

    var namebuf: [std.fs.max_name_bytes]u8 = undefined;
    // var progbuf: [256]u8 = undefined;
    // var mytotalsize: u64 = 0;

    var i: usize = 0;
    while (true) {
        const maybe_index = ctx.task_queue.pop();
        if (maybe_index == null) break;
        const index = maybe_index.?;

        if (@mod(i, 100) == 0) {
            const basename = takename(ctx, ctx.getNode(index).basename, &namebuf);
            progress.setName(basename);
        }
        i += 1;

        //        progress.increaseEstimatedTotalItems(1);
        defer progress.completeOne();

        defer ctx.progress_node.completeOne();

        _ = processDirectory(ctx, &progress, index) catch |err| blk: {
            std.debug.print("error processing directory {d}: {t}\n", .{ index, err });
            _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
            ctx.markInaccessible(index);
            break :blk 0;
        };

        if (ctx.outstanding.fetchSub(1, .acq_rel) == 1) {
            ctx.task_queue.close();
        }
    }
}

fn takename(ctx: *Context, idx: u32, buffer: []u8) []const u8 {
    ctx.namelock.lock();
    defer ctx.namelock.unlock();

    const slice = std.mem.sliceTo(ctx.namedata.items[idx..], 0);
    @memcpy(buffer[0..slice.len], slice);

    return buffer[0..slice.len];
}

fn processDirectory(
    ctx: *Context,
    _: *std.Progress.Node,
    index: usize,
) !usize {
    var buf: [16 * 1024]u8 = undefined;
    var namebuf: [std.fs.max_name_bytes]u8 = undefined;

    const node = ctx.getNode(index);
    const basename = takename(ctx, node.basename, &namebuf);

    // var scanprogress = progress.start(basename, 0);
    // defer scanprogress.end();

    if (node.parent != null) {
        const parent = ctx.getNode(node.parent.?);

        const opened = std.posix.openat(parent.dir.?.fd, basename, .{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        }, 0);

        const refcnt = parent.fdrefcount.fetchSub(1, .seq_cst);
        if (refcnt == 1) {
            parent.dir.?.close();
        }

        if (opened) |fd| {
            node.dir = std.fs.Dir{ .fd = fd };
        } else |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            },
            error.FileNotFound => {
                std.debug.panic("directory disappeared: {s}\n", .{basename});
            },

            else => return err,
        }
    } else {
        const parent = ctx.getNode(index);
        const opened = std.posix.openat(parent.dir.?.fd, basename, .{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        }, 0) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            },
            else => return err,
        };
        node.dir = std.fs.Dir{ .fd = opened };
    }

    var scanner = Scanner.init(node.dir.?.fd, &buf);
    var total_size: u64 = 0;
    var total_files: usize = 0;

    _ = node.fdrefcount.fetchAdd(1, .acq_rel);

    while (true) {
        if (scanner.next()) |it| {
            //            scanprogress.setEstimatedTotalItems(scanner.n);
            if (it) |entry| {
                const name = std.mem.sliceTo(entry.name, 0);
                if (name.len == 0) continue;
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

                switch (entry.kind) {
                    .dir => {
                        if (ctx.skip_hidden and name[0] == '.') continue;

                        _ = node.fdrefcount.fetchAdd(1, .acq_rel);
                        const child_index = try ctx.addChild(index, name);
                        //                        ctx.progress_node.increaseEstimatedTotalItems(1);
                        try ctx.scheduleDirectory(child_index);
                    },
                    .file => {
                        const file = entry.details.file;
                        total_size += file.allocsize;
                        total_files += 1;
                    },
                    .symlink, .other => {},
                }
            } else break;
        } else |err| {
            if (err == error.DeadLock) {
                // iCloud dataless file
                _ = ctx.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            }
            return err;
        }
    }

    if (node.fdrefcount.fetchSub(1, .acq_rel) == 1) {
        node.dir.?.close();
    }

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
        .progress_node = progress_root,
        .skip_hidden = skip_hidden,
        .namedata = &namedata,
        .idxset = &idxset,
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

    progress_root.end();

    const dir_count = ctx.directories.count();
    progress_root.setName("Summarizing");
    progress_root.setEstimatedTotalItems(dir_count);
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
        progress_root.completeOne();
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

    var top_level_entries = std.ArrayList(SummaryEntry){};
    progress_root.setName("Building paths");
    progress_root.setEstimatedTotalItems(dir_count);
    var depth_index: usize = 0;
    while (depth_index < dir_count) : (depth_index += 1) {
        const node = ctx.getNode(depth_index);
        if (node.depth == 1) {
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

    if (top_level_entries.items.len > 0) {
        try stdout.print("\nTop-level directories by total size:\n\n", .{});
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
