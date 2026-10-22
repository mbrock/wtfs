//! Parallel tree scan. Produces one `Row` per filesystem entry, including the
//! root. Rows are numbered so that every parent has a smaller id than its
//! children, which lets subtree totals be summed in one reverse pass.
const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");

const Scan = @This();

pub const Kind = sys.Kind;

/// Why a row has no (complete) children, if it should have had some.
pub const Err = enum(u8) {
    none,
    access_denied,
    vanished,
    other_fs,
    pseudo_fs,
    name_too_long,
    io,
    other,

    pub fn describe(e: Err) ?[]const u8 {
        return switch (e) {
            .none => null,
            .access_denied => "permission denied",
            .vanished => "vanished during scan",
            .other_fs => "not scanned: other filesystem (-x)",
            .pseudo_fs => "not scanned: pseudo filesystem",
            .name_too_long => "path too long",
            .io => "I/O error",
            .other => "error",
        };
    }

    fn from(err: sys.Error) Err {
        return switch (err) {
            error.AccessDenied => .access_denied,
            error.FileNotFound, error.NotDir => .vanished,
            error.NameTooLong => .name_too_long,
            error.InputOutput => .io,
            else => .other,
        };
    }
};

pub const no_parent = std.math.maxInt(u32);

pub const Row = struct {
    /// Directories: full path. Everything else: base name.
    str: []const u8,
    size: u64,
    alloc: u64,
    ino: u64,
    dev: u64,
    mtime_ns: i64,
    /// Sum of `du()` over this entry and everything below it.
    tree_du: u64 = 0,
    /// Number of entries in this subtree, including itself.
    tree_count: u64 = 0,
    parent: u32,
    depth: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    perm: u16,
    kind: Kind,
    /// Another path to the same inode was already counted.
    dup: bool = false,
    err: Err = .none,

    /// Bytes this row contributes to the total, as `du` counts them:
    /// allocated size, with hardlinked inodes counted once.
    pub fn du(r: *const Row) u64 {
        return if (r.dup) 0 else r.alloc;
    }

    fn fromStat(str: []const u8, parent: u32, depth: u32, st: sys.Stat) Row {
        return .{
            .str = str,
            .size = st.size,
            .alloc = st.alloc,
            .ino = st.ino,
            .dev = st.dev,
            .mtime_ns = st.mtime_ns,
            .parent = parent,
            .depth = depth,
            .nlink = st.nlink,
            .uid = st.uid,
            .gid = st.gid,
            .perm = st.perm,
            .kind = st.kind,
        };
    }
};

// ===== Options and results =====

pub const Options = struct {
    one_file_system: bool = false,
    skip_hidden: bool = false,
    threads: usize = 0,
    progress: std.Progress.Node = .none,
};

/// Must be thread-safe. Owns row segments and bookkeeping.
allocator: std.mem.Allocator,
io: std.Io,
root: []const u8,
options: Options,

rows: Rows = .{},
/// Names and paths referenced by rows. Thread-safe given `allocator` is.
strings: ?std.heap.ArenaAllocator = null,
errors: std.atomic.Value(u64) = .init(0),
elapsed_ns: u64 = 0,

/// Rows live in fixed-size segments so they never move while workers write
/// to them; ids are reserved with one atomic add per directory.
pub const Rows = struct {
    const seg_shift = 16;
    const seg_len = 1 << seg_shift;
    const max_segs = 1 << 16;

    segs: [max_segs]?[*]Row = @splat(null),
    next: std.atomic.Value(u32) = .init(0),
    grow_mutex: std.Io.Mutex = .init,

    pub fn len(self: *const Rows) usize {
        return self.next.load(.acquire);
    }

    pub fn at(self: *const Rows, id: usize) *Row {
        return &self.segs[id >> seg_shift].?[id & (seg_len - 1)];
    }

    fn reserve(self: *Rows, io: std.Io, allocator: std.mem.Allocator, n: u32) !u32 {
        const base = self.next.fetchAdd(n, .acq_rel);
        if (@as(u64, base) + n >= no_parent) return error.TooManyEntries;
        if (n == 0) return base;
        const first_seg = base >> seg_shift;
        const last_seg = (base + n - 1) >> seg_shift;
        var s = first_seg;
        while (s <= last_seg) : (s += 1) {
            if (@atomicLoad(?[*]Row, &self.segs[s], .acquire) != null) continue;
            self.grow_mutex.lockUncancelable(io);
            defer self.grow_mutex.unlock(io);
            if (self.segs[s] == null) {
                const seg = try allocator.alloc(Row, seg_len);
                @atomicStore(?[*]Row, &self.segs[s], seg.ptr, .release);
            }
        }
        return base;
    }

    pub fn deinit(self: *Rows, allocator: std.mem.Allocator) void {
        for (&self.segs) |*s| {
            if (s.*) |p| allocator.free(p[0..seg_len]);
            s.* = null;
        }
    }
};

// ===== Work queue =====

const Task = struct {
    id: u32,
};

const Queue = struct {
    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    /// LIFO keeps the walk depth-first, which bounds the queue size.
    items: std.ArrayList(Task) = .empty,
    /// Tasks pushed but not yet finished.
    outstanding: usize = 0,
    failed: ?anyerror = null,

    fn push(q: *Queue, io: std.Io, allocator: std.mem.Allocator, tasks: []const Task) !void {
        if (tasks.len == 0) return;
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        try q.items.appendSlice(allocator, tasks);
        q.outstanding += tasks.len;
        if (tasks.len == 1) q.cond.signal(io) else q.cond.broadcast(io);
    }

    fn pop(q: *Queue, io: std.Io) ?Task {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        while (q.items.items.len == 0 and q.outstanding != 0 and q.failed == null) {
            q.cond.waitUncancelable(io, &q.mutex);
        }
        if (q.failed != null) return null;
        return q.items.pop();
    }

    fn done(q: *Queue, io: std.Io) void {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        q.outstanding -= 1;
        if (q.outstanding == 0) q.cond.broadcast(io);
    }

    fn fail(q: *Queue, io: std.Io, err: anyerror) void {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        if (q.failed == null) q.failed = err;
        q.cond.broadcast(io);
    }
};

// ===== Scan =====

const Shared = struct {
    scan: *Scan,
    queue: Queue = .{},
    root_fd: sys.fd_t,
    /// Length of the root path plus separator; strip it to get a path
    /// relative to `root_fd`.
    rel_start: usize,
    progress: std.Progress.Node,
};

pub fn run(self: *Scan) !void {
    sys.preventDatalessMaterialization();
    self.strings = .init(self.allocator);
    const started = std.Io.Clock.awake.now(self.io);

    const root_z = try self.strings.?.allocator().dupeSentinel(u8, self.root, 0);
    const root_stat = try sys.statAt(std.posix.AT.FDCWD, root_z, true);
    if (root_stat.kind != .dir) return error.NotDir;
    const root_fd = try sys.openRoot(root_z);
    defer sys.close(root_fd);

    const root_id = try self.rows.reserve(self.io, self.allocator, 1);
    std.debug.assert(root_id == 0);
    self.rows.at(0).* = Row.fromStat(self.root, no_parent, 0, root_stat);

    var shared: Shared = .{
        .scan = self,
        .root_fd = root_fd,
        .rel_start = if (std.mem.endsWith(u8, self.root, "/")) self.root.len else self.root.len + 1,
        .progress = self.options.progress,
    };
    defer shared.queue.items.deinit(self.allocator);

    try shared.queue.push(self.io, self.allocator, &.{.{ .id = 0 }});

    const n_threads = if (builtin.single_threaded) 1 else if (self.options.threads != 0)
        self.options.threads
    else
        std.Thread.getCpuCount() catch 4;

    var group: std.Io.Group = .init;
    for (0..n_threads) |_| {
        try group.concurrent(self.io, worker, .{&shared});
    }
    try group.await(self.io);
    if (shared.queue.failed) |err| return err;

    try self.markHardlinkDups();
    self.elapsed_ns = @intCast(started.untilNow(self.io, .awake).toNanoseconds());
    self.sumTrees();
}

/// Count each multiply-linked inode once, like du. Which path gets charged
/// must not depend on thread timing, so pick deterministically: paths outside
/// a `.links` directory (nix-store --optimise's pool) first, then the
/// lexicographically smallest path.
fn markHardlinkDups(self: *Scan) !void {
    const Link = struct { dev: u64, ino: u64, id: u32 };
    var links: std.ArrayList(Link) = .empty;
    defer links.deinit(self.allocator);
    for (0..self.rows.len()) |i| {
        const r = self.rows.at(i);
        if (r.kind != .dir and r.nlink > 1 and r.err == .none)
            try links.append(self.allocator, .{ .dev = r.dev, .ino = r.ino, .id = @intCast(i) });
    }
    std.mem.sort(Link, links.items, {}, struct {
        fn lt(_: void, x: Link, y: Link) bool {
            return if (x.dev != y.dev) x.dev < y.dev else if (x.ino != y.ino) x.ino < y.ino else x.id < y.id;
        }
    }.lt);

    var a: std.ArrayList(u8) = .empty;
    defer a.deinit(self.allocator);
    var b: std.ArrayList(u8) = .empty;
    defer b.deinit(self.allocator);
    var i: usize = 0;
    while (i < links.items.len) {
        var j = i + 1;
        while (j < links.items.len and links.items[j].dev == links.items[i].dev and links.items[j].ino == links.items[i].ino) j += 1;
        var best = links.items[i].id;
        for (links.items[i + 1 .. j]) |l| {
            if (try self.preferLink(l.id, best, &a, &b)) best = l.id;
        }
        for (links.items[i..j]) |l| self.rows.at(l.id).dup = l.id != best;
        i = j;
    }
}

fn preferLink(self: *Scan, x: u32, y: u32, a: *std.ArrayList(u8), b: *std.ArrayList(u8)) !bool {
    const x_pool = self.inLinksDir(x);
    const y_pool = self.inLinksDir(y);
    if (x_pool != y_pool) return y_pool;
    a.clearRetainingCapacity();
    b.clearRetainingCapacity();
    try self.writePath(x, a, self.allocator);
    try self.writePath(y, b, self.allocator);
    return std.mem.lessThan(u8, a.items, b.items);
}

fn inLinksDir(self: *Scan, id: u32) bool {
    return std.mem.endsWith(u8, self.rows.at(self.rows.at(id).parent).str, "/.links");
}

/// Fill `tree_du` and `tree_count` bottom-up.
fn sumTrees(self: *Scan) void {
    const n = self.rows.len();
    var i = n;
    while (i > 0) {
        i -= 1;
        const r = self.rows.at(i);
        r.tree_du += r.du();
        r.tree_count += 1;
        if (r.parent != no_parent) {
            const p = self.rows.at(r.parent);
            p.tree_du += r.tree_du;
            p.tree_count += r.tree_count;
        }
    }
}

pub fn deinit(self: *Scan) void {
    self.rows.deinit(self.allocator);
    if (self.strings) |*s| s.deinit();
}

const Pending = struct {
    /// Offset and length in `Worker.names`.
    name_off: usize,
    name_len: usize,
    stat: sys.Error!sys.Stat,
};

const Worker = struct {
    shared: *Shared,
    /// Names and directory paths. Lives as long as the scan's allocator.
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    pending: std.ArrayList(Pending) = .empty,
    names: std.ArrayList(u8) = .empty,
    tasks: std.ArrayList(Task) = .empty,
    path_buf: std.ArrayList(u8) = .empty,
    dirbuf: []align(8) u8,
};

fn worker(shared: *Shared) void {
    const scan = shared.scan;
    var scratch_state: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer scratch_state.deinit();
    const dirbuf = std.heap.page_allocator.alignedAlloc(u8, .@"8", 256 * 1024) catch |err| {
        shared.queue.fail(scan.io, err);
        return;
    };
    defer std.heap.page_allocator.free(dirbuf);

    var w: Worker = .{
        .shared = shared,
        .allocator = scan.strings.?.allocator(),
        .scratch = scratch_state.allocator(),
        .dirbuf = dirbuf,
    };
    while (shared.queue.pop(scan.io)) |task| {
        scanDirectory(&w, task.id) catch |err| shared.queue.fail(scan.io, err);
        shared.queue.done(scan.io);
        _ = scratch_state.reset(.retain_capacity);
        w.pending = .empty;
        w.names = .empty;
        w.tasks = .empty;
        w.path_buf = .empty;
    }
}

fn scanDirectory(w: *Worker, id: u32) !void {
    const shared = w.shared;
    const scan = shared.scan;
    const io = scan.io;
    const dir = scan.rows.at(id);

    var fd: sys.fd_t = undefined;
    if (id == 0) {
        fd = shared.root_fd;
    } else {
        try w.path_buf.appendSlice(w.scratch, dir.str[shared.rel_start..]);
        try w.path_buf.append(w.scratch, 0);
        fd = sys.openDir(shared.root_fd, w.path_buf.items[0 .. w.path_buf.items.len - 1 :0]) catch |err| {
            dir.err = Err.from(err);
            _ = scan.errors.fetchAdd(1, .monotonic);
            return;
        };
    }
    defer if (id != 0) sys.close(fd);

    if (id != 0 and dir.dev != scan.rows.at(dir.parent).dev) {
        if (sys.isPseudoFs(fd)) |_| {
            dir.err = .pseudo_fs;
            return;
        }
    }

    // Read every entry first; ids for the whole directory are then reserved
    // in one step so they are contiguous.
    var reader = sys.DirReader.init(fd, w.dirbuf);
    while (true) {
        const entry = reader.next() catch |err| {
            dir.err = Err.from(err);
            _ = scan.errors.fetchAdd(1, .monotonic);
            break;
        } orelse break;
        if (scan.options.skip_hidden and entry.name[0] == '.') continue;
        if (entry.stat) |_| {} else |err| if (err == error.FileNotFound) continue;
        try w.pending.append(w.scratch, .{
            .name_off = w.names.items.len,
            .name_len = entry.name.len,
            .stat = entry.stat,
        });
        try w.names.appendSlice(w.scratch, entry.name);
    }
    if (w.pending.items.len == 0) return;

    const base = try scan.rows.reserve(io, scan.allocator, @intCast(w.pending.items.len));
    // Base names of non-directories are stored once, in one allocation.
    const names = try w.allocator.dupe(u8, w.names.items);
    const depth = dir.depth + 1;

    for (w.pending.items, 0..) |p, i| {
        const child_id: u32 = base + @as(u32, @intCast(i));
        const name = names[p.name_off..][0..p.name_len];
        const row = scan.rows.at(child_id);
        const st = p.stat catch |err| {
            row.* = Row.fromStat(name, id, depth, .{ .kind = .unknown });
            row.err = Err.from(err);
            _ = scan.errors.fetchAdd(1, .monotonic);
            continue;
        };
        if (st.kind == .dir) {
            const path = try std.mem.concat(w.allocator, u8, &.{
                dir.str,
                if (std.mem.endsWith(u8, dir.str, "/")) "" else "/",
                name,
            });
            row.* = Row.fromStat(path, id, depth, st);
            if (st.dev != dir.dev or st.mount_point) {
                if (scan.options.one_file_system) {
                    row.err = .other_fs;
                    continue;
                }
            }
            try w.tasks.append(w.scratch, .{ .id = child_id });
        } else {
            row.* = Row.fromStat(name, id, depth, st);
        }
    }
    shared.progress.completeOne();
    try shared.queue.push(io, scan.allocator, w.tasks.items);
}

/// Full path of a row.
pub fn writePath(self: *const Scan, id: usize, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    const r = self.rows.at(id);
    if (r.kind == .dir or r.parent == no_parent) {
        try out.appendSlice(allocator, r.str);
        return;
    }
    const parent = self.rows.at(r.parent).str;
    try out.appendSlice(allocator, parent);
    if (!std.mem.endsWith(u8, parent, "/")) try out.append(allocator, '/');
    try out.appendSlice(allocator, r.str);
}

test "scan totals match a hand-built tree, with hardlinks counted once" {
    const io = std.testing.io;
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "a/b/c");
    try tmp.dir.createDirPath(io, "d");
    const big: [10000]u8 = @splat('x');
    try tmp.dir.writeFile(io, .{ .sub_path = "a/b/c/big", .data = &big });
    try tmp.dir.writeFile(io, .{ .sub_path = "d/small", .data = "hi" });
    try tmp.dir.symLink(io, "a/b/c/big", "link", .{});
    // A hardlink to the big file somewhere else in the tree.
    const root_path = try tmp.dir.realPathFileAlloc(io, ".", a);
    const src = try std.fmt.allocPrintSentinel(a, "{s}/a/b/c/big", .{root_path}, 0);
    const dst = try std.fmt.allocPrintSentinel(a, "{s}/d/big2", .{root_path}, 0);
    try std.testing.expectEqual(.SUCCESS, std.posix.errno(std.posix.system.link(src, dst)));

    for (0..10) |_| {
        var scan: Scan = .{ .allocator = std.testing.allocator, .io = io, .root = root_path, .options = .{ .threads = 4 } };
        defer scan.deinit();
        try scan.run();

        // root, a, a/b, a/b/c, a/b/c/big, d, d/small, d/big2, link
        try std.testing.expectEqual(@as(usize, 9), scan.rows.len());
        const root = scan.rows.at(0);
        try std.testing.expectEqual(@as(u64, 9), root.tree_count);

        var expect_du: u64 = 0;
        var dups: usize = 0;
        var big_alloc: u64 = 0;
        for (0..scan.rows.len()) |i| {
            const r = scan.rows.at(i);
            if (r.dup) dups += 1 else expect_du += r.alloc;
            if (r.kind == .file and r.size == big.len) big_alloc = r.alloc;
            if (i != 0) try std.testing.expect(r.parent < i);
        }
        try std.testing.expectEqual(@as(usize, 1), dups);
        try std.testing.expect(big_alloc >= big.len);
        try std.testing.expectEqual(expect_du, root.tree_du);

        var path: std.ArrayList(u8) = .empty;
        var found = false;
        for (0..scan.rows.len()) |i| {
            path.clearRetainingCapacity();
            try scan.writePath(i, &path, a);
            if (std.mem.endsWith(u8, path.items, "/a/b/c/big")) {
                try std.testing.expectEqualStrings(src, path.items);
                found = true;
            }
        }
        try std.testing.expect(found);
    }
}
