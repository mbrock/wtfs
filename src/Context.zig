const std = @import("std");

const strpool = @import("wtfs").strpool;
const TaskQueue = @import("TaskQueue.zig");

pub const DirectoryNode = struct {
    parent: u32,
    basename: u32,
    fd: std.posix.fd_t = invalid_fd,
    fdrefcount: AtomicU16,
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};

pub const DirectoryStore = std.MultiArrayList(DirectoryNode);

pub const Stats = struct {
    directories_started: AtomicUsize = AtomicUsize.init(0),
    directories_completed: AtomicUsize = AtomicUsize.init(0),
    directories_scheduled: AtomicUsize = AtomicUsize.init(0),
    directories_discovered: AtomicUsize = AtomicUsize.init(0),
    files_discovered: AtomicUsize = AtomicUsize.init(0),
    symlinks_discovered: AtomicUsize = AtomicUsize.init(0),
    other_discovered: AtomicUsize = AtomicUsize.init(0),
    scanner_batches: AtomicUsize = AtomicUsize.init(0),
    scanner_entries: AtomicUsize = AtomicUsize.init(0),
    scanner_max_batch: AtomicUsize = AtomicUsize.init(0),
    scanner_errors: AtomicUsize = AtomicUsize.init(0),
    inaccessible_dirs: AtomicUsize = AtomicUsize.init(0),
    high_watermark: AtomicUsize = AtomicUsize.init(0),

    pub fn init() Stats {
        return .{};
    }
};

pub const AtomicUsize = std.atomic.Value(usize);
pub const AtomicU16 = std.atomic.Value(u16);
pub const invalid_fd: std.posix.fd_t = -1;

const Context = @This();

allocator: std.mem.Allocator,
directories: *DirectoryStore,
pool: *std.Thread.Pool,
wait_group: *std.Thread.WaitGroup,
progress_node: std.Progress.Node,
errprogress: std.Progress.Node,
skip_hidden: bool,
directories_mutex: std.Thread.Mutex = .{},
task_queue: TaskQueue,
outstanding: AtomicUsize = AtomicUsize.init(0),

namelock: std.Thread.Mutex = .{},
namedata: *std.ArrayList(u8),
idxset: *strpool.IndexSet,
stats: Stats,

pub fn getNode(self: *Context, index: usize) DirectoryNode {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    return self.directories.get(index);
}

fn internPath(self: *Context, path: []const u8) !u32 {
    self.namelock.lock();
    defer self.namelock.unlock();

    return strpool.intern(self.idxset, self.allocator, self.namedata, path);
}

pub fn setTotals(self: *Context, index: usize, size: u64, files: usize) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    var slices = self.directories.slice();
    slices.items(.total_size)[index] = size;
    slices.items(.total_files)[index] = files;
    slices.items(.total_dirs)[index] = 1;
}

pub fn markInaccessible(self: *Context, index: usize) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    var slices = self.directories.slice();
    const fdrefs = slices.items(.fdrefcount);
    const ref_ptr = &fdrefs[index];
    const current_fd = slices.items(.fd)[index];
    if (current_fd != invalid_fd) {
        std.posix.close(current_fd);
        slices.items(.fd)[index] = invalid_fd;
    }
    ref_ptr.store(0, .release);
    slices.items(.inaccessible)[index] = true;
    slices.items(.total_size)[index] = 0;
    slices.items(.total_files)[index] = 0;
    slices.items(.total_dirs)[index] = 1;
}

pub fn addRoot(self: *Context, path: []const u8, dir: ?std.fs.Dir, inaccessible: bool) !usize {
    var dir_copy = dir;
    errdefer if (dir_copy) |*d| d.close();

    const name_copy = try self.internPath(path);

    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();

    const index = try self.directories.addOne(self.allocator);
    var slices = self.directories.slice();
    slices.items(.parent)[index] = 0;
    slices.items(.basename)[index] = name_copy;
    slices.items(.fd)[index] = if (dir_copy) |d| d.fd else invalid_fd;
    slices.items(.total_size)[index] = 0;
    slices.items(.total_files)[index] = 0;
    slices.items(.total_dirs)[index] = 1;
    slices.items(.inaccessible)[index] = inaccessible;
    slices.items(.fdrefcount)[index] = AtomicU16.init(0);

    return index;
}

/// Caller must hold directories_mutex.
pub fn addChild(self: *Context, parent_index: usize, name: []const u8) !usize {
    const name_copy = try self.internPath(name);

    const index = try self.directories.addOne(self.allocator);
    var slices = self.directories.slice();
    slices.items(.parent)[index] = @intCast(parent_index);
    slices.items(.basename)[index] = name_copy;
    slices.items(.fd)[index] = invalid_fd;
    slices.items(.total_size)[index] = 0;
    slices.items(.total_files)[index] = 0;
    slices.items(.total_dirs)[index] = 1;
    slices.items(.inaccessible)[index] = false;
    slices.items(.fdrefcount)[index] = AtomicU16.init(0);

    return index;
}

pub fn setDirectoryFd(self: *Context, index: usize, fd: std.posix.fd_t) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    var slices = self.directories.slice();
    slices.items(.fd)[index] = fd;
}

/// Caller must hold directories_mutex.
pub fn closeDirectory(self: *Context, index: usize) void {
    var slices = self.directories.slice();
    const fd = slices.items(.fd)[index];
    if (fd != invalid_fd) {
        std.posix.close(fd);
        slices.items(.fd)[index] = invalid_fd;
    }
}

/// Caller must hold directories_mutex.
pub fn fdRefAdd(self: *Context, index: usize, value: u16, comptime order: std.builtin.AtomicOrder) u16 {
    var slices = self.directories.slice();
    return slices.items(.fdrefcount)[index].fetchAdd(value, order);
}

/// Caller must hold directories_mutex.
pub fn fdRefSub(self: *Context, index: usize, value: u16, comptime order: std.builtin.AtomicOrder) u16 {
    var slices = self.directories.slice();
    return slices.items(.fdrefcount)[index].fetchSub(value, order);
}

pub fn scheduleDirectory(self: *Context, index: usize) !void {
    _ = self.outstanding.fetchAdd(1, .acq_rel);
    errdefer _ = self.outstanding.fetchSub(1, .acq_rel);
    try self.task_queue.push(self.allocator, index);
    _ = self.stats.directories_scheduled.fetchAdd(1, .monotonic);
}
