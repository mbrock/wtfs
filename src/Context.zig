const std = @import("std");

const strpool = @import("pool.zig");
const TaskQueue = @import("TaskQueue.zig");

pub const DirectoryNode = struct {
    parent: u32,
    basename: u32,
    fd: std.posix.fd_t = invalid_fd,
    fdrefcount: AtomicU16 = .init(0),
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};

pub const LargeFile = struct {
    directory_index: usize,
    basename: u32,
    size: u64,
};

pub const AtomicUsize = std.atomic.Value(usize);
pub const AtomicU16 = std.atomic.Value(u16);
pub const invalid_fd: std.posix.fd_t = -1;

const Context = @This();

allocator: std.mem.Allocator,

directories_mutex: std.Thread.Mutex = .{},
directories: *std.MultiArrayList(DirectoryNode),

namelock: std.Thread.Mutex = .{},
namedata: *std.ArrayList(u8),
idxset: *strpool.IndexSet,

large_files: *std.MultiArrayList(LargeFile),

task_queue: *TaskQueue,
outstanding: AtomicUsize = AtomicUsize.init(0),

pool: *std.Thread.Pool,
wait_group: *std.Thread.WaitGroup,

progress_node: std.Progress.Node,
errprogress: std.Progress.Node,

skip_hidden: bool,
large_file_threshold: u64,

pub fn internPath(self: *Context, path: []const u8) !u32 {
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
}

pub fn addRoot(self: *Context, path: []const u8, dir: std.fs.Dir) !usize {
    const name_copy = try self.internPath(path);

    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();

    const index = try self.directories.addOne(self.allocator);
    var slices = self.directories.slice();

    slices.items(.parent)[index] = 0;
    slices.items(.basename)[index] = name_copy;
    slices.items(.fd)[index] = dir.fd;
    slices.items(.total_size)[index] = 0;
    slices.items(.total_files)[index] = 0;
    slices.items(.total_dirs)[index] = 1;
    slices.items(.inaccessible)[index] = false;
    slices.items(.fdrefcount)[index] = AtomicU16.init(0);

    return index;
}

// ===== Parent Directory File Descriptor Lifecycle Management =====
//
// These methods manage the reference counting of directory file descriptors.
// A directory's fd must stay open as long as any child might need to be opened
// via openat() from it. We use reference counting to track how many pending
// child operations still need the parent fd.

/// Store the opened file descriptor for a directory (thread-safe)
pub fn setDirectoryFd(self: *Context, index: usize, fd: std.posix.fd_t) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    var slices = self.directories.slice();
    slices.items(.fd)[index] = fd;
}

/// Increment reference count to keep a parent directory fd open (thread-safe)
/// Call when scheduling a child directory that will need openat() from this parent
pub fn retainParentFd(self: *Context, parent_index: usize) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();
    self.retainParentFdLocked(parent_index);
}

/// Increment reference count when already holding the directories_mutex
/// Used from batch processing where we hold the lock for performance
pub fn retainParentFdLocked(self: *Context, parent_index: usize) void {
    var slices = self.directories.slice();
    _ = slices.items(.fdrefcount)[parent_index].fetchAdd(1, .acq_rel);
}

pub fn recordLargeFileLocked(
    self: *Context,
    directory_index: usize,
    name: []const u8,
    size: u64,
) !void {
    const name_copy = try self.internPath(name);
    try self.large_files.append(self.allocator, .{
        .directory_index = directory_index,
        .basename = name_copy,
        .size = size,
    });
}

pub fn recordLargeFile(
    self: *Context,
    directory_index: usize,
    name: []const u8,
    size: u64,
) !void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();

    try self.recordLargeFileLocked(directory_index, name, size);
}

/// Decrement reference count and close fd if no longer needed (thread-safe)
/// Call after opening a child directory or when done scanning
pub fn releaseParentFd(self: *Context, parent_index: usize) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();

    var slices = self.directories.slice();
    const prev_count = slices.items(.fdrefcount)[parent_index].fetchSub(1, .acq_rel);

    // If this was the last child needing this parent fd, close it
    if (prev_count == 1) {
        const fd = slices.items(.fd)[parent_index];
        if (fd != invalid_fd) {
            std.posix.close(fd);
            slices.items(.fd)[parent_index] = invalid_fd;
        }
    }
}

/// Release parent fd after successfully opening a child directory from it
/// Uses stricter memory ordering to ensure parent-child operations are properly sequenced
pub fn releaseParentFdAfterOpen(self: *Context, parent_index: usize) void {
    self.directories_mutex.lock();
    defer self.directories_mutex.unlock();

    var slices = self.directories.slice();
    const prev_count = slices.items(.fdrefcount)[parent_index].fetchSub(1, .seq_cst);

    // If this was the last child needing this parent fd, close it
    if (prev_count == 1) {
        const fd = slices.items(.fd)[parent_index];
        if (fd != invalid_fd) {
            std.posix.close(fd);
            slices.items(.fd)[parent_index] = invalid_fd;
        }
    }
}
