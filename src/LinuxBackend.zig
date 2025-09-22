// Clean Linux-specific implementation of the disk scanner.
// Uses std.Thread.Pool and WaitGroup for simple coordination.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;
const Table = @import("SegmentedMultiArray.zig").SegmentedMultiArray;
const strpool = @import("pool.zig");

const Self = @This();

// Shared io_uring instance
ring: linux.IoUring,
allocator: std.mem.Allocator,

// Directory state table
dirs: Table(struct {
    parent: u64,
    name: u32,
    dirfd: std.atomic.Value(i32), // -1 if not opened, -2 if error, >= 0 if open
    fdrefcount: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),
    size: std.atomic.Value(u64),
}, 512) = .empty,

// String pool for names
namedata: std.ArrayList(u8) = .empty,
idxset: strpool.IndexSet = .empty,
names_mutex: std.Thread.Mutex = .{},

// Thread coordination
thread_pool: *std.Thread.Pool,
wait_group: std.Thread.WaitGroup = .{},

pub fn init(allocator: std.mem.Allocator, thread_pool: *std.Thread.Pool) !Self {
    return Self{
        .ring = try linux.IoUring.init(1024, 0),
        .allocator = allocator,
        .thread_pool = thread_pool,
    };
}

pub fn deinit(self: *Self) void {
    self.ring.deinit();
    self.dirs.deinit(self.allocator);
    self.idxset.deinit(self.allocator);
    self.namedata.deinit(self.allocator);
}

// Add a name to the pool and return its index
fn addName(self: *Self, name: []const u8) !u32 {
    self.names_mutex.lock();
    defer self.names_mutex.unlock();
    return strpool.intern(&self.idxset, self.allocator, &self.namedata, name);
}

// Get a name from the pool (thread-safe)
fn getName(self: *Self, idx: u32) [:0]const u8 {
    self.names_mutex.lock();
    defer self.names_mutex.unlock();
    return @as([:0]const u8, @ptrCast(std.mem.sliceTo(self.namedata.items[idx..], 0)));
}

const ScanTask = struct {
    backend: *Self,
    dir_index: usize,

    pub fn run(self: ScanTask) void {
        defer self.backend.wait_group.finish();
        self.scanDirectory() catch |err| {
            std.debug.print("Scan error: {any}\n", .{err});
        };
    }

    fn scanDirectory(self: ScanTask) !void {
        const backend = self.backend;
        const dir_index = self.dir_index;

        const dirfd = backend.dirs.ptr(.dirfd, dir_index).load(.acquire);

        std.debug.print("SCAN i={d} fd={d}\n", .{ dir_index, dirfd });

        if (dirfd <= 0) {
            std.debug.print("BAD fd={d}\n", .{dirfd});
            return;
        }

        var dir = std.fs.Dir{ .fd = dirfd };
        var iter = dir.iterate();

        // Allocate a single statx buffer to reuse for all stat operations
        const statx_buf: *linux.Statx = try backend.allocator.create(linux.Statx);
        defer backend.allocator.destroy(statx_buf);

        // Submit all operations for this directory at once
        var batch_sqes: u32 = 0;

        while (try iter.next()) |entry| {
            const child_name = try backend.addName(entry.name);
            const child_index = try backend.dirs.addOne(backend.allocator);

            backend.dirs.ptr(.parent, child_index).* = dir_index;
            backend.dirs.ptr(.name, child_index).* = child_name;
            backend.dirs.ptr(.dirfd, child_index).* = std.atomic.Value(i32).init(-1);
            backend.dirs.ptr(.fdrefcount, child_index).* = std.atomic.Value(u16).init(0);
            backend.dirs.ptr(.size, child_index).* = std.atomic.Value(u64).init(0);

            const name_slice = backend.getName(child_name);

            switch (entry.kind) {
                .directory => {
                    std.debug.print("SUB openat {s}\n", .{name_slice});
                    // Submit openat for subdirectory
                    const sqe = try backend.ring.get_sqe();
                    sqe.prep_openat(
                        dirfd,
                        name_slice.ptr,
                        linux.O{ .DIRECTORY = true, .NOFOLLOW = true, .NONBLOCK = true },
                        0,
                    );
                    sqe.user_data = (child_index << 1) | 0; // 0 = openat
                    batch_sqes += 1;
                },
                .file => {
                    std.debug.print("SUB statx {s}\n", .{name_slice});
                    // Submit statx for file
                    const sqe = try backend.ring.get_sqe();
                    sqe.prep_statx(
                        dirfd,
                        name_slice.ptr,
                        0,
                        linux.STATX_BASIC_STATS,
                        statx_buf,
                    );
                    sqe.user_data = (child_index << 1) | 1; // 1 = statx
                    batch_sqes += 1;
                },
                else => {},
            }

            if (batch_sqes >= 32) {
                _ = try backend.ring.submit();
                try self.processCompletions(batch_sqes);
                batch_sqes = 0;
            }
        }

        // Submit and process remaining operations
        if (batch_sqes > 0) {
            _ = try backend.ring.submit();
            try self.processCompletions(batch_sqes);
        }

        // Decrement refcount and close if no more refs
        const old_refs = backend.dirs.ptr(.fdrefcount, dir_index).fetchSub(1, .acq_rel);
        if (old_refs == 1) {
            // Last reference, safe to close
            posix.close(dirfd);
            backend.dirs.ptr(.dirfd, dir_index).store(-2, .release);
        }
    }

    fn processCompletions(self: ScanTask, expected_count: u32) !void {
        const backend = self.backend;
        var processed: u32 = 0;
        var cqes: [32]linux.io_uring_cqe = undefined;

        while (processed < expected_count) {
            const count = try backend.ring.copy_cqes(&cqes, 1); // Wait for at least 1

            for (cqes[0..count]) |cqe| {
                const dir_index = cqe.user_data >> 1;
                const is_stat = (cqe.user_data & 1) != 0;

                if (dir_index >= backend.dirs.len.load(.acquire)) {
                    processed += 1;
                    continue;
                }

                if (is_stat) {
                    std.debug.print("STAT done i={d} r={d}\n", .{ dir_index, cqe.res });
                    // Handle statx completion
                    if (cqe.res >= 0) {
                        // Extract actual size from statx buffer in real implementation
                        _ = backend.dirs.ptr(.size, @intCast(dir_index)).fetchAdd(4096, .monotonic);
                    }
                } else {
                    // Handle openat completion
                    if (cqe.res >= 0) {
                        const name_offset = backend.dirs.ptr(.name, @intCast(dir_index)).*;
                        const name = backend.getName(name_offset);
                        std.debug.print("OPEN '{s}' -> fd={d}\n", .{ name, cqe.res });
                        backend.dirs.ptr(.dirfd, @intCast(dir_index)).store(@intCast(cqe.res), .release);

                        // Increment refcount for the new scan task
                        _ = backend.dirs.ptr(.fdrefcount, @intCast(dir_index)).fetchAdd(1, .acq_rel);

                        // Spawn new task for subdirectory
                        backend.wait_group.start();
                        const task = ScanTask{ .backend = backend, .dir_index = @intCast(dir_index) };
                        try backend.thread_pool.spawn(ScanTask.run, .{task});
                    } else {
                        backend.dirs.ptr(.dirfd, @intCast(dir_index)).store(-2, .release);
                    }
                }
                processed += 1;
            }
        }
    }
};

pub fn scan(self: *Self, root_path: []const u8) !void {
    // Open root directory
    const root_fd = try posix.open(root_path, posix.O{ .DIRECTORY = true, .ACCMODE = .RDONLY }, 0);

    // Add root to dirs table
    const root_name = try self.addName(root_path);
    const root_index = try self.dirs.addOne(self.allocator);

    self.dirs.ptr(.parent, root_index).* = 0;
    self.dirs.ptr(.name, root_index).* = root_name;
    self.dirs.ptr(.dirfd, root_index).* = std.atomic.Value(i32).init(root_fd);
    self.dirs.ptr(.fdrefcount, root_index).* = std.atomic.Value(u16).init(1); // Initial ref for root task
    self.dirs.ptr(.size, root_index).* = std.atomic.Value(u64).init(0);

    // Start scanning from root
    self.wait_group.start();
    const root_task = ScanTask{ .backend = self, .dir_index = root_index };
    try self.thread_pool.spawn(ScanTask.run, .{root_task});

    // Wait for all scanning to complete
    self.wait_group.wait();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{ .allocator = allocator, .n_jobs = 8 });
    defer thread_pool.deinit();

    var backend = try init(allocator, &thread_pool);
    defer backend.deinit();

    try backend.scan(".");

    // Print results
    std.debug.print("Scanned {} entries\n", .{backend.dirs.len.load(.acquire)});

    var total_size: u64 = 0;
    for (0..backend.dirs.len.load(.acquire)) |i| {
        total_size += backend.dirs.ptr(.size, i).load(.acquire);
    }
    std.debug.print("Total size: {} bytes\n", .{total_size});
}
