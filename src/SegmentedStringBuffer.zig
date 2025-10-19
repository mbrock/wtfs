const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const CapacityLatch = @import("CapacityLatch.zig");
const test_threads = @import("test_threading.zig");

/// A lock-free, append-only buffer for storing strings in exponentially-sized shelves.
///
/// Designed for efficient concurrent appends without requiring a global lock. Multiple threads
/// can append simultaneously using atomic reservations coordinated by a capacity latch that
/// serializes growth operations while allowing concurrent appends to proceed independently.
///
/// Strings are stored contiguously within individual shelves (never straddling boundaries).
/// This wastes some space when a string would cross a shelf boundary, but the waste is
/// negligible since shelves grow exponentially (1KB, 2KB, 4KB, 8KB, ...) and strings are
/// typically short.
///
/// **Stability guarantee**: Pointers to appended data remain valid for the lifetime of the
/// buffer. Shelves are never reallocated or moved once created.
pub const SegmentedStringBuffer = struct {
    const Self = @This();

    const base_shift: comptime_int = 10; // 1024 bytes
    const base_unit: usize = @as(usize, 1) << base_shift;
    const max_supported_shelves: usize = 32; // 32 shelves supports up to ~4GB
    const ShiftInt = std.math.Log2Int(usize);

    /// Size of the first shelf in bytes (1024).
    pub const first_shelf_bytes = base_unit;

    /// Statistics about buffer usage.
    pub const DebugStats = struct {
        shelf_count: usize,
        length: usize,
        occupied_bytes: usize,
        capacity_bytes: usize,
    };

    const Location = struct {
        shelf_index: usize,
        offset: usize,
    };

    /// A 32-bit byte offset into the buffer, supporting up to 4GB of string data.
    /// The shelf index and offset within the shelf are computed dynamically using bit math.
    pub const Index = u32;

    inline fn indexToLocation(idx: Index) Location {
        const global_offset = @as(usize, idx);
        // Given geometrically-sized shelves starting at base_unit:
        // Shelf 0: base_unit bytes (offset 0 to base_unit-1)
        // Shelf 1: 2*base_unit bytes (offset base_unit to 3*base_unit-1)
        // Shelf N: 2^N * base_unit bytes
        // Total bytes before shelf N: base_unit * (2^N - 1)
        
        // Find shelf: floor(log2((global_offset / base_unit) + 1))
        const units = (global_offset >> base_shift) + 1;
        const shelf_index = std.math.log2_int(usize, units);
        
        // Find offset within shelf
        const shelf_start = shelfStartBytes(shelf_index);
        const offset = global_offset - shelf_start;
        
        return .{ .shelf_index = shelf_index, .offset = offset };
    }

    inline fn locationToIndex(loc: Location) Index {
        const global_offset = shelfStartBytes(loc.shelf_index) + loc.offset;
        return @intCast(global_offset);
    }

    /// A reference to a null-terminated contiguous string in the buffer.
    /// Only stores the 32-bit byte offset; length is computed by scanning for the null terminator.
    /// This reduces memory usage from 128 bits to 32 bits per reference.
    pub const Slice = Index;

    shelves: [max_supported_shelves][]u8 = mem.zeroes([max_supported_shelves][]u8),
    shelf_count: u8 = 0,
    latch: CapacityLatch = .{},

    pub const empty: Self = .{};

    /// Free all allocated shelves.
    pub fn deinit(self: *Self, alloc: Allocator) void {
        const count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const shelf = self.shelves[i];
            if (shelf.len != 0) alloc.free(shelf);
            self.shelves[i] = (@constCast(&[_]u8{}))[0..0];
        }
        self.* = undefined;
    }

    /// Append a string to the buffer with a null terminator and return a slice pointing to it.
    ///
    /// A null byte is automatically appended after the string data.
    /// The slice points to stable memory; subsequent appends do not invalidate it.
    ///
    /// Thread-safe: can be called concurrently from multiple threads.
    pub fn append(
        self: *Self,
        alloc: Allocator,
        bytes: []const u8,
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!Slice {
        if (bytes.len == 0) {
            const start: u32 = @intCast(try self.reserveContiguous(alloc, 1));
            const start_loc = indexToLocation(start);
            self.shelves[start_loc.shelf_index][start_loc.offset] = 0;
            return start;
        }

        const total_len = bytes.len + 1; // +1 for null terminator
        const start: u32 = @intCast(try self.reserveContiguous(alloc, total_len));
        const start_loc = indexToLocation(start);
        const dest = self.shelves[start_loc.shelf_index][start_loc.offset .. start_loc.offset + total_len];
        @memcpy(dest[0..bytes.len], bytes);
        dest[bytes.len] = 0; // Add null terminator
        return start;
    }

    /// Total bytes occupied (including padding/waste from straddling).
    pub fn len(self: *const Self) usize {
        return self.latch.len.load(.acquire);
    }

    /// Alias for len().
    pub fn occupiedBytes(self: *const Self) usize {
        return self.len();
    }

    /// Total bytes of allocated shelf space.
    pub fn capacityBytes(self: *const Self) usize {
        return self.latch.cap.load(.acquire);
    }

    /// Number of shelves currently allocated.
    pub fn shelfCount(self: *const Self) usize {
        return @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
    }

    /// Check if the buffer is empty.
    pub fn isEmpty(self: *const Self) bool {
        return self.len() == 0;
    }

    /// Get detailed statistics about buffer usage.
    pub fn debugStats(self: *const Self) DebugStats {
        return .{
            .shelf_count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire)),
            .length = self.len(),
            .occupied_bytes = self.occupiedBytes(),
            .capacity_bytes = self.capacityBytes(),
        };
    }

    const empty_cstring = [_:0]u8{0};

    /// Get the raw bytes for a slice (including the null terminator).
    /// Scans for the null terminator to determine length.
    pub fn sliceBytes(self: *const Self, slice: Slice) []const u8 {
        const loc = indexToLocation(slice);
        const shelf = self.shelves[loc.shelf_index];
        const string_len = mem.indexOfScalar(u8, shelf[loc.offset..], 0) orelse 0;
        return shelf[loc.offset .. loc.offset + string_len + 1];
    }

    /// Get the value bytes for a slice (excluding the null terminator).
    /// Scans for the null terminator to determine length.
    pub fn sliceValue(self: *const Self, slice: Slice) []const u8 {
        const loc = indexToLocation(slice);
        const shelf = self.shelves[loc.shelf_index];
        const string_len = mem.indexOfScalar(u8, shelf[loc.offset..], 0) orelse 0;
        return shelf[loc.offset .. loc.offset + string_len];
    }

    /// Get the bytes as a null-terminated C string.
    /// Scans for the null terminator to determine length.
    pub fn sliceCString(self: *const Self, slice: Slice) [:0]const u8 {
        const loc = indexToLocation(slice);
        const shelf = self.shelves[loc.shelf_index];
        const string_len = mem.indexOfScalar(u8, shelf[loc.offset..], 0) orelse 0;
        if (string_len == 0) return empty_cstring[0..1 :0];
        return shelf[loc.offset .. loc.offset + string_len :0];
    }

    fn reserveContiguous(
        self: *Self,
        alloc: Allocator,
        reserve_len: usize,
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!usize {
        // Retry loop handles race conditions with concurrent appends:
        // Between checking if we'd straddle and actually reserving, another thread
        // might move the cursor forward. We recheck after each reservation.
        while (true) {
            const len_snapshot = self.len();
            const loc = indexToLocation(@intCast(len_snapshot));
            const shelf = self.shelves[loc.shelf_index];
            const available_contiguously = shelf.len - loc.offset;

            // Strings must be contiguous within a single shelf. If this append
            // would straddle shelf boundaries, skip the remaining space on the
            // current shelf. This waste is negligible since strings are short and
            // shelves grow exponentially.
            if (available_contiguously > 0 and available_contiguously < reserve_len) {
                _ = try self.reserveRange(alloc, available_contiguously);
                continue;
            }

            // Atomically reserve space for the string.
            const start = try self.reserveRange(alloc, reserve_len);
            const start_loc = indexToLocation(@intCast(start));
            const start_shelf = self.shelves[start_loc.shelf_index];

            // Race check: another thread may have moved us forward while we were
            // reserving. Verify we still have contiguous space, retry if not.
            if (start_shelf.len - start_loc.offset < reserve_len) {
                std.atomic.spinLoopHint();
                continue;
            }

            return start;
        }
    }

    fn reserveRange(
        self: *Self,
        alloc: Allocator,
        reserve_len: usize,
    ) (Allocator.Error || error{ Overflow, ShelfLimitReached })!usize {
        if (reserve_len == 0) {
            return self.latch.len.load(.acquire);
        }

        // The latch coordinates concurrent reservations atomically. Each thread
        // gets a unique range of bytes that won't overlap with other threads.
        var reservation = try self.latch.reservation(reserve_len);
        while (true) {
            // Ensure we have physical capacity for the reservation
            try self.ensureCapacityTo(alloc, reservation.len);

            // Atomically claim the reservation if still valid, otherwise retry
            if (reservation.take()) |base| {
                return base;
            }
            reservation = try self.latch.reservation(reserve_len);
            std.atomic.spinLoopHint();
        }
    }

    fn ensureCapacityTo(
        self: *Self,
        alloc: Allocator,
        target: usize,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        // Fast path: already have enough capacity
        while (true) {
            const snap = self.latch.snapshot();
            if (snap.cap >= target) return;

            // The latch ensures only one thread grows at a time. Other threads
            // wait for the grower to publish the new capacity.
            if (self.latch.acquireOrWaitFor(target)) |growth_value| {
                var g = growth_value;
                defer g.unlock();

                try self.growUntilCapacityLocked(alloc, target);
                const new_cap = self.totalCapacity();
                std.debug.assert(new_cap >= target);
                std.debug.assert(new_cap >= g.start_cap);
                g.publishTo(new_cap);
            } else {
                // Another thread grew the buffer, loop to recheck
                return;
            }
        }
    }

    fn growUntilCapacityLocked(
        self: *Self,
        alloc: Allocator,
        target: usize,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        while (self.totalCapacity() < target) {
            try self.growOneShelf(alloc);
        }
    }

    fn growOneShelf(
        self: *Self,
        alloc: Allocator,
    ) (Allocator.Error || error{ShelfLimitReached})!void {
        const index = @as(usize, @atomicLoad(u8, &self.shelf_count, .monotonic));
        if (index >= max_supported_shelves) return error.ShelfLimitReached;

        const capacity = shelfCapacity(index);
        if (capacity == 0) return error.ShelfLimitReached;

        const buf = try alloc.alloc(u8, capacity);
        errdefer alloc.free(buf);

        std.debug.assert(self.shelves[index].len == 0);
        self.shelves[index] = buf;
        @atomicStore(u8, &self.shelf_count, @intCast(index + 1), .release);
    }

    /// Calculate the capacity of a shelf at the given index.
    /// Shelves grow exponentially: shelf 0 = 1KB, shelf 1 = 2KB, shelf 2 = 4KB, etc.
    pub fn shelfCapacity(index: usize) usize {
        if (index >= max_supported_shelves) return 0;
        var size: usize = base_unit;
        var i: usize = 0;
        while (i < index) : (i += 1) {
            size = std.math.mul(usize, size, 2) catch return 0;
        }
        return size;
    }

    fn shelfStartBytes(shelf_index: usize) usize {
        var total: usize = 0;
        var size: usize = base_unit;
        var i: usize = 0;
        while (i < shelf_index) : (i += 1) {
            total += size;
            size = std.math.mul(usize, size, 2) catch return std.math.maxInt(usize);
        }
        return total;
    }

    fn totalCapacity(self: *const Self) usize {
        const count = @as(usize, @atomicLoad(u8, &self.shelf_count, .acquire));
        var total: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            total += self.shelves[i].len;
        }
        return total;
    }



    fn toMutableShelfSlices(slice: [][]u8) [][]u8 {
        const ptr = @as([*][]u8, @ptrCast(slice.ptr));
        return ptr[0..slice.len];
    }
};

fn expectViewEquals(
    expected: []const u8,
    buffer: *SegmentedStringBuffer,
    slice: SegmentedStringBuffer.Slice,
) !void {
    try std.testing.expectEqualStrings(expected, buffer.sliceValue(slice));
}

test "SegmentedStringBuffer append and view" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first = (try buffer.append(allocator, "hello"));
    const second = (try buffer.append(allocator, "world"));

    try std.testing.expectEqual(@as(usize, 12), buffer.len()); // 5 + 1 + 5 + 1 (null terminators)
    try expectViewEquals("hello", &buffer, first);
    try expectViewEquals("world", &buffer, second);

    const stats = buffer.debugStats();
    try std.testing.expectEqual(@as(usize, 12), stats.length);
}

test "SegmentedStringBuffer spans multiple shelves" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const first_data = "a" ** (SegmentedStringBuffer.first_shelf_bytes - 1); // Leave room for null
    const first = try buffer.append(allocator, first_data);
    try std.testing.expectEqual(@as(usize, 1), buffer.shelfCount());
    try std.testing.expectEqualStrings(first_data, buffer.sliceValue(first));

    const second_len = SegmentedStringBuffer.first_shelf_bytes + 128;
    const payload = try allocator.alloc(u8, second_len);
    defer allocator.free(payload);
    @memset(payload, 'b');

    const second = try buffer.append(allocator, payload);
    try std.testing.expectEqual(@as(usize, 2), buffer.shelfCount()); // Spans 2 shelves
    try expectViewEquals(payload, &buffer, second);
}

test "SegmentedStringBuffer append keeps entries within shelf" {
    var buffer = SegmentedStringBuffer.empty;
    const allocator = std.testing.allocator;
    defer buffer.deinit(allocator);

    const near_capacity = SegmentedStringBuffer.first_shelf_bytes - 4;
    const filler = try allocator.alloc(u8, near_capacity);
    defer allocator.free(filler);
    @memset(filler, 'x');
    _ = try buffer.append(allocator, filler);

    const slice = try buffer.append(allocator, "name");
    // Check that it was placed on shelf 1 (second shelf) due to straddling
    const loc = SegmentedStringBuffer.indexToLocation(slice);
    try std.testing.expectEqual(@as(usize, 1), loc.shelf_index);
    try std.testing.expectEqualStrings("name", buffer.sliceValue(slice));
}

test "SegmentedStringBuffer concurrent append" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var buffer = SegmentedStringBuffer.empty;
    defer buffer.deinit(allocator);

    const thread_count = 4;
    const per_thread = 20;
    const total = thread_count * per_thread;

    var slices: [total]SegmentedStringBuffer.Slice = undefined;

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(
            tid: usize,
            buf: *SegmentedStringBuffer,
            alloc: Allocator,
            per: usize,
            slice_storage: []SegmentedStringBuffer.Slice,
        ) !void {
            const start = tid * per;
            var i: usize = 0;
            while (i < per) : (i += 1) {
                var tmp: [32]u8 = undefined;
                const payload = try std.fmt.bufPrint(&tmp, "t{d}-{d}", .{ tid, i });
                const res = try buf.append(alloc, payload);
                slice_storage[start + i] = res;
            }
        }
    };

    try group.spawnMany(
        thread_count,
        Worker.run,
        .{ &buffer, allocator, per_thread, &slices },
    );
    group.wait();

    for (slices, 0..) |slice, idx| {
        var tmp: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(
            &tmp,
            "t{d}-{d}",
            .{ idx / per_thread, idx % per_thread },
        );
        try expectViewEquals(expected, &buffer, slice);
    }
}
