//! The output table: one row per filesystem entry, as columns. Both the Arrow
//! and Parquet writers consume `Batch`es built here, so the schema is defined
//! once.
const std = @import("std");
const Scan = @import("Scan.zig");
const Row = Scan.Row;

/// Byte counts, ids and inodes are signed 64-bit so SQL arithmetic and
/// functions like DuckDB's format_bytes() need no casts.
pub const Type = enum { utf8, i64, u32, timestamp_us };

pub const Column = struct {
    name: []const u8,
    type: Type,
    nullable: bool = false,
    doc: []const u8,
};

pub const columns = [_]Column{
    .{ .name = "path", .type = .utf8, .doc = "full path, starting with the scanned root as given" },
    .{ .name = "kind", .type = .utf8, .doc = "file, dir, symlink, fifo, socket, block, char, unknown, or deleted" },
    .{ .name = "size", .type = .i64, .doc = "apparent size in bytes (st_size)" },
    .{ .name = "alloc", .type = .i64, .doc = "bytes allocated on disk (st_blocks*512)" },
    .{ .name = "du", .type = .i64, .doc = "bytes du charges to this path: alloc, or 0 if another hardlink to the inode was counted" },
    .{ .name = "tree_du", .type = .i64, .doc = "sum of du over this entry and everything under it" },
    .{ .name = "tree_count", .type = .i64, .doc = "number of entries in this subtree, including itself" },
    .{ .name = "id", .type = .i64, .doc = "row number; the root is 0 and parents come before children" },
    .{ .name = "parent", .type = .i64, .nullable = true, .doc = "id of the containing directory (NULL for the root)" },
    .{ .name = "depth", .type = .u32, .doc = "0 for the root, 1 for its children, ..." },
    .{ .name = "dev", .type = .i64, .doc = "device id (Linux: major<<32 | minor)" },
    .{ .name = "ino", .type = .i64, .doc = "inode number" },
    .{ .name = "nlink", .type = .u32, .doc = "hard link count" },
    .{ .name = "uid", .type = .u32, .doc = "owner user id" },
    .{ .name = "gid", .type = .u32, .doc = "owner group id" },
    .{ .name = "mode", .type = .u32, .doc = "st_mode: type and permission bits" },
    .{ .name = "mtime", .type = .timestamp_us, .doc = "modification time (UTC)" },
    .{ .name = "error", .type = .utf8, .nullable = true, .doc = "why a directory was not (fully) read, else NULL" },
    .{ .name = "open_by", .type = .utf8, .nullable = true, .doc = "kind=deleted only: processes holding the file open" },
};

/// A deleted file that a process still holds open (Linux, from /proc).
pub const Deleted = struct {
    row: Row,
    path: []const u8,
    open_by: []const u8,
};

/// Everything a writer needs: the scan plus extra rows appended after it.
pub const Source = struct {
    scan: *const Scan,
    deleted: []const Deleted = &.{},

    pub fn len(self: Source) usize {
        return self.scan.rows.len() + self.deleted.len;
    }
};

pub const ColumnData = struct {
    /// Arrow-style validity bitmap (bit set = valid). Empty when no nulls.
    validity: std.ArrayList(u8) = .empty,
    null_count: usize = 0,
    /// Little-endian fixed-width values, or concatenated string bytes.
    values: std.ArrayList(u8) = .empty,
    /// utf8 only: len+1 offsets into `values`.
    offsets: std.ArrayList(i32) = .empty,

    fn clear(c: *ColumnData) void {
        c.validity.clearRetainingCapacity();
        c.null_count = 0;
        c.values.clearRetainingCapacity();
        c.offsets.clearRetainingCapacity();
    }

    fn deinit(c: *ColumnData, a: std.mem.Allocator) void {
        c.validity.deinit(a);
        c.values.deinit(a);
        c.offsets.deinit(a);
    }
};

pub const Batch = struct {
    len: usize = 0,
    cols: [columns.len]ColumnData = @splat(.{}),
    path_buf: std.ArrayList(u8) = .empty,

    pub fn deinit(b: *Batch, a: std.mem.Allocator) void {
        for (&b.cols) |*c| c.deinit(a);
        b.path_buf.deinit(a);
    }

    /// Fill the batch with rows [start, end) of `src`.
    pub fn fill(b: *Batch, a: std.mem.Allocator, src: Source, start: usize, end: usize) !void {
        b.len = end - start;
        inline for (&b.cols, columns) |*c, col| {
            c.clear();
            if (col.type == .utf8) try c.offsets.append(a, 0);
        }
        const n_scan = src.scan.rows.len();
        for (start..end) |i| {
            var open_by: ?[]const u8 = null;
            const row: *const Row = if (i < n_scan) src.scan.rows.at(i) else blk: {
                const d = &src.deleted[i - n_scan];
                open_by = d.open_by;
                break :blk &d.row;
            };
            b.path_buf.clearRetainingCapacity();
            if (i < n_scan) {
                try src.scan.writePath(i, &b.path_buf, a);
            } else {
                try b.path_buf.appendSlice(a, src.deleted[i - n_scan].path);
            }
            const kind: []const u8 = if (open_by != null) "deleted" else @tagName(row.kind);
            const mode: u32 = @as(u32, row.perm) | typeBits(row.kind);
            const mtime_us = @divFloor(row.mtime_ns, std.time.ns_per_us);
            const parent: ?u64 = if (row.parent == Scan.no_parent) null else row.parent;

            const k = i - start;
            try putPath(a, &b.cols[0], b.path_buf.items);
            try putStr(a, &b.cols[1], kind);
            try putInt(a, &b.cols[2], i64, row.size);
            try putInt(a, &b.cols[3], i64, row.alloc);
            try putInt(a, &b.cols[4], i64, row.du());
            try putInt(a, &b.cols[5], i64, row.tree_du);
            try putInt(a, &b.cols[6], i64, row.tree_count);
            try putInt(a, &b.cols[7], i64, i);
            try putOptInt(a, &b.cols[8], k, b.len, parent);
            try putInt(a, &b.cols[9], u32, row.depth);
            try putInt(a, &b.cols[10], i64, @as(i64, @bitCast(row.dev)));
            try putInt(a, &b.cols[11], i64, @as(i64, @bitCast(row.ino)));
            try putInt(a, &b.cols[12], u32, row.nlink);
            try putInt(a, &b.cols[13], u32, row.uid);
            try putInt(a, &b.cols[14], u32, row.gid);
            try putInt(a, &b.cols[15], u32, mode);
            try putInt(a, &b.cols[16], i64, mtime_us);
            try putOptStr(a, &b.cols[17], k, b.len, row.err.describe());
            try putOptStr(a, &b.cols[18], k, b.len, open_by);
        }
    }
};

fn typeBits(kind: Scan.Kind) u32 {
    return switch (kind) {
        .file => 0o100000,
        .dir => 0o040000,
        .symlink => 0o120000,
        .fifo => 0o010000,
        .socket => 0o140000,
        .block => 0o060000,
        .char => 0o020000,
        .unknown => 0,
    };
}

fn putInt(a: std.mem.Allocator, c: *ColumnData, comptime T: type, v: anytype) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, @intCast(v), .little);
    try c.values.appendSlice(a, &bytes);
}

/// Paths are bytes, but utf8 columns must be valid UTF-8 (DuckDB checks).
/// Escape invalid bytes as \xNN, and backslashes as \\ only in paths that
/// needed escaping, so ordinary paths are stored exactly.
fn putPath(a: std.mem.Allocator, c: *ColumnData, s: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(s)) return putStr(a, c, s);
    var i: usize = 0;
    while (i < s.len) {
        const n = std.unicode.utf8ByteSequenceLength(s[i]) catch 0;
        if (n > 1 and i + n <= s.len and std.unicode.utf8ValidateSlice(s[i .. i + n])) {
            try c.values.appendSlice(a, s[i .. i + n]);
            i += n;
        } else if (n == 1 and s[i] != '\\') {
            try c.values.append(a, s[i]);
            i += 1;
        } else if (s[i] == '\\') {
            try c.values.appendSlice(a, "\\\\");
            i += 1;
        } else {
            var hex: [4]u8 = undefined;
            _ = std.fmt.bufPrint(&hex, "\\x{x:0>2}", .{s[i]}) catch unreachable;
            try c.values.appendSlice(a, &hex);
            i += 1;
        }
    }
    try c.offsets.append(a, @intCast(c.values.items.len));
}

fn putStr(a: std.mem.Allocator, c: *ColumnData, s: []const u8) !void {
    try c.values.appendSlice(a, s);
    try c.offsets.append(a, @intCast(c.values.items.len));
}

/// Record validity for row `k` of a batch of `n`, allocating the bitmap
/// lazily on the first null.
fn setValid(a: std.mem.Allocator, c: *ColumnData, k: usize, n: usize, valid: bool) !void {
    if (valid and c.null_count == 0) return;
    if (c.validity.items.len == 0) {
        try c.validity.appendNTimes(a, 0, (n + 7) / 8);
        // Everything before k was valid.
        for (0..k) |j| c.validity.items[j / 8] |= @as(u8, 1) << @intCast(j % 8);
    }
    if (valid) {
        c.validity.items[k / 8] |= @as(u8, 1) << @intCast(k % 8);
    } else {
        c.null_count += 1;
    }
}

fn putOptInt(a: std.mem.Allocator, c: *ColumnData, k: usize, n: usize, v: ?u64) !void {
    try setValid(a, c, k, n, v != null);
    try putInt(a, c, i64, v orelse 0);
}

fn putOptStr(a: std.mem.Allocator, c: *ColumnData, k: usize, n: usize, v: ?[]const u8) !void {
    try setValid(a, c, k, n, v != null);
    try putStr(a, c, v orelse "");
}

pub const batch_rows = 64 * 1024;
