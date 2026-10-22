//! Plain-text summary. Every data line is
//!   BYTES  HUMAN  [ENTRIES]  PATH
//! with the full, untruncated path last; lines starting with '#' are headers.
const std = @import("std");
const Scan = @import("Scan.zig");
const table = @import("table.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;

pub fn formatBytes(buf: []u8, bytes: u64) []const u8 {
    const units = [_][]const u8{ "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
    if (bytes < 1024) return std.fmt.bufPrint(buf, "{d}B", .{bytes}) catch unreachable;
    var v: f64 = @floatFromInt(bytes);
    var u: usize = 0;
    while (v >= 1024 and u + 1 < units.len) : (u += 1) v /= 1024;
    return std.fmt.bufPrint(buf, "{d:.1}{s}", .{ v, units[u] }) catch unreachable;
}

/// Write a path, escaping control characters and invalid UTF-8 as \xNN so
/// one entry is always one line.
pub fn writePath(w: *Writer, path: []const u8) !void {
    var i: usize = 0;
    while (i < path.len) {
        const c = path[i];
        const n = std.unicode.utf8ByteSequenceLength(c) catch 0;
        if (n > 1 and i + n <= path.len and std.unicode.utf8ValidateSlice(path[i .. i + n])) {
            try w.writeAll(path[i .. i + n]);
            i += n;
        } else if (n == 1 and c >= 0x20 and c != 0x7f and c != '\\') {
            try w.writeByte(c);
            i += 1;
        } else if (c == '\\') {
            try w.writeAll("\\\\");
            i += 1;
        } else {
            try w.print("\\x{x:0>2}", .{c});
            i += 1;
        }
    }
}

/// Keeps the `n` largest items by key.
fn TopN(comptime Item: type) type {
    return struct {
        const Self = @This();
        const Entry = struct { key: u64, item: Item };
        items: std.ArrayList(Entry) = .empty,
        n: usize,

        fn add(self: *Self, a: Allocator, key: u64, item: Item) !void {
            if (self.n == 0) return;
            if (self.items.items.len == self.n) {
                if (key <= self.items.items[self.items.items.len - 1].key) return;
                _ = self.items.pop();
            }
            var pos = self.items.items.len;
            while (pos > 0 and self.items.items[pos - 1].key < key) pos -= 1;
            try self.items.insert(a, pos, .{ .key = key, .item = item });
        }
    };
}

pub const Options = struct {
    top: usize = 20,
    output_path: ?[]const u8 = null,
};

pub fn print(a: Allocator, w: *Writer, scan: *const Scan, deleted: []const table.Deleted, opts: Options) !void {
    const n = scan.rows.len();
    const root = scan.rows.at(0);
    var counts: [@typeInfo(Scan.Kind).@"enum".field_names.len]u64 = @splat(0);
    var apparent: u64 = 0;
    var alloc_all: u64 = 0;
    var errors: u64 = 0;
    var skipped: u64 = 0;
    var root_children: u64 = 0;

    var children: TopN(u32) = .{ .n = opts.top };
    var dirs: TopN(u32) = .{ .n = opts.top };
    var files: TopN(u32) = .{ .n = opts.top };
    var errs: std.ArrayList(u32) = .empty;

    for (0..n) |i| {
        const r = scan.rows.at(i);
        counts[@backingInt(r.kind)] += 1;
        if (!r.dup) apparent += r.size;
        alloc_all += r.alloc;
        if (r.err == .other_fs or r.err == .pseudo_fs) skipped += 1;
        if (r.err != .none and r.err != .other_fs and r.err != .pseudo_fs) {
            errors += 1;
            if (errs.items.len < opts.top) try errs.append(a, @intCast(i));
        }
        if (i == 0) continue;
        if (r.parent == 0) {
            root_children += 1;
            try children.add(a, r.tree_du, @intCast(i));
        }
        if (r.kind == .dir) {
            if (r.tree_du > 0) try dirs.add(a, r.tree_du, @intCast(i));
        } else if (r.du() > 0) try files.add(a, r.du(), @intCast(i));
    }

    var hb: [32]u8 = undefined;
    var path: std.ArrayList(u8) = .empty;

    try w.writeAll("# wtfs ");
    try writePath(w, root.str);
    try w.writeByte('\n');
    try w.print("# du {d} bytes ({s})", .{ root.tree_du, formatBytes(&hb, root.tree_du) });
    try w.print("; apparent size {d} bytes ({s})", .{ apparent, formatBytes(&hb, apparent) });
    try w.print("; hardlinked copies not counted {d} bytes ({s})\n", .{ alloc_all - root.tree_du, formatBytes(&hb, alloc_all - root.tree_du) });
    try w.print("# {d} entries: {d} files, {d} dirs, {d} symlinks, {d} other; {d} errors; {d} mount points not scanned; {d:.2}s\n", .{
        n,
        counts[@backingInt(Scan.Kind.file)],
        counts[@backingInt(Scan.Kind.dir)],
        counts[@backingInt(Scan.Kind.symlink)],
        n - counts[@backingInt(Scan.Kind.file)] - counts[@backingInt(Scan.Kind.dir)] - counts[@backingInt(Scan.Kind.symlink)],
        errors,
        skipped,
        @as(f64, @floatFromInt(scan.elapsed_ns)) / std.time.ns_per_s,
    });
    if (opts.top == 0) return;

    const Section = struct {
        fn rows(sw: *Writer, sa: Allocator, s: *const Scan, top: anytype, with_count: bool, p: *std.ArrayList(u8)) !void {
            var b: [32]u8 = undefined;
            if (with_count) {
                try sw.writeAll("#          du_bytes   du_human     entries  path\n");
            } else {
                try sw.writeAll("#          du_bytes   du_human  path\n");
            }
            for (top.items.items) |e| {
                const r = s.rows.at(e.item);
                p.clearRetainingCapacity();
                try s.writePath(e.item, p, sa);
                try sw.print("{d:>19}  {s:>9}  ", .{ e.key, formatBytes(&b, e.key) });
                if (with_count) try sw.print("{d:>10}  ", .{r.tree_count});
                try writePath(sw, p.items);
                try sw.writeByte('\n');
            }
        }
    };

    try w.print("\n## children of ", .{});
    try writePath(w, root.str);
    try w.print(" by du (top {d} of {d})\n", .{ children.items.items.len, root_children });
    try Section.rows(w, a, scan, &children, true, &path);

    try w.print("\n## largest directories by du, at any depth (top {d})\n", .{dirs.items.items.len});
    try Section.rows(w, a, scan, &dirs, true, &path);

    try w.print("\n## largest files by du (top {d})\n", .{files.items.items.len});
    try Section.rows(w, a, scan, &files, false, &path);

    if (errors > 0) {
        try w.print("\n## errors (first {d} of {d})\n", .{ errs.items.len, errors });
        for (errs.items) |i| {
            path.clearRetainingCapacity();
            try scan.writePath(i, &path, a);
            try w.print("{s}: ", .{scan.rows.at(i).err.describe().?});
            try writePath(w, path.items);
            try w.writeByte('\n');
        }
    }

    if (deleted.len > 0) {
        var total: u64 = 0;
        for (deleted) |d| total += d.row.alloc;
        try w.print("\n## deleted files still held open: {d}, {d} bytes ({s})\n", .{ deleted.len, total, formatBytes(&hb, total) });
        try w.writeAll("#          du_bytes   du_human  path  <-  open by\n");
        for (deleted) |d| {
            try w.print("{d:>19}  {s:>9}  ", .{ d.row.alloc, formatBytes(&hb, d.row.alloc) });
            try writePath(w, d.path);
            try w.writeAll("  <-  ");
            try w.writeAll(d.open_by);
            try w.writeByte('\n');
        }
    }

    if (opts.output_path == null) {
        try w.writeAll("\n# For every entry as a table: wtfs -o scan.parquet PATH, then query it with DuckDB.\n");
    }
}
