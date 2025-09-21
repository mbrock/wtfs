const std = @import("std");
const Context = @import("Context.zig");
const DiskScan = @import("DiskScan.zig");
const SummaryEntry = DiskScan.SummaryEntry;
const FileSummaryEntry = DiskScan.FileSummaryEntry;
const DirectoryTotals = DiskScan.DirectoryTotals;
const ScanResults = DiskScan.ScanResults;
const Writer = std.io.Writer;
const Allocator = std.mem.Allocator;

pub fn printHeader(stdout: *Writer, root: []const u8, results: ScanResults) !void {
    try stdout.print("{s}: {d} dirs, {d} files, {Bi: <.1} total\n\n", .{
        root,
        results.totals.directories,
        results.totals.files,
        results.totals.bytes,
    });
}

const Self = DiskScan;

pub fn printTopLevelDirectories(
    directories: *std.MultiArrayList(Context.DirectoryNode),
    stdout: *Writer,
    totals: DirectoryTotals,
    entries: []const SummaryEntry,
) !void {
    if (entries.len == 0) return;

    try stdout.print(
        "Top-level directories by total size:\n\n",
        .{},
    );
    try stdout.print(
        "  Size         Share   Files      Dirs\n",
        .{},
    );

    const limit = @min(entries.len, 10);
    for (entries[0..limit]) |entry| {
        try printTopLevelEntry(directories, stdout, totals, entry);
    }
}

pub fn printHeaviestDirectories(
    allocator: Allocator,
    stdout: *Writer,
    directories: *std.MultiArrayList(Context.DirectoryNode),
    namedata: *std.ArrayList(u8),
    totals: DirectoryTotals,
    entries: []const SummaryEntry,
) !void {
    if (entries.len == 0) return;

    try stdout.print("Heaviest directories in tree:\n\n", .{});
    try stdout.print("  Size         Share   Files      Dirs\n", .{});

    const ChildMap = std.AutoHashMap(usize, std.ArrayList(usize));
    var child_map = ChildMap.init(allocator);
    defer {
        var iter = child_map.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit(allocator);
        }
        child_map.deinit();
    }

    var node_set = std.AutoHashMap(usize, void).init(allocator);
    defer node_set.deinit();

    const slices = directories.slice();

    for (entries) |entry| {
        var current = entry.index;
        while (true) {
            try node_set.put(current, {});
            if (current == 0) break;

            const parent_index: usize = @intCast(slices.items(.parent)[current]);
            try node_set.put(parent_index, {});

            const gop = try child_map.getOrPut(parent_index);
            if (!gop.found_existing) {
                gop.value_ptr.* = std.ArrayList(usize){};
            }
            const children_ptr = gop.value_ptr;
            if (std.mem.indexOfScalar(usize, children_ptr.items, current) == null) {
                try children_ptr.append(allocator, current);
            }

            if (parent_index == 0) {
                current = 0;
                break;
            }
            current = parent_index;
        }
    }

    try node_set.put(0, {});

    var sort_iter = child_map.iterator();
    while (sort_iter.next()) |entry| {
        const children_ptr = entry.value_ptr;
        const SortCtx = struct {
            directories: *std.MultiArrayList(Context.DirectoryNode),

            fn bySize(ctx: @This(), lhs: usize, rhs: usize) bool {
                const dir_slices = ctx.directories.slice();
                const sizes = dir_slices.items(.total_size);
                const lhs_size = sizes[lhs];
                const rhs_size = sizes[rhs];
                if (lhs_size != rhs_size) return lhs_size > rhs_size;
                return lhs < rhs;
            }
        };

        if (children_ptr.items.len > 1) {
            std.sort.heap(
                usize,
                children_ptr.items,
                SortCtx{ .directories = directories },
                SortCtx.bySize,
            );
        }
    }

    try printHeaviestNode(stdout, directories, namedata, &child_map, &node_set, totals, 0, 0, true);
    try stdout.print("\n", .{});
}

fn printHeaviestNode(
    stdout: *Writer,
    directories: *std.MultiArrayList(Context.DirectoryNode),
    namedata: *std.ArrayList(u8),
    child_map: *std.AutoHashMap(usize, std.ArrayList(usize)),
    node_set: *std.AutoHashMap(usize, void),
    totals: DirectoryTotals,
    index: usize,
    depth: usize,
    include_self: bool,
) !void {
    if (!node_set.contains(index)) return;

    if (include_self) {
        try printHeaviestLine(stdout, directories, namedata, totals, index, depth);
    }

    if (child_map.getPtr(index)) |children| {
        for (children.items) |child_index| {
            try printHeaviestNode(stdout, directories, namedata, child_map, node_set, totals, child_index, depth + 1, true);
        }
    }
}

fn printHeaviestLine(
    stdout: *Writer,
    directories: *std.MultiArrayList(Context.DirectoryNode),
    namedata: *std.ArrayList(u8),
    totals: DirectoryTotals,
    index: usize,
    depth: usize,
) !void {
    try stdout.flush();

    const slices = directories.slice();

    const size = slices.items(.total_size)[index];

    var size_buf: [32]u8 = undefined;
    const size_str = try formatBytes(size_buf[0..], size);

    var share_buf: [16]u8 = undefined;
    const share_str = try formatPercent(share_buf[0..], size, totals.bytes);

    var indent_buf: [64]u8 = undefined;
    const indent_len = @min(depth * 2, indent_buf.len);
    for (indent_buf[0..indent_len]) |*ch| ch.* = ' ';

    const name = if (index == 0) "." else DiskScan.directoryName(directories, namedata, index);

    try stdout.print("{s: <32} ", .{name});
    try stdout.print(
        "{s: <8}  {s: <8}\n",
        .{
            size_str,
            share_str,
        },
    );
}

fn printTopLevelEntry(
    directories: *std.MultiArrayList(Context.DirectoryNode),
    stdout: *Writer,
    totals: DirectoryTotals,
    entry: SummaryEntry,
) !void {
    const slices = directories.slice();

    const size = slices.items(.total_size)[entry.index];
    const files = slices.items(.total_files)[entry.index];
    const dirs_total = slices.items(.total_dirs)[entry.index];
    const dir_count = if (dirs_total == 0) 0 else dirs_total - 1;

    var size_buf: [32]u8 = undefined;
    const size_str = try formatBytes(size_buf[0..], size);

    var share_buf: [16]u8 = undefined;
    const share_str = try formatPercent(share_buf[0..], size, totals.bytes);

    var files_buf: [32]u8 = undefined;
    const files_str = try formatCount(files_buf[0..], files);

    var dirs_buf: [32]u8 = undefined;
    const dirs_str = try formatCount(dirs_buf[0..], dir_count);

    try stdout.print(
        "{s} {s:>11}  {s:>6}  {s:>9} files  {s:>8} dirs\n",
        .{ entry.path, size_str, share_str, files_str, dirs_str },
    );
}

pub fn printLargeFiles(
    stdout: *Writer,
    totals: DirectoryTotals,
    entries: []const FileSummaryEntry,
    threshold: u64,
) !void {
    if (entries.len == 0) return;

    var threshold_buf: [32]u8 = undefined;
    const threshold_str = try formatBytes(threshold_buf[0..], threshold);

    try stdout.print("Largest files (>= {s})\n\n", .{threshold_str});

    for (entries) |entry| {
        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], entry.size);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], entry.size, totals.bytes);

        try stdout.print("{s:>11}  {s:>6}  ", .{ size_str, share_str });
        try stdout.print(
            "{s:<52}\n",
            .{entry.path[if (entry.path.len > 52) entry.path.len - 52 else 0..]},
        );
    }
}

fn formatPercent(buf: []u8, value: u64, total: u64) ![]const u8 {
    if (total == 0) {
        return try std.fmt.bufPrint(buf, "0.0%", .{});
    }

    const percent = @min(
        100.0,
        (@as(f64, @floatFromInt(value)) / @as(f64, @floatFromInt(total))) * 100.0,
    );
    return try std.fmt.bufPrint(buf, "{d:>5.1}%", .{percent});
}

fn formatBytes(buf: []u8, bytes: u64) ![]const u8 {
    const Unit = struct {
        threshold: u64,
        suffix: []const u8,
    };

    const units = [_]Unit{
        .{ .threshold = 1024 * 1024 * 1024 * 1024, .suffix = "TiB" },
        .{ .threshold = 1024 * 1024 * 1024, .suffix = "GiB" },
        .{ .threshold = 1024 * 1024, .suffix = "MiB" },
        .{ .threshold = 1024, .suffix = "KiB" },
    };

    for (units) |unit| {
        if (bytes >= unit.threshold) {
            const value = @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(unit.threshold));
            return try std.fmt.bufPrint(buf, "{d:>7.1} {s}", .{ value, unit.suffix });
        }
    }

    return try std.fmt.bufPrint(buf, "{d} B", .{bytes});
}

fn formatCount(buf: []u8, value: usize) ![]const u8 {
    const cast_value = std.math.cast(u64, value) orelse std.math.maxInt(u64);
    const Unit = struct {
        threshold: u64,
        suffix: []const u8,
    };

    const units = [_]Unit{
        .{ .threshold = 1_000_000_000_000, .suffix = "T" },
        .{ .threshold = 1_000_000_000, .suffix = "B" },
        .{ .threshold = 1_000_000, .suffix = "M" },
        .{ .threshold = 1_000, .suffix = "K" },
    };

    for (units) |unit| {
        if (cast_value >= unit.threshold) {
            const scaled = @as(f64, @floatFromInt(cast_value)) / @as(f64, @floatFromInt(unit.threshold));
            const decimals: u8 = if (scaled >= 100.0) 0 else if (scaled >= 10.0) 1 else 2;
            return switch (decimals) {
                0 => try std.fmt.bufPrint(buf, "{d:.0}{s}", .{ scaled, unit.suffix }),
                1 => try std.fmt.bufPrint(buf, "{d:.1}{s}", .{ scaled, unit.suffix }),
                else => try std.fmt.bufPrint(buf, "{d:.2}{s}", .{ scaled, unit.suffix }),
            };
        }
    }

    return try std.fmt.bufPrint(buf, "{d}", .{cast_value});
}
