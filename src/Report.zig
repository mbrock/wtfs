const std = @import("std");
const strpool = @import("wtfs").strpool;

const Context = @import("Context.zig");
const DiskScan = @import("DiskScan.zig");
const Tabular = @import("TabWriter.zig");

const SummaryEntry = DiskScan.SummaryEntry;
const FileSummaryEntry = DiskScan.FileSummaryEntry;
const DirectoryTotals = DiskScan.DirectoryTotals;
const ScanResults = DiskScan.ScanResults;
const Writer = std.io.Writer;
const Allocator = std.mem.Allocator;

const TabWriter = Tabular.TabWriter;
const Column = Tabular.Column;

pub fn printHeader(stdout: *Writer, root: []const u8, results: ScanResults) !void {
    try stdout.print("{s}: {d} dirs, {d} files, {Bi:.1} total\n\n", .{
        root,
        results.totals.directories,
        results.totals.files,
        results.totals.bytes,
    });
}

pub const ReportData = struct {
    directories: *const std.MultiArrayList(Context.DirectoryNode),
    namedata: *const std.ArrayList(u8),
    idxset: *const strpool.IndexSet,
    large_files: *const std.MultiArrayList(Context.LargeFile),
    totals: DirectoryTotals,

    pub fn init(
        directories: *const std.MultiArrayList(Context.DirectoryNode),
        namedata: *const std.ArrayList(u8),
        idxset: *const strpool.IndexSet,
        large_files: *const std.MultiArrayList(Context.LargeFile),
        totals: DirectoryTotals,
    ) ReportData {
        return .{
            .directories = directories,
            .namedata = namedata,
            .idxset = idxset,
            .large_files = large_files,
            .totals = totals,
        };
    }
};

const TopLevelRow = struct {
    path: []const u8,
    size: u64,
    files: usize,
    dirs: usize,
};

pub const TopLevelSummary = struct {
    rows: std.ArrayList(TopLevelRow),

    pub fn deinit(self: *TopLevelSummary, allocator: Allocator) void {
        self.rows.deinit(allocator);
    }
};

pub fn buildTopLevelSummary(
    allocator: Allocator,
    data: ReportData,
    entries: []const SummaryEntry,
    limit: usize,
) !TopLevelSummary {
    var summary = TopLevelSummary{ .rows = std.ArrayList(TopLevelRow){} };
    errdefer summary.rows.deinit(allocator);

    if (entries.len == 0) return summary;

    const count = @min(entries.len, limit);
    try summary.rows.ensureTotalCapacityPrecise(allocator, count);

    const slices = data.directories.slice();
    const total_sizes = slices.items(.total_size);
    const total_files = slices.items(.total_files);
    const total_dirs = slices.items(.total_dirs);

    for (entries[0..count]) |entry| {
        const index = entry.index;
        const dir_total = total_dirs[index];
        const dir_count = if (dir_total == 0) 0 else dir_total - 1;

        summary.rows.appendAssumeCapacity(.{
            .path = entry.path,
            .size = total_sizes[index],
            .files = total_files[index],
            .dirs = dir_count,
        });
    }

    return summary;
}

pub fn printTopLevelDirectories(stdout: *Writer, data: ReportData, summary: TopLevelSummary) !void {
    if (summary.rows.items.len == 0) return;

    try stdout.print("Top-level directories by total size:\n\n", .{});

    var table = TabWriter.init(stdout, &[_]Column{
        Column{ .width = 32 },
        Column{ .width = 11, .alignment = .right },
        Column{ .width = 7, .alignment = .right },
        Column{ .width = 12, .alignment = .right },
        Column{ .width = 11, .alignment = .right },
    });

    try table.writeHeader(&[_][]const u8{ "Directory", "Size", "Share", "Files", "Dirs" });
    try table.writeSeparator('-');

    for (summary.rows.items) |row| {
        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], row.size);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], row.size, data.totals.bytes);

        var files_buf: [32]u8 = undefined;
        const files_str = try formatCount(files_buf[0..], row.files);

        var dirs_buf: [32]u8 = undefined;
        const dirs_str = try formatCount(dirs_buf[0..], row.dirs);

        try table.writeRow(&[_][]const u8{
            row.path,
            size_str,
            share_str,
            files_str,
            dirs_str,
        });
    }

    try stdout.writeByte('\n');
}

const HeaviestRow = struct {
    index: usize,
    depth: usize,
};

pub const HeaviestSummary = struct {
    rows: std.ArrayList(HeaviestRow),

    pub fn deinit(self: *HeaviestSummary, allocator: Allocator) void {
        self.rows.deinit(allocator);
    }
};

pub fn buildHeaviestSummary(
    allocator: Allocator,
    data: ReportData,
    entries: []const SummaryEntry,
) !HeaviestSummary {
    var summary = HeaviestSummary{ .rows = std.ArrayList(HeaviestRow){} };
    errdefer summary.rows.deinit(allocator);

    if (entries.len == 0) return summary;

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

    const slices = data.directories.slice();

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

    const SortCtx = struct {
        directories: *const std.MultiArrayList(Context.DirectoryNode),

        fn bySize(ctx: @This(), lhs: usize, rhs: usize) bool {
            const dir_slices = ctx.directories.slice();
            const sizes = dir_slices.items(.total_size);
            const lhs_size = sizes[lhs];
            const rhs_size = sizes[rhs];
            if (lhs_size != rhs_size) return lhs_size > rhs_size;
            return lhs < rhs;
        }
    };

    var sort_iter = child_map.iterator();
    while (sort_iter.next()) |entry| {
        const children_ptr = entry.value_ptr;
        if (children_ptr.items.len > 1) {
            std.sort.heap(
                usize,
                children_ptr.items,
                SortCtx{ .directories = data.directories },
                SortCtx.bySize,
            );
        }
    }

    try appendHeaviest(&summary.rows, allocator, &child_map, &node_set, 0, 0, true);

    return summary;
}

pub fn printHeaviestDirectories(stdout: *Writer, data: ReportData, summary: HeaviestSummary) !void {
    if (summary.rows.items.len == 0) return;

    try stdout.print("Heaviest directories in tree:\n\n", .{});

    var table = TabWriter.init(stdout, &[_]Column{
        Column{ .width = 32 },
        Column{ .width = 11, .alignment = .right },
        Column{ .width = 7, .alignment = .right },
    });

    try table.writeHeader(&[_][]const u8{ "Directory", "Size", "Share" });
    try table.writeSeparator('-');

    const slices = data.directories.slice();
    const sizes = slices.items(.total_size);

    for (summary.rows.items) |row| {
        var name_buf: [128]u8 = undefined;
        const label = formatDirectoryLabel(name_buf[0..], data, row.index, row.depth);

        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], sizes[row.index]);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], sizes[row.index], data.totals.bytes);

        try table.writeRow(&[_][]const u8{ label, size_str, share_str });
    }

    try stdout.writeByte('\n');
}

pub fn printLargeFiles(
    stdout: *Writer,
    data: ReportData,
    entries: []const FileSummaryEntry,
    threshold: u64,
) !void {
    if (entries.len == 0) return;

    var threshold_buf: [32]u8 = undefined;
    const threshold_str = try formatBytes(threshold_buf[0..], threshold);

    try stdout.print("Largest files (>= {s})\n\n", .{threshold_str});

    var table = TabWriter.init(stdout, &[_]Column{
        Column{ .width = 11, .alignment = .right },
        Column{ .width = 7, .alignment = .right },
        Column{ .width = 0 },
    });

    try table.writeHeader(&[_][]const u8{ "Size", "Share", "Path" });
    try table.writeSeparator('-');

    for (entries) |entry| {
        var size_buf: [32]u8 = undefined;
        const size_str = try formatBytes(size_buf[0..], entry.size);

        var share_buf: [16]u8 = undefined;
        const share_str = try formatPercent(share_buf[0..], entry.size, data.totals.bytes);

        const path = entry.path;
        const slice_start = if (path.len > 52) path.len - 52 else 0;
        const path_slice = path[slice_start..];

        try table.writeRow(&[_][]const u8{ size_str, share_str, path_slice });
    }

    try stdout.writeByte('\n');
}

fn appendHeaviest(
    rows: *std.ArrayList(HeaviestRow),
    allocator: Allocator,
    child_map: *std.AutoHashMap(usize, std.ArrayList(usize)),
    node_set: *std.AutoHashMap(usize, void),
    index: usize,
    depth: usize,
    include_self: bool,
) !void {
    if (!node_set.contains(index)) return;

    if (include_self) {
        try rows.append(allocator, .{ .index = index, .depth = depth });
    }

    if (child_map.getPtr(index)) |children| {
        for (children.items) |child_index| {
            try appendHeaviest(rows, allocator, child_map, node_set, child_index, depth + 1, true);
        }
    }
}

fn formatDirectoryLabel(
    buffer: []u8,
    data: ReportData,
    index: usize,
    depth: usize,
) []const u8 {
    if (buffer.len == 0) return "";

    const indent_cap = if (buffer.len <= 1) 0 else buffer.len - 1;
    const requested_indent = depth * 2;
    const indent_len = if (requested_indent > indent_cap) indent_cap else requested_indent;

    if (indent_len != 0) {
        var i: usize = 0;
        while (i < indent_len) : (i += 1) {
            buffer[i] = ' ';
        }
    }

    const raw_name = if (index == 0) "." else DiskScan.directoryName(data.directories, data.namedata, index);
    const available = buffer.len - indent_len;
    if (available == 0) return buffer[0..indent_len];

    const slice_start = if (raw_name.len > available) raw_name.len - available else 0;
    const name_slice = raw_name[slice_start..];
    @memcpy(buffer[indent_len .. indent_len + name_slice.len], name_slice);

    return buffer[0 .. indent_len + name_slice.len];
}

fn formatPercent(buf: []u8, value: u64, total: u64) ![]const u8 {
    if (total == 0) {
        return try std.fmt.bufPrint(buf, "0.0%", .{});
    }

    const percent = @min(
        100.0,
        (@as(f64, @floatFromInt(value)) / @as(f64, @floatFromInt(total))) * 100.0,
    );
    return try std.fmt.bufPrint(buf, "{d:.1}%", .{percent});
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
            return try std.fmt.bufPrint(buf, "{d:.1} {s}", .{ value, unit.suffix });
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
