//! Legacy `wtfsdumpv1` snapshot (directories plus large files), read by the
//! Python TUI in `wtfs/`. New consumers should use the Arrow/Parquet table.
const std = @import("std");
const Scan = @import("Scan.zig");

pub fn write(a: std.mem.Allocator, w: *std.Io.Writer, scan: *const Scan, large_file_threshold: u64) !void {
    const n = scan.rows.len();
    // Row id -> directory index, for directories only.
    const dir_index = try a.alloc(u32, n);
    defer a.free(dir_index);
    var dir_ids: std.ArrayList(u32) = .empty;
    defer dir_ids.deinit(a);
    var large: std.ArrayList(u32) = .empty;
    defer large.deinit(a);
    for (0..n) |i| {
        const r = scan.rows.at(i);
        if (r.kind == .dir) {
            dir_index[i] = @intCast(dir_ids.items.len);
            try dir_ids.append(a, @intCast(i));
        } else if (r.kind == .file and r.alloc >= large_file_threshold) {
            try large.append(a, @intCast(i));
        }
    }

    const nd = dir_ids.items.len;
    const files = try a.alloc(u64, nd);
    defer a.free(files);
    const dirs = try a.alloc(u64, nd);
    defer a.free(dirs);
    @memset(files, 0);
    @memset(dirs, 1);
    for (0..n) |i| {
        const r = scan.rows.at(i);
        if (r.kind == .file and r.parent != Scan.no_parent) files[dir_index[r.parent]] += 1;
    }
    // Children come after parents, so one reverse pass sums subtrees.
    var d = nd;
    while (d > 1) {
        d -= 1;
        const p = dir_index[scan.rows.at(dir_ids.items[d]).parent];
        files[p] += files[d];
        dirs[p] += dirs[d];
    }

    try w.writeAll("wtfsdumpv1     \n");
    try w.writeInt(u64, nd, .little);
    try w.writeInt(u64, files[0], .little);
    try w.writeInt(u64, scan.rows.at(0).tree_du, .little);

    var names_len: u64 = 0;
    for (dir_ids.items, 0..) |id, k| names_len += dirName(scan, id, k).len + 1;
    for (large.items) |id| names_len += scan.rows.at(id).str.len + 1;
    try w.writeInt(u64, names_len, .little);
    for (dir_ids.items, 0..) |id, k| {
        try w.writeAll(dirName(scan, id, k));
        try w.writeByte(0);
    }
    for (large.items) |id| {
        try w.writeAll(scan.rows.at(id).str);
        try w.writeByte(0);
    }

    try w.writeInt(u64, nd, .little);
    for (dir_ids.items, 0..) |id, k| {
        const r = scan.rows.at(id);
        try w.writeInt(u32, if (k == 0) 0 else dir_index[r.parent], .little);
    }
    for (0..nd) |k| try w.writeInt(u32, @intCast(k), .little);
    for (dir_ids.items) |id| try w.writeInt(u64, scan.rows.at(id).tree_du, .little);
    for (files) |f| try w.writeInt(u64, f, .little);
    for (dirs) |x| try w.writeInt(u64, x, .little);

    try w.writeInt(u64, large.items.len, .little);
    for (large.items) |id| try w.writeInt(u64, dir_index[scan.rows.at(id).parent], .little);
    for (large.items, 0..) |_, k| try w.writeInt(u32, @intCast(nd + k), .little);
    for (large.items) |id| try w.writeInt(u64, scan.rows.at(id).alloc, .little);
    try w.flush();
}

fn dirName(scan: *const Scan, id: u32, k: usize) []const u8 {
    if (k == 0) return ".";
    const path = scan.rows.at(id).str;
    return path[(std.mem.lastIndexOfScalar(u8, path, '/') orelse return path) + 1 ..];
}
