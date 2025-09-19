const std = @import("std");
const wtfs = @import("wtfs").mac;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

const Scanner = wtfs.DirScanner(.{
    .common = .{
        .name = true,
        .obj_type = true,
    },
    .dir = .{
        .datalength = true,
    },
    .file = .{
        .totalsize = true,
    },
});

const WorkItem = struct {
    path: []u8,
    depth: usize,
    parent: ?usize,
    total_size: u64 = 0,
    total_files: usize = 0,
    total_dirs: usize = 1,
    inaccessible: bool = false,
};

pub fn main() !void {
    const allocator = std.heap.c_allocator;

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const exe_name = if (args.len > 0)
        std.mem.sliceTo(args[0], 0)
    else
        "wtfs";

    var skip_hidden = false;
    var root_arg: ?[]const u8 = null;

    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        const arg = std.mem.sliceTo(args[arg_index], 0);
        if (std.mem.eql(u8, arg, "--skip-hidden")) {
            skip_hidden = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--help")) {
            try stdout.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        if (root_arg != null) {
            try stdout.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        root_arg = arg;
    }

    const root = root_arg orelse ".";

    var queue = std.ArrayList(WorkItem){};
    defer {
        for (queue.items) |item| {
            allocator.free(item.path);
        }
        queue.deinit(allocator);
    }

    try queue.append(allocator, .{
        .path = try allocator.dupe(u8, root),
        .depth = 0,
        .parent = null,
    });

    var cursor: usize = 0;
    var buf: [16 * 1024]u8 = undefined;
    var inaccessible_dirs: usize = 0;

    var progress_root = std.Progress.start(.{ .root_name = "Scanning" });
    defer progress_root.end();

    var progress_node = progress_root.start("directories", 0);
    defer progress_node.end();
    progress_node.setEstimatedTotalItems(queue.items.len);

    var processed_dirs: usize = 0;

    while (cursor < queue.items.len) : (cursor += 1) {
        const item_index = cursor;
        const item = queue.items[item_index];

        progress_node.setName(item.path);
        defer {
            processed_dirs += 1;
            progress_node.setCompletedItems(processed_dirs);
        }

        var dir_total: u64 = 0;
        var file_count: usize = 0;

        var dir = std.fs.cwd().openDir(item.path, .{ .iterate = true }) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                queue.items[item_index].inaccessible = true;
                queue.items[item_index].total_size = 0;
                queue.items[item_index].total_files = 0;
                inaccessible_dirs += 1;
                continue;
            },
            else => return err,
        };
        defer dir.close();

        var scanner = Scanner.init(dir.fd, &buf);

        while (try scanner.next()) |entry| {
            const name = std.mem.sliceTo(entry.name, 0);
            if (name.len == 0) continue;
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

            const child_path = try std.fs.path.join(allocator, &[_][]const u8{ item.path, name });

            switch (entry.kind) {
                .dir => {
                    if (skip_hidden and name[0] == '.') {
                        allocator.free(child_path);
                        continue;
                    }

                    errdefer allocator.free(child_path);
                    try queue.append(allocator, .{
                        .path = child_path,
                        .depth = item.depth + 1,
                        .parent = item_index,
                    });
                    progress_node.increaseEstimatedTotalItems(1);
                },
                .file => {
                    const file = entry.details.file;
                    defer allocator.free(child_path);

                    dir_total += file.totalsize;
                    file_count += 1;
                },
                .symlink, .other => {
                    defer allocator.free(child_path);
                },
            }
        }

        queue.items[item_index].total_size = dir_total;
        queue.items[item_index].total_files = file_count;
        queue.items[item_index].total_dirs = 1;
    }

    var idx = queue.items.len;
    while (idx > 0) {
        idx -= 1;
        const child = queue.items[idx];
        if (child.parent) |parent_index| {
            queue.items[parent_index].total_size += child.total_size;
            queue.items[parent_index].total_files += child.total_files;
            queue.items[parent_index].total_dirs += child.total_dirs;
        }
    }

    const root_item = queue.items[0];
    const directories_including_root = root_item.total_dirs;
    const files_total = root_item.total_files;
    const bytes_total = root_item.total_size;

    var top_level = std.ArrayList(usize){};
    defer top_level.deinit(allocator);
    for (queue.items, 0..) |entry, idx2| {
        if (entry.depth == 1) {
            try top_level.append(allocator, idx2);
        }
    }

    const SortContext = struct {
        items: []const WorkItem,
        pub fn lessThan(ctx: @This(), lhs: usize, rhs: usize) bool {
            const a = ctx.items[lhs];
            const b = ctx.items[rhs];
            if (a.total_size == b.total_size) {
                return std.mem.lessThan(u8, a.path, b.path);
            }
            return a.total_size > b.total_size;
        }
    };

    if (top_level.items.len > 1) {
        std.sort.heap(usize, top_level.items, SortContext{ .items = queue.items }, SortContext.lessThan);
    }

    try stdout.print("Summary for {s}:\n", .{root});
    try stdout.print("  Directories (incl. root): {d}\n", .{directories_including_root});
    try stdout.print("  Files: {d}\n", .{files_total});
    try stdout.print("  Total size: {Bi}\n", .{bytes_total});
    if (inaccessible_dirs > 0) {
        try stdout.print("  Inaccessible directories: {d}\n", .{inaccessible_dirs});
    }
    if (skip_hidden) {
        try stdout.print("  Hidden entries skipped: yes\n", .{});
    }

    if (top_level.items.len > 0) {
        try stdout.print("\nTop-level directories by total size:\n", .{});
        const max_show = @min(top_level.items.len, 10);
        var i: usize = 0;
        while (i < max_show) : (i += 1) {
            const entry = queue.items[top_level.items[i]];
            const marker = if (entry.inaccessible) " (inaccessible)" else "";
            try stdout.print(
                "  {d:>2}. {Bi:>10}  {d:>7} files  {s}{s}\n",
                .{ i + 1, entry.total_size, entry.total_files, entry.path, marker },
            );
        }
    }

    try stdout.flush();
}
