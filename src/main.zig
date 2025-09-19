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
    direct_size: u64 = 0,
    total_size: u64 = 0,
    inaccessible: bool = false,
};

fn printIndent(depth: usize) !void {
    var i: usize = 0;
    while (i < depth) : (i += 1) {
        try stdout.print("  ", .{});
    }
}

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

    while (cursor < queue.items.len) : (cursor += 1) {
        const item_index = cursor;
        const item = queue.items[item_index];

        var dir_total: u64 = 0;

        try stdout.print("{s}:\n", .{item.path});

        var dir = std.fs.cwd().openDir(item.path, .{ .iterate = true }) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                queue.items[item_index].inaccessible = true;
                try printIndent(item.depth + 1);
                try stdout.print("[inaccessible]\n\n", .{});
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

                    try printIndent(item.depth + 1);
                    try stdout.print("[dir] {s}\n", .{child_path});

                    errdefer allocator.free(child_path);
                    try queue.append(allocator, .{
                        .path = child_path,
                        .depth = item.depth + 1,
                        .parent = item_index,
                    });
                },
                .file => {
                    const file = entry.details.file;
                    defer allocator.free(child_path);

                    try printIndent(item.depth + 1);
                    try stdout.print("{Bi:>10}  {s}\n", .{ file.totalsize, child_path });
                    dir_total += file.totalsize;
                },
                .symlink => {
                    defer allocator.free(child_path);

                    try printIndent(item.depth + 1);
                    try stdout.print("<sym>      {s}\n", .{child_path});
                },
                .other => {
                    defer allocator.free(child_path);

                    try printIndent(item.depth + 1);
                    try stdout.print("<other>    {s}\n", .{child_path});
                },
            }
        }

        queue.items[item_index].direct_size = dir_total;
        queue.items[item_index].total_size = dir_total;

        if (dir_total > 0) {
            try printIndent(item.depth + 1);
            try stdout.print("[direct total] {Bi}\n", .{dir_total});
        }

        try stdout.print("\n", .{});
    }

    var idx = queue.items.len;
    while (idx > 0) {
        idx -= 1;
        const child = queue.items[idx];
        if (child.parent) |parent_index| {
            queue.items[parent_index].total_size += child.total_size;
        }
    }

    try stdout.print("Totals:\n", .{});
    for (queue.items) |item| {
        try printIndent(item.depth);
        const marker = if (item.inaccessible) " (inaccessible)" else "";
        try stdout.print("{Bi:>10}  {s}{s}\n", .{ item.total_size, item.path, marker });
    }

    try stdout.flush();
}
