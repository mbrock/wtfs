const std = @import("std");
const DiskScan = @import("DiskScan.zig");
const ascii = std.ascii;

var stderr_buffer: [4096]u8 = undefined;
var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
const stderr = &stderr_writer.interface;
pub fn main() !void {
    const gpa = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const exe_name = if (args.len > 0)
        std.mem.sliceTo(args[0], 0)
    else
        "wtfs";

    var skip_hidden = false;
    var root_arg: ?[]const u8 = null;
    var large_file_threshold = DiskScan.default_large_file_threshold;
    var binary_output: ?[]const u8 = null;

    const threshold_prefix = "--large-file-threshold=";
    const binary_prefix = "--binary-output=";

    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        const arg = std.mem.sliceTo(args[arg_index], 0);
        if (std.mem.eql(u8, arg, "--skip-hidden")) {
            skip_hidden = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--binary-output")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                try printUsage(exe_name);
                return;
            }
            binary_output = std.mem.sliceTo(args[arg_index], 0);
            continue;
        }
        if (std.mem.eql(u8, arg, "--large-file-threshold")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                try printUsage(exe_name);
                return;
            }
            const value_arg = std.mem.sliceTo(args[arg_index], 0);
            large_file_threshold = parseSize(value_arg) catch {
                try stderr.print("invalid size for --large-file-threshold: {s}\n", .{value_arg});
                try printUsage(exe_name);
                return;
            };
            continue;
        }
        if (std.mem.startsWith(u8, arg, binary_prefix)) {
            binary_output = arg[binary_prefix.len..];
            continue;
        }
        if (std.mem.startsWith(u8, arg, threshold_prefix)) {
            const value_arg = arg[threshold_prefix.len..];
            large_file_threshold = parseSize(value_arg) catch {
                try stderr.print("invalid size for --large-file-threshold: {s}\n", .{value_arg});
                try printUsage(exe_name);
                return;
            };
            continue;
        }
        if (std.mem.eql(u8, arg, "--help")) {
            try printUsage(exe_name);
            return;
        }
        if (root_arg != null) {
            try printUsage(exe_name);
            return;
        }
        root_arg = arg;
    }

    const root = root_arg orelse ".";

    var diskScan = DiskScan{
        .allocator = allocator,
        .skip_hidden = skip_hidden,
        .root = root,
        .large_file_threshold = large_file_threshold,
    };

    if (binary_output) |path| {
        var binary_file = try std.fs.cwd().createFile(path, .{ .truncate = true, .read = false });
        defer binary_file.close();

        var binary_buffer: [16 * 1024]u8 = undefined;
        var file_writer = binary_file.writer(binary_buffer[0..]);

        try diskScan.runWithBinaryOutput(&file_writer.interface, true);
        try file_writer.end();
    } else {
        try diskScan.run();
    }
}

fn parseSize(value: []const u8) !u64 {
    if (value.len == 0) return error.InvalidSize;

    var slice = value;
    var multiplier: u64 = 1;

    var suffix = ascii.toUpper(slice[slice.len - 1]);
    if (suffix == 'B') {
        if (slice.len == 1) return error.InvalidSize;
        slice = slice[0 .. slice.len - 1];
        suffix = ascii.toUpper(slice[slice.len - 1]);
    }

    switch (suffix) {
        'K' => {
            multiplier = 1024;
            slice = slice[0 .. slice.len - 1];
        },
        'M' => {
            multiplier = 1024 * 1024;
            slice = slice[0 .. slice.len - 1];
        },
        'G' => {
            multiplier = 1024 * 1024 * 1024;
            slice = slice[0 .. slice.len - 1];
        },
        'T' => {
            multiplier = 1024 * 1024 * 1024 * 1024;
            slice = slice[0 .. slice.len - 1];
        },
        else => {},
    }

    if (slice.len == 0) return error.InvalidSize;

    const number = std.fmt.parseInt(u64, slice, 10) catch return error.InvalidSize;
    if (multiplier == 1) return number;

    if (number > std.math.maxInt(u64) / multiplier) {
        return error.InvalidSize;
    }

    return number * multiplier;
}

fn printUsage(exe_name: []const u8) !void {
    try stderr.print(
        "usage: {s} [--skip-hidden] [--large-file-threshold SIZE] [--binary-output PATH] [dir]\n",
        .{exe_name},
    );
    try stderr.print("       SIZE accepts optional K/M/G/T suffix (base 1024)\n", .{});
}
