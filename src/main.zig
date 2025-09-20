const std = @import("std");
const DiskScan = @import("DiskScan.zig");

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

    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        const arg = std.mem.sliceTo(args[arg_index], 0);
        if (std.mem.eql(u8, arg, "--skip-hidden")) {
            skip_hidden = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--help")) {
            try stderr.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        if (root_arg != null) {
            try stderr.print("usage: {s} [--skip-hidden] [dir]\n", .{exe_name});
            return;
        }
        root_arg = arg;
    }

    const root = root_arg orelse ".";

    var diskScan = DiskScan{
        .allocator = allocator,
        .skip_hidden = skip_hidden,
        .root = root,
    };

    try diskScan.run();
}
