comptime {
    _ = @import("TabWriter.zig");
    _ = @import("DiskScan.zig");
    _ = @import("DirScanner.zig");
    _ = @import("SysDispatcher.zig");
    _ = @import("SegmentedMultiArray.zig");
}

const std = @import("std");
const DiskScan = @import("DiskScan.zig");
const SysDispatcher = @import("SysDispatcher.zig");
const WorkerMod = @import("Worker.zig");
const Tabular = @import("TabWriter.zig");
const Writer = std.Io.Writer;
const builtin = @import("builtin");
const ascii = std.ascii;
const Report = @import("Report.zig");

const TabWriter = Tabular.TabWriter;
const Column = Tabular.Column;

var stderr_buffer: [4096]u8 = undefined;
var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
const stderr = &stderr_writer.interface;
pub fn main() !void {
    const gpa = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var thread_safe_arena = std.heap.ThreadSafeAllocator{
        .child_allocator = arena.allocator(),
    };
    const allocator = thread_safe_arena.allocator();

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
    var dump_structs = false;

    const threshold_prefix = "--large-file-threshold=";
    const binary_prefix = "--binary-output=";

    defer stderr.flush() catch {};

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
        if (std.mem.eql(u8, arg, "--dump-structs")) {
            dump_structs = true;
            continue;
        }
        if (root_arg != null) {
            try printUsage(exe_name);
            return;
        }
        root_arg = arg;
    }

    if (dump_structs) {
        try dumpStructLayouts();
        return;
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
        "usage: {s} [--skip-hidden] [--large-file-threshold SIZE] [--binary-output PATH] [--dump-structs] [dir]\n",
        .{exe_name},
    );
    try stderr.print("       SIZE accepts optional K/M/G/T suffix (base 1024)\n", .{});
}

fn dumpStructLayouts() !void {
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    try dumpStruct(stdout, "Context", @import("Context.zig"));
    try dumpStruct(stdout, "Worker", WorkerMod.Worker);
    try dumpStruct(stdout, "Worker.Scanner", WorkerMod.Scanner);
    if (builtin.target.os.tag == .linux) {
        const entries_type = @FieldType(WorkerMod.Scanner, "entries");
        const entries_info = @typeInfo(entries_type);
        switch (entries_info) {
            .array => |array_info| {
                try dumpStruct(stdout, "Scanner.BatchEntry", array_info.child);
            },
            else => {},
        }
    }
    try dumpStruct(stdout, "SysDispatcher.Config", SysDispatcher.Config);
    try dumpStruct(stdout, "SysDispatcher.StatRequest", SysDispatcher.StatRequest);
    try dumpStruct(stdout, "SysDispatcher.LinuxBackend", SysDispatcher.LinuxBackend);
    try dumpStruct(stdout, "fs.Dir.Iterator", std.fs.Dir.Iterator);
}

fn dumpStruct(writer: *Writer, comptime name: []const u8, comptime T: type) !void {
    const info = @typeInfo(T);

    switch (info) {
        .@"struct" => |struct_info| {
            try writer.print("{s: <20} ", .{name});
            try writer.print("{d} bytes, aligned to {d}\n\n", .{
                @as(usize, @sizeOf(T)),
                @as(usize, @alignOf(T)),
            });

            if (struct_info.fields.len > 0) {
                const Row = struct {
                    field: []const u8,
                    type_name: []const u8,
                    size: usize,
                };

                const capacity = struct_info.fields.len * 2 + 1;
                var rows: [capacity]Row = undefined;
                var row_count: usize = 0;

                var expected_offset: usize = 0;

                inline for (struct_info.fields) |field| {
                    const field_offset = @offsetOf(T, field.name);
                    const field_size = @sizeOf(field.type);
                    const field_align = @alignOf(field.type);

                    const aligned_offset = std.mem.alignForward(usize, expected_offset, field_align);
                    const padding = aligned_offset - expected_offset;

                    if (padding > 0) {
                        rows[row_count] = .{
                            .field = "",
                            .type_name = "",
                            .size = padding,
                        };
                        row_count += 1;
                    }

                    const type_name = @typeName(field.type);
                    const max_type_len = 32;
                    const display_type = if (type_name.len > max_type_len)
                        type_name[0 .. max_type_len - 2] ++ ".."
                    else
                        type_name;

                    rows[row_count] = .{
                        .field = "." ++ field.name,
                        .type_name = display_type,
                        .size = field_size,
                    };
                    row_count += 1;

                    expected_offset = field_offset + field_size;
                }

                const struct_size = @sizeOf(T);
                const trailing_padding = struct_size - expected_offset;
                if (trailing_padding > 0) {
                    rows[row_count] = .{
                        .field = "",
                        .type_name = "",
                        .size = trailing_padding,
                    };
                    row_count += 1;
                }

                var field_width: usize = "Field".len;
                var size_width: usize = "Size".len;
                var type_width: usize = "Type".len;

                var i: usize = 0;
                while (i < row_count) : (i += 1) {
                    const row = rows[i];

                    if (row.field.len > field_width) field_width = row.field.len;
                    if (row.type_name.len > type_width) type_width = row.type_name.len;

                    var size_buf: [32]u8 = undefined;
                    const size_value: u64 = @intCast(row.size);
                    const size_str = try Report.formatBytes(size_buf[0..], size_value);
                    if (size_str.len > size_width) size_width = size_str.len;
                }

                var columns = [_]Column{
                    Column{ .width = field_width },
                    Column{ .width = type_width },
                    Column{ .width = size_width, .alignment = .right },
                };

                var table = TabWriter.initWithOptions(writer, columns[0..], .{
                    .style = .box,
                });
                try table.writeHeader(&[_][]const u8{ "Field", "Type", "Size" });
                try table.writeSeparator("-");

                i = 0;
                while (i < row_count) : (i += 1) {
                    const row = rows[i];
                    var size_buf: [32]u8 = undefined;
                    const size_value: u64 = @intCast(row.size);
                    const size_str = try Report.formatBytes(size_buf[0..], size_value);

                    try table.writeRow(&[_][]const u8{
                        row.field,
                        row.type_name,
                        size_str,
                    });
                }

                try table.finish();

                try writer.writeByte('\n');
            } else {
                try writer.writeByte('\n');
            }
        },
        else => {
            try writer.print("{s}: not a struct\n\n", .{name});
        },
    }
}
