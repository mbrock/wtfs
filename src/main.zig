const std = @import("std");
const wtfs = @import("wtfs").mac;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;
pub fn main() !void {
    const args = try std.process.argsAlloc(std.heap.page_allocator);
    if (args.len != 2) {
        try stdout.print("usage: {s} <dir>\n", .{args[0]});
        return;
    }
    var rootdir = try std.fs.cwd().openDir(args[1], .{ .iterate = true });
    defer rootdir.close();

    try stdout.print("Scanning dir (fd={d})...\n", .{rootdir.fd});

    const Scanner = wtfs.DirScanner(.{
        .common = .{
            .name = true,
            .obj_type = true,
            .file_id = true,
            .fsid = true,
        },
        .dir = .{
            .linkcount = true,
            .entrycount = false,
            .mountstatus = false,
            .allocsize = false,
            .ioblocksize = false,
            .datalength = true,
        },
        .file = .{
            .linkcount = true,
            .totalsize = true,
            .allocsize = true,
        },
    });

    var buf: [16 * 1024]u8 = undefined;
    var scanner = Scanner.init(rootdir.fd, &buf);

    while (try scanner.next()) |e| {
        switch (e.kind) {
            .file => try stdout.print("- ", .{}),
            .dir => try stdout.print("* ", .{}),
            .symlink => try stdout.print("& ", .{}),
            .other => try stdout.print("? ", .{}),
        }
        try stdout.print("{s: <32} ", .{e.name});
        switch (e.details) {
            .dir => |dir| {
                try stdout.print(" x{d}", .{dir.datalength});
            },
            .file => |file| {
                try stdout.print(" {Bi: <12.2} ", .{file.totalsize});
                try stdout.print(" ({Bi: <.2})", .{file.allocsize});
            },
            .other => {},
        }
        try stdout.print("\n", .{});
        try stdout.flush();
    }
}
