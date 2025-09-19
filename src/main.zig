const std = @import("std");
const wtfs = @import("wtfs").mac;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;
pub fn main() !void {
    const gpa = std.heap.page_allocator;

    var rootdir = try std.fs.cwd().openDir(".", .{ .iterate = true });
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
            .allocsize = true,
            .ioblocksize = false,
            .datalength = false,
        },
        .file = .{
            .linkcount = true,
            .totalsize = true,
            .allocsize = true,
        },
    }, void);

    const result = try Scanner.scan(gpa, rootdir.fd, void{}, struct {
        fn on(_: void, e: Scanner.Entry) !void {
            try stdout.print("{s} ", .{e.name});
            switch (e.kind) {
                .file => try stdout.print("(file) ", .{}),
                .dir => try stdout.print("(dir) ", .{}),
                .symlink => try stdout.print("(symlink) ", .{}),
                .other => try stdout.print("(other) ", .{}),
            }
            try stdout.print("  {x}:{x}\n", .{ e.fsid.id0, e.fsid.id1 });
            try stdout.print("  fileid={x}\n", .{e.fileid});
            switch (e.details) {
                .dir => |dir| {
                    try stdout.print("  linkcount={d}\n", .{dir.linkcount});
                    try stdout.print("  allocsize={Bi: <.2}\n", .{dir.allocsize});
                },
                .file => |file| {
                    try stdout.print("  linkcount={d}\n", .{file.linkcount});
                    try stdout.print("  totalsize={Bi: <.2}\n", .{file.totalsize});
                    try stdout.print("  allocsize={Bi: <.2}\n", .{file.allocsize});
                },
                .other => {},
            }
        }
    }.on);

    try stdout.print("Total entries: {d}\n", .{result});

    try stdout.flush();
}
