const std = @import("std");
const wtfs = @import("wtfs").mac;

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;
pub fn main() !void {
    const gpa = std.heap.page_allocator;

    var dir = try std.fs.cwd().openDir(".", .{ .iterate = true });
    defer dir.close();

    try stdout.print("Scanning dir (fd={d})...\n", .{dir.fd});

    const result = try wtfs.scanDirBulk(dir.fd, gpa, struct {
        fn on(e: wtfs.Entry) void {
            stdout.print("{s}\t{s}\t{d} alloc={d} total={d}\n", .{
                switch (e.kind) {
                    .dir => "dir ",
                    .file => "file",
                    .symlink => "lnk ",
                    .other => "oth ",
                },
                e.name,
                e.linkcount,
                e.alloc,
                e.total,
            }) catch {};
        }
    }.on);

    try stdout.print("Total entries: {d}\n", .{result});

    try stdout.flush();
}
