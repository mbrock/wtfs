//! Find deleted files that processes still hold open (Linux). Their space is
//! not freed until the last descriptor closes, and no directory walk can see
//! them. Reads /proc/PID/fd/*; without root only your own processes show up.
//! Only files on filesystems the scan visited are reported.
const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");
const Scan = @import("Scan.zig");
const table = @import("table.zig");

const Allocator = std.mem.Allocator;
const suffix = " (deleted)";

pub fn find(a: Allocator, scan: *const Scan) ![]table.Deleted {
    if (builtin.os.tag != .linux) return &.{};

    var devs: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (0..scan.rows.len()) |i| {
        const r = scan.rows.at(i);
        if (r.kind == .dir and r.err == .none) try devs.put(a, r.dev, {});
    }

    const Key = struct { dev: u64, ino: u64 };
    var by_inode: std.AutoArrayHashMapUnmanaged(Key, usize) = .empty;
    var found: std.ArrayList(table.Deleted) = .empty;
    var holders: std.ArrayList(std.ArrayList(u8)) = .empty;

    const proc = sys.openRoot("/proc") catch return &.{};
    defer sys.close(proc);
    var proc_buf: [64 * 1024]u8 align(8) = undefined;
    var fd_buf: [64 * 1024]u8 align(8) = undefined;
    var procs = sys.DirReader.init(proc, &proc_buf);
    while (procs.next() catch null) |p| {
        const pid = std.fmt.parseInt(u32, p.name, 10) catch continue;
        var path_buf: [64]u8 = undefined;
        const fd_dir_path = try std.fmt.bufPrintSentinel(&path_buf, "{d}/fd", .{pid}, 0);
        const fd_dir = sys.openDir(proc, fd_dir_path) catch continue;
        defer sys.close(fd_dir);

        var comm_buf: [64]u8 = undefined;
        const comm = readComm(proc, pid, &comm_buf);

        var fds = sys.DirReader.init(fd_dir, &fd_buf);
        while (fds.next() catch null) |e| {
            var link_buf: [4096]u8 = undefined;
            const target = sys.readLinkAt(fd_dir, e.name, &link_buf) catch continue;
            if (!std.mem.endsWith(u8, target, suffix)) continue;
            if (target.len == 0 or target[0] != '/' or std.mem.startsWith(u8, target, "/memfd:")) continue;
            const st = sys.statAt(fd_dir, e.name, true) catch continue;
            if (st.kind != .file or !devs.contains(st.dev)) continue;

            const gop = try by_inode.getOrPut(a, .{ .dev = st.dev, .ino = st.ino });
            if (!gop.found_existing) {
                gop.value_ptr.* = found.items.len;
                var row: Scan.Row = .{
                    .str = "",
                    .size = st.size,
                    .alloc = st.alloc,
                    .ino = st.ino,
                    .dev = st.dev,
                    .mtime_ns = st.mtime_ns,
                    .parent = Scan.no_parent,
                    .depth = 0,
                    .nlink = st.nlink,
                    .uid = st.uid,
                    .gid = st.gid,
                    .perm = st.perm,
                    .kind = .file,
                };
                row.tree_du = row.alloc;
                row.tree_count = 1;
                try found.append(a, .{
                    .row = row,
                    .path = try a.dupe(u8, target[0 .. target.len - suffix.len]),
                    .open_by = "",
                });
                try holders.append(a, .empty);
            }
            const h = &holders.items[gop.value_ptr.*];
            if (h.items.len > 0) try h.appendSlice(a, ", ");
            try h.print(a, "pid {d} ({s}) fd {s}", .{ pid, comm, e.name });
        }
    }
    for (found.items, holders.items) |*d, h| d.open_by = h.items;
    std.mem.sort(table.Deleted, found.items, {}, struct {
        fn f(_: void, x: table.Deleted, y: table.Deleted) bool {
            return x.row.alloc > y.row.alloc;
        }
    }.f);
    return found.items;
}

fn readComm(proc: sys.fd_t, pid: u32, buf: []u8) []const u8 {
    var path_buf: [64]u8 = undefined;
    const path = std.fmt.bufPrintSentinel(&path_buf, "{d}/comm", .{pid}, 0) catch return "?";
    const fd = std.posix.openatZ(proc, path, .{ .CLOEXEC = true }, 0) catch return "?";
    defer sys.close(fd);
    const rc = std.posix.system.read(fd, buf.ptr, buf.len);
    if (std.posix.errno(rc) != .SUCCESS) return "?";
    const n: usize = @intCast(rc);
    return std.mem.trimEnd(u8, buf[0..n], "\n");
}
