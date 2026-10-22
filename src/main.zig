const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const Scan = @import("Scan.zig");
const table = @import("table.zig");
const arrow = @import("arrow.zig");
const parquet = @import("parquet.zig");
const report = @import("report.zig");
const dump = @import("dump.zig");
const deleted_files = @import("deleted.zig");

const usage =
    \\wtfs - find out where disk space went
    \\
    \\usage: wtfs [options] [PATH]
    \\
    \\Scans PATH (default .) in parallel and prints a plain-text summary: the
    \\total, PATH's children, the largest directories and the largest files.
    \\Data lines are "DU_BYTES  HUMAN  [ENTRIES]  PATH" with full paths; header
    \\lines start with '#'. Sizes are bytes allocated on disk, like du: hard-
    \\linked files are counted once.
    \\
    \\options:
    \\  -o, --output FILE     also write every entry as one table row to FILE:
    \\                          *.parquet   Parquet (recommended); DuckDB reads it natively
    \\                          *.arrow     Arrow IPC (Feather v2); DuckDB needs
    \\                                      INSTALL arrow FROM community; LOAD arrow;
    \\  -n, --top N           lines per summary section (default 20; 0 = totals only)
    \\  -x, --one-file-system do not descend into other filesystems
    \\      --deleted         also list deleted files still held open on the scanned
    \\                        filesystems (Linux, /proc); table rows kind='deleted'
    \\  -j, --threads N       worker threads (default: number of CPUs)
    \\  -q, --quiet           no progress display on stderr
    \\  -h, --help            show this help
    \\      --version         print the version
    \\
    \\table columns (one row per file, directory, symlink, ...):
    \\  path kind size alloc du tree_du tree_count id parent depth
    \\  dev ino nlink uid gid mode mtime error open_by
    \\  du = alloc, or 0 for a hardlink whose inode was counted at another path;
    \\  sum(du) WHERE kind <> 'deleted' equals `du -sB1 PATH`.
    \\  tree_du = du of the whole subtree.
    \\
    \\examples:
    \\  wtfs -x /home
    \\  wtfs -o /tmp/nix.parquet /nix/store
    \\  duckdb -c "SELECT tree_du, path FROM '/tmp/nix.parquet'
    \\             WHERE kind = 'dir' ORDER BY tree_du DESC LIMIT 20"
    \\
    \\Pseudo filesystems (proc, sysfs, cgroup, ...) are never descended into.
    \\
    \\Python TUI compatibility: --binary-output FILE writes the legacy wtfsdumpv1
    \\snapshot, with --large-file-threshold SIZE and --skip-hidden.
    \\
;

fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("wtfs: " ++ fmt ++ "\n", args);
    std.process.exit(2);
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    const gpa = std.heap.smp_allocator;

    var args_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, arena);
    _ = args_it.next();
    var args: std.ArrayList([]const u8) = .empty;
    while (args_it.next()) |arg| try args.append(arena, try arena.dupe(u8, arg));

    var root: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var binary_output: ?[]const u8 = null;
    var large_file_threshold: u64 = 100 * 1024 * 1024;
    var top: usize = 20;
    var opts: Scan.Options = .{};
    var quiet = false;
    var force = false;
    var want_deleted = false;

    var i: usize = 0;
    while (i < args.items.len) : (i += 1) {
        const arg = args.items[i];
        var value: ?[]const u8 = null;
        var flag = arg;
        if (std.mem.startsWith(u8, arg, "--")) {
            if (std.mem.indexOfScalar(u8, arg, '=')) |eq| {
                flag = arg[0..eq];
                value = arg[eq + 1 ..];
            }
        } else if (arg.len > 2 and arg[0] == '-' and std.mem.indexOfScalar(u8, "onj", arg[1]) != null) {
            // -n5, -j8, -oscan.parquet
            flag = arg[0..2];
            value = arg[2..];
        }
        const Next = struct {
            fn get(items: []const []const u8, idx: *usize, f: []const u8, v: ?[]const u8) []const u8 {
                if (v) |x| return x;
                idx.* += 1;
                if (idx.* >= items.len) fail("{s} needs a value", .{f});
                return items[idx.*];
            }
        };
        if (eql(flag, "-h") or eql(flag, "--help")) {
            try writeStdout(io, usage);
            return;
        } else if (eql(flag, "--version")) {
            try writeStdout(io, "wtfs " ++ build_options.version ++ "\n");
            return;
        } else if (eql(flag, "-o") or eql(flag, "--output")) {
            output = Next.get(args.items, &i, flag, value);
        } else if (eql(flag, "-n") or eql(flag, "--top")) {
            const v = Next.get(args.items, &i, flag, value);
            top = std.fmt.parseInt(usize, v, 10) catch fail("invalid number for {s}: {s}", .{ flag, v });
        } else if (eql(flag, "-j") or eql(flag, "--threads")) {
            const v = Next.get(args.items, &i, flag, value);
            opts.threads = std.fmt.parseInt(usize, v, 10) catch fail("invalid number for {s}: {s}", .{ flag, v });
        } else if (eql(flag, "-x") or eql(flag, "--one-file-system")) {
            opts.one_file_system = true;
        } else if (eql(flag, "--deleted")) {
            want_deleted = true;
        } else if (eql(flag, "-q") or eql(flag, "--quiet")) {
            quiet = true;
        } else if (eql(flag, "--binary-output")) {
            binary_output = Next.get(args.items, &i, flag, value);
        } else if (eql(flag, "--large-file-threshold")) {
            const v = Next.get(args.items, &i, flag, value);
            large_file_threshold = parseSize(v) catch fail("invalid size for {s}: {s}", .{ flag, v });
        } else if (eql(flag, "--skip-hidden")) {
            opts.skip_hidden = true;
        } else if (eql(flag, "--force")) {
            force = true;
        } else if (arg.len > 1 and arg[0] == '-') {
            fail("unknown option {s} (see wtfs --help)", .{arg});
        } else if (root != null) {
            fail("only one PATH may be given", .{});
        } else {
            root = arg;
        }
    }
    const root_path = root orelse ".";

    const Format = enum { arrow, parquet };
    const format: ?Format = if (output) |o|
        if (std.mem.endsWith(u8, o, ".parquet"))
            .parquet
        else if (std.mem.endsWith(u8, o, ".arrow") or std.mem.endsWith(u8, o, ".feather") or std.mem.endsWith(u8, o, ".ipc"))
            .arrow
        else
            fail("cannot tell the format of {s}: use a .parquet or .arrow extension", .{o})
    else
        null;

    if (builtin.os.tag.isDarwin() and eql(root_path, "/") and !force) {
        fail(
            \\scanning / on macOS also walks firmlinked and system volumes.
            \\Scan /System/Volumes/Data, /Users or /Volumes/NAME instead, or pass --force.
        , .{});
    }

    // Create output files first so a bad path fails before a long scan.
    var out_file: ?std.Io.File = null;
    if (output) |o| out_file = std.Io.Dir.cwd().createFile(io, o, .{}) catch |err|
        fail("cannot create {s}: {t}", .{ o, err });
    var dump_file: ?std.Io.File = null;
    if (binary_output) |o| dump_file = std.Io.Dir.cwd().createFile(io, o, .{}) catch |err|
        fail("cannot create {s}: {t}", .{ o, err });

    var progress: std.Progress.Node = .none;
    if (!quiet) progress = std.Progress.start(io, .{ .root_name = "wtfs: directories scanned", .refresh_rate_ns = .fromMilliseconds(100) });
    opts.progress = progress;

    var scan: Scan = .{ .allocator = gpa, .io = io, .root = root_path, .options = opts };
    scan.run() catch |err| {
        progress.end();
        fail("cannot scan {s}: {t}", .{ root_path, err });
    };

    const deleted: []const table.Deleted = if (want_deleted) try deleted_files.find(arena, &scan) else &.{};

    if (out_file) |f| {
        progress.setName("wtfs: writing table");
        defer f.close(io);
        var buf: [256 * 1024]u8 = undefined;
        var fw = f.writer(io, &buf);
        const src: table.Source = .{ .scan = &scan, .deleted = deleted };
        switch (format.?) {
            .parquet => {
                const threads = if (opts.threads != 0) opts.threads else std.Thread.getCpuCount() catch 4;
                try parquet.write(gpa, io, &fw.interface, src, threads);
            },
            .arrow => {
                var batch: table.Batch = .{};
                defer batch.deinit(gpa);
                var w = try arrow.Writer.init(gpa, &fw.interface);
                defer w.deinit();
                var start: usize = 0;
                while (start < src.len()) {
                    const end = @min(start + table.batch_rows, src.len());
                    try batch.fill(gpa, src, start, end);
                    try w.writeBatch(&batch);
                    start = end;
                }
                try w.finish();
            },
        }
    }

    if (dump_file) |f| {
        defer f.close(io);
        var buf: [64 * 1024]u8 = undefined;
        var fw = f.writer(io, &buf);
        try dump.write(gpa, &fw.interface, &scan, large_file_threshold);
    }
    progress.end();

    var stdout_buf: [64 * 1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buf);
    try report.print(arena, &stdout.interface, &scan, deleted, .{ .top = top, .output_path = output });
    try stdout.interface.flush();
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn writeStdout(io: std.Io, s: []const u8) !void {
    var buf: [4096]u8 = undefined;
    var w = std.Io.File.stdout().writer(io, &buf);
    try w.interface.writeAll(s);
    try w.interface.flush();
}

fn parseSize(value: []const u8) !u64 {
    if (value.len == 0) return error.InvalidSize;
    var s = value;
    if (s[s.len - 1] == 'B' or s[s.len - 1] == 'b') s = s[0 .. s.len - 1];
    if (s.len == 0) return error.InvalidSize;
    const shift: u6 = switch (std.ascii.toUpper(s[s.len - 1])) {
        'K' => 10,
        'M' => 20,
        'G' => 30,
        'T' => 40,
        else => 0,
    };
    if (shift != 0) s = s[0 .. s.len - 1];
    const n = std.fmt.parseInt(u64, s, 10) catch return error.InvalidSize;
    return std.math.shlExact(u64, n, shift) catch error.InvalidSize;
}

test {
    _ = Scan;
    _ = table;
    _ = arrow;
    _ = parquet;
}
