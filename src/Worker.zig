const std = @import("std");
const Context = @import("Context.zig");
const wtfs = @import("wtfs").mac;

/// Scanner configured to read names, object types, and file sizes.
const Scanner = wtfs.DirScanner(.{
    .common = .{
        .name = true,
        .objtype = true,
    },
    .dir = .{
        .mountstatus = true,
    },
    .file = .{
        .totalsize = false,
        .allocsize = true,
    },
});

pub fn directoryWorker(ctx: *Context) void {
    var path_buffer = std.ArrayList(u8){};
    defer path_buffer.deinit(ctx.allocator);

    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    var errprogress = ctx.errprogress;
    var marquee = ctx.progress_node.start("...", 0);
    defer marquee.end();

    var timer = std.time.Timer.start() catch unreachable;

    var namebuf: [std.fs.max_name_bytes]u8 = undefined;

    var i: usize = 0;
    while (true) {
        const maybe_index = ctx.task_queue.pop();
        if (maybe_index == null) break;
        const index = maybe_index.?;

        if (@mod(i, 100) == 0) {
            const basename = takename(ctx, ctx.getNode(index).basename, &namebuf);

            const dt_ns = timer.lap();
            const speed = i * 1_000_000_000 / @max(dt_ns, 1);
            w.print("{d: >5} Hz  {s}", .{ speed, basename }) catch unreachable;
            marquee.setName(w.buffered());
            w.end = 0;
            //            speedometer.setCompletedItems(speed / 1000);
            i = 1;
        }
        i += 1;

        //        progress.increaseEstimatedTotalItems(1);

        _ = processDirectory(ctx, &ctx.progress_node, &errprogress, index) catch unreachable;
        //        progress.setCompletedItems(n);

        if (ctx.outstanding.fetchSub(1, .acq_rel) == 1) {
            ctx.task_queue.close();
        }
    }
}

fn takename(ctx: *Context, idx: u32, buffer: []u8) [:0]const u8 {
    ctx.namelock.lock();
    defer ctx.namelock.unlock();

    const slice = std.mem.sliceTo(ctx.namedata.items[idx..], 0);
    @memcpy(buffer[0..slice.len], slice);
    buffer[slice.len] = 0;

    return buffer[0..slice.len :0];
}

fn processDirectory(
    ctx: *Context,
    progress: *std.Progress.Node,
    errprogress: *std.Progress.Node,
    index: usize,
) !usize {
    _ = ctx.stats.directories_started.fetchAdd(1, .monotonic);
    defer _ = ctx.stats.directories_completed.fetchAdd(1, .monotonic);

    var buf: [1024 * 1024]u8 = undefined;
    var namebuf: [std.fs.max_name_bytes]u8 = undefined;

    const node = ctx.getNode(index);
    const basename = takename(ctx, node.basename, &namebuf);

    // var scanprogress = progress.start(basename, 0);
    // defer scanprogress.end();

    var dir_fd: std.posix.fd_t = undefined;
    if (index != 0) {
        const parent_index: usize = @intCast(node.parent);
        const parent = ctx.getNode(parent_index);
        const opened = wtfs.openSubdirectory(parent.fd, basename);

        ctx.directories_mutex.lock();
        const refcnt = ctx.fdRefSub(parent_index, 1, .seq_cst);
        if (refcnt == 1) {
            ctx.closeDirectory(parent_index);
        }
        ctx.directories_mutex.unlock();

        if (opened) |fd| {
            dir_fd = fd;
            ctx.setDirectoryFd(index, fd);
        } else |err| {
            switch (err) {
                error.PermissionDenied, error.AccessDenied => {
                    _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
                    ctx.markInaccessible(index);
                    return 0;
                },
                else => {
                    errprogress.setName(@errorName(err));
                    errprogress.completeOne();
                    return err;
                },
            }
        }
    } else {
        const opened = std.posix.openat(node.fd, basename, .{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        }, 0) catch |err| switch (err) {
            error.PermissionDenied, error.AccessDenied => {
                progress.completeOne();
                _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            },
            else => {
                errprogress.setName(@errorName(err));
                errprogress.completeOne();
                return err;
            },
        };
        if (node.fd != Context.invalid_fd) {
            std.posix.close(node.fd);
        }
        dir_fd = opened;
        ctx.setDirectoryFd(index, opened);
    }

    var scanner = Scanner.init(dir_fd, &buf);
    var total_size: u64 = 0;
    var total_files: usize = 0;

    ctx.directories_mutex.lock();
    _ = ctx.fdRefAdd(index, 1, .acq_rel);
    ctx.directories_mutex.unlock();

    while (true) {
        const has_batch = scanner.fill() catch |err| {
            _ = ctx.stats.scanner_errors.fetchAdd(1, .monotonic);
            errprogress.completeOne();
            if (err == error.DeadLock) {
                errprogress.setName("dataless file");
                _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
                ctx.markInaccessible(index);
                return 0;
            } else {
                errprogress.setName(@errorName(err));
                return err;
            }
        };
        if (!has_batch) break;

        ctx.directories_mutex.lock();
        var lock_released = false;
        defer if (!lock_released) ctx.directories_mutex.unlock();

        var batch_entries: usize = 0;
        var batch_dirs: usize = 0;
        var batch_files: usize = 0;
        var batch_symlinks: usize = 0;
        var batch_other: usize = 0;

        while (true) {
            const maybe_entry = scanner.next() catch |err| {
                ctx.directories_mutex.unlock();
                lock_released = true;
                _ = ctx.stats.scanner_errors.fetchAdd(1, .monotonic);
                errprogress.completeOne();
                if (err == error.DeadLock) {
                    errprogress.setName("dataless file");
                    _ = ctx.stats.inaccessible_dirs.fetchAdd(1, .monotonic);
                    ctx.markInaccessible(index);
                    return 0;
                } else {
                    errprogress.setName(@errorName(err));
                    return err;
                }
            };

            const entry = maybe_entry orelse break;
            batch_entries += 1;
            const name = std.mem.sliceTo(entry.name, 0);
            if (name.len == 0) continue;
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;

            switch (entry.kind) {
                .dir => {
                    if (ctx.skip_hidden and name[0] == '.') continue;
                    _ = ctx.fdRefAdd(index, 1, .acq_rel);
                    const child_index = try ctx.addChild(index, name);
                    try ctx.scheduleDirectory(child_index);
                    batch_dirs += 1;
                },
                .file => {
                    const file = entry.details.file;
                    total_size += file.allocsize;
                    total_files += 1;
                    batch_files += 1;
                },
                .symlink => batch_symlinks += 1,
                .other => batch_other += 1,
            }
        }

        ctx.directories_mutex.unlock();
        lock_released = true;

        _ = ctx.stats.scanner_batches.fetchAdd(1, .monotonic);
        _ = ctx.stats.scanner_entries.fetchAdd(batch_entries, .monotonic);
        _ = ctx.stats.directories_discovered.fetchAdd(batch_dirs, .monotonic);
        _ = ctx.stats.files_discovered.fetchAdd(batch_files, .monotonic);
        _ = ctx.stats.symlinks_discovered.fetchAdd(batch_symlinks, .monotonic);
        _ = ctx.stats.other_discovered.fetchAdd(batch_other, .monotonic);
        _ = ctx.stats.scanner_max_batch.fetchMax(batch_entries, .acq_rel);
    }

    ctx.directories_mutex.lock();
    const refcnt = ctx.fdRefSub(index, 1, .acq_rel);
    if (refcnt == 1) {
        ctx.closeDirectory(index);
    }
    ctx.directories_mutex.unlock();

    ctx.setTotals(index, total_size, total_files);

    return total_size;
}
