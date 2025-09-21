const std = @import("std");
const Tabular = @import("TabWriter.zig");
const TabWriter = Tabular.TabWriter;

pub const magic: u32 = 0x52545457; // "WTTR"
pub const version: u16 = 1;

const Header = packed struct {
    magic: u32,
    version: u16,
    reserved: u16,
};

pub const Operation = enum(u8) {
    open,
    stat,
};

const EventType = enum(u8) { submit, completion, fallback, drain, cache_hit };

const SubmitData = packed struct {
    operation: Operation,
    context_fd: i32,
    name_len: u32,
};

const CompletionData = packed struct {
    operation: Operation,
    res: i32,
    duration_ns: u64,
};

const FallbackData = packed struct {
    operation: Operation,
    reason_len: u32,
};

const DrainData = packed struct {
    wait_nr: u32,
    count: u32,
    matched: u8,
    others: u32,
};

const CacheData = packed struct {
    res: i32,
};

const EventPayload = packed union {
    submit: SubmitData,
    completion: CompletionData,
    fallback: FallbackData,
    drain: DrainData,
    cache_hit: CacheData,
};

const EventRecord = packed struct {
    timestamp_ns: u64,
    token: u64,
    tag: EventType,
    payload: EventPayload,
};

pub const Writer = struct {
    mutex: std.Thread.Mutex = .{},
    stream: *std.Io.Writer,
    allocator: std.mem.Allocator,
    timer: ?std.time.Timer,
    fallback_origin: u64,
    token_times: std.AutoHashMap(u64, u64),

    inline fn nowNs() u64 {
        return @as(u64, @intCast(std.time.nanoTimestamp()));
    }

    pub fn init(allocator: std.mem.Allocator, stream: *std.Io.Writer) !Writer {
        const maybe_timer = std.time.Timer.start() catch null;
        const origin = if (maybe_timer != null) 0 else nowNs();

        var writer = Writer{
            .stream = stream,
            .allocator = allocator,
            .timer = maybe_timer,
            .fallback_origin = origin,
            .token_times = std.AutoHashMap(u64, u64).init(allocator),
        };

        try writer.stream.writeStruct(Header{ .magic = magic, .version = version, .reserved = 0 }, .little);
        return writer;
    }

    pub fn deinit(self: *Writer) void {
        self.token_times.deinit();
    }

    fn currentElapsed(self: *Writer) u64 {
        if (self.timer) |*timer| {
            return timer.read();
        }
        return nowNs() - self.fallback_origin;
    }

    fn writeRecord(self: *Writer, record: EventRecord) !void {
        try self.stream.writeStruct(record, .little);
    }

    pub fn logSubmitOpen(self: *Writer, token: u64, parent_fd: std.posix.fd_t, name: []const u8) void {
        self.logSubmit(.open, token, parent_fd, name);
    }

    pub fn logSubmitStat(self: *Writer, token: u64, dir_fd: std.posix.fd_t, name: []const u8) void {
        self.logSubmit(.stat, token, dir_fd, name);
    }

    fn logSubmit(self: *Writer, op: Operation, token: u64, ctx_fd: std.posix.fd_t, name: []const u8) void {
        const elapsed = self.currentElapsed();

        self.mutex.lock();
        defer self.mutex.unlock();

        const record = EventRecord{
            .timestamp_ns = elapsed,
            .token = token,
            .tag = .submit,
            .payload = .{ .submit = .{
                .operation = op,
                .context_fd = ctx_fd,
                .name_len = @intCast(name.len),
            } },
        };

        self.writeRecord(record) catch return;
        self.stream.writeAll(name) catch return;
        self.token_times.put(token, elapsed) catch {};
    }

    pub fn logCompletion(self: *Writer, op: Operation, token: u64, res: i32) void {
        const elapsed = self.currentElapsed();

        self.mutex.lock();
        defer self.mutex.unlock();

        const duration_entry = self.token_times.fetchRemove(token);
        const duration_ns = if (duration_entry) |entry|
            elapsed - entry.value
        else
            std.math.maxInt(u64);

        const record = EventRecord{
            .timestamp_ns = elapsed,
            .token = token,
            .tag = .completion,
            .payload = .{ .completion = .{
                .operation = op,
                .res = res,
                .duration_ns = duration_ns,
            } },
        };
        self.writeRecord(record) catch return;
    }

    pub fn logFallback(self: *Writer, op: Operation, reason: []const u8) void {
        const elapsed = self.currentElapsed();

        self.mutex.lock();
        defer self.mutex.unlock();

        const record = EventRecord{
            .timestamp_ns = elapsed,
            .token = 0,
            .tag = .fallback,
            .payload = .{ .fallback = .{
                .operation = op,
                .reason_len = @intCast(reason.len),
            } },
        };
        self.writeRecord(record) catch return;
        self.stream.writeAll(reason) catch return;
    }

    pub fn logDrain(
        self: *Writer,
        token: u64,
        wait_nr: u32,
        total: usize,
        matched: bool,
        others: usize,
    ) void {
        const elapsed = self.currentElapsed();

        self.mutex.lock();
        defer self.mutex.unlock();

        const record = EventRecord{
            .timestamp_ns = elapsed,
            .token = token,
            .tag = .drain,
            .payload = .{ .drain = .{
                .wait_nr = wait_nr,
                .count = @intCast(total),
                .matched = @intCast(@intFromBool(matched)),
                .others = @intCast(others),
            } },
        };
        self.writeRecord(record) catch return;
    }

    pub fn logCacheHit(self: *Writer, token: u64, res: i32) void {
        const elapsed = self.currentElapsed();

        self.mutex.lock();
        defer self.mutex.unlock();

        const record = EventRecord{
            .timestamp_ns = elapsed,
            .token = token,
            .tag = .cache_hit,
            .payload = .{ .cache_hit = .{ .res = res } },
        };
        self.writeRecord(record) catch return;
    }
};

pub const SubmitEvent = struct {
    timestamp_ns: u64,
    operation: Operation,
    token: u64,
    context_fd: i32,
    name: []const u8,
};

pub const CompletionEvent = struct {
    timestamp_ns: u64,
    operation: Operation,
    token: u64,
    res: i32,
    duration_ns: ?u64,
};

pub const FallbackEvent = struct {
    timestamp_ns: u64,
    operation: Operation,
    reason: []const u8,
};

pub const DrainEvent = struct {
    timestamp_ns: u64,
    token: u64,
    wait_nr: u32,
    count: u32,
    matched: bool,
    others: u32,
};

pub const CacheHitEvent = struct {
    timestamp_ns: u64,
    token: u64,
    res: i32,
};

pub const Event = union(EventType) {
    submit: SubmitEvent,
    completion: CompletionEvent,
    fallback: FallbackEvent,
    drain: DrainEvent,
    cache_hit: CacheHitEvent,
};

pub const Reader = struct {
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    arena: std.heap.ArenaAllocator,
    exhausted: bool = false,

    pub fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader) !Reader {
        const arena = std.heap.ArenaAllocator.init(allocator);
        const r = Reader{
            .allocator = allocator,
            .reader = reader,
            .arena = arena,
        };

        const header = try reader.takeStruct(packed struct {
            magic: u32,
            version: u16,
            reserved: u16,
        }, .little);
        if (header.magic != magic or header.version != version) return error.BadTrace;

        return r;
    }

    pub fn deinit(self: *Reader) void {
        self.arena.deinit();
    }

    pub fn next(self: *Reader) !?Event {
        if (self.exhausted) return null;
        _ = self.arena.reset(.retain_capacity);

        const record = self.reader.takeStruct(EventRecord, .little) catch |err| switch (err) {
            error.EndOfStream => {
                self.exhausted = true;
                return null;
            },
            else => return err,
        };

        return switch (record.tag) {
            .submit => blk: {
                const data = record.payload.submit;
                const name = try self.readBytes(data.name_len);
                break :blk Event{ .submit = .{
                    .timestamp_ns = record.timestamp_ns,
                    .operation = data.operation,
                    .token = record.token,
                    .context_fd = data.context_fd,
                    .name = name,
                } };
            },
            .completion => blk: {
                const data = record.payload.completion;
                const duration = if (data.duration_ns == std.math.maxInt(u64)) null else data.duration_ns;
                break :blk Event{ .completion = .{
                    .timestamp_ns = record.timestamp_ns,
                    .operation = data.operation,
                    .token = record.token,
                    .res = data.res,
                    .duration_ns = duration,
                } };
            },
            .fallback => blk: {
                const data = record.payload.fallback;
                const reason = try self.readBytes(data.reason_len);
                break :blk Event{ .fallback = .{
                    .timestamp_ns = record.timestamp_ns,
                    .operation = data.operation,
                    .reason = reason,
                } };
            },
            .drain => blk: {
                const data = record.payload.drain;
                break :blk Event{ .drain = .{
                    .timestamp_ns = record.timestamp_ns,
                    .token = record.token,
                    .wait_nr = data.wait_nr,
                    .count = data.count,
                    .matched = data.matched != 0,
                    .others = data.others,
                } };
            },
            .cache_hit => blk: {
                const data = record.payload.cache_hit;
                break :blk Event{ .cache_hit = .{
                    .timestamp_ns = record.timestamp_ns,
                    .token = record.token,
                    .res = data.res,
                } };
            },
        };
    }

    fn readBytes(self: *Reader, len: u32) ![]const u8 {
        if (len == 0) return &[_]u8{};
        const buf = try self.arena.allocator().alloc(u8, len);
        try self.reader.*.readSliceAll(buf);
        return buf;
    }
};

pub fn summarizeFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    stdout: *std.Io.Writer,
) !void {
    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    defer file.close();

    var reader_buffer: [8 * 1024]u8 = undefined;
    var reader_iface = file.reader(reader_buffer[0..]);
    var trace_reader = try Reader.init(allocator, &reader_iface.interface);
    defer trace_reader.deinit();

    const SubmitInfo = struct {
        operation: Operation,
        timestamp_ns: u64,
    };

    var submit_map = std.AutoHashMap(u64, SubmitInfo).init(allocator);
    defer submit_map.deinit();

    const DurationStat = struct {
        count: usize = 0,
        total_ns: u128 = 0,
        min_ns: u64 = std.math.maxInt(u64),
        max_ns: u64 = 0,

        fn add(self: *@This(), value: u64) void {
            self.count += 1;
            self.total_ns += value;
            if (value < self.min_ns) self.min_ns = value;
            if (value > self.max_ns) self.max_ns = value;
        }
    };

    var duration_stats = [_]DurationStat{ .{}, .{} };
    var result_maps = [_]std.AutoHashMap(i32, usize){
        std.AutoHashMap(i32, usize).init(allocator),
        std.AutoHashMap(i32, usize).init(allocator),
    };
    defer result_maps[0].deinit();
    defer result_maps[1].deinit();

    var fallback_maps = [_]std.StringHashMap(usize){
        std.StringHashMap(usize).init(allocator),
        std.StringHashMap(usize).init(allocator),
    };
    defer {
        for (&fallback_maps) |*map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
            }
            map.deinit();
        }
    }

    var drain_calls: usize = 0;
    var drain_wait_total: usize = 0;
    var drain_items_total: usize = 0;
    var drain_matched_total: usize = 0;
    var drain_foreign_total: usize = 0;
    var drain_with_foreign: usize = 0;

    var cache_hits: usize = 0;

    while (try trace_reader.next()) |event| {
        switch (event) {
            .submit => |sub| {
                try submit_map.put(sub.token, .{ .operation = sub.operation, .timestamp_ns = sub.timestamp_ns });
            },
            .completion => |comp| {
                const idx = @intFromEnum(comp.operation);
                if (comp.duration_ns) |dur| {
                    duration_stats[idx].add(dur);
                } else if (submit_map.get(comp.token)) |info| {
                    const fallback_dur = comp.timestamp_ns - info.timestamp_ns;
                    duration_stats[idx].add(fallback_dur);
                }

                const entry = try result_maps[idx].getOrPut(comp.res);
                if (!entry.found_existing) entry.value_ptr.* = 0;
                entry.value_ptr.* += 1;

                _ = submit_map.fetchRemove(comp.token);
            },
            .fallback => |fb| {
                const idx = @intFromEnum(fb.operation);
                const entry = try fallback_maps[idx].getOrPut(fb.reason);
                if (!entry.found_existing) {
                    entry.key_ptr.* = try allocator.dupe(u8, fb.reason);
                    entry.value_ptr.* = 0;
                }
                entry.value_ptr.* += 1;
            },
            .drain => |dr| {
                drain_calls += 1;
                drain_wait_total += dr.wait_nr;
                drain_items_total += dr.count;
                if (dr.matched) drain_matched_total += 1;
                drain_foreign_total += dr.others;
                if (dr.others > 0) drain_with_foreign += 1;
            },
            .cache_hit => |_| {
                cache_hits += 1;
            },
        }
    }

    var duration_columns = [_]Tabular.Column{
        .{ .width = 10 },
        .{ .width = 10, .alignment = .right },
        .{ .width = 12, .alignment = .right },
        .{ .width = 12, .alignment = .right },
        .{ .width = 12, .alignment = .right },
    };

    try stdout.writeAll("Trace Durations (ms)\n");
    var duration_table = TabWriter.initWithOptions(stdout, duration_columns[0..], .{});
    try duration_table.writeHeader(&[_][]const u8{ "Operation", "Count", "Average", "Min", "Max" });
    try duration_table.writeSeparator("-");

    for (duration_stats, 0..) |stat, idx| {
        const op_name = switch (@as(Operation, @enumFromInt(idx))) {
            .open => "open",
            .stat => "stat",
        };

        var count_buf: [16]u8 = undefined;
        const count_str = try std.fmt.bufPrint(count_buf[0..], "{d}", .{stat.count});

        var avg_buf: [32]u8 = undefined;
        var min_buf: [32]u8 = undefined;
        var max_buf: [32]u8 = undefined;

        const avg_str = if (stat.count == 0)
            "-"
        else blk: {
            const avg_ns = @as(f64, @floatFromInt(stat.total_ns)) / @as(f64, @floatFromInt(stat.count));
            break :blk try std.fmt.bufPrint(avg_buf[0..], "{d:.3}", .{avg_ns / 1_000_000.0});
        };

        const min_str = if (stat.count == 0)
            "-"
        else blk: {
            break :blk try std.fmt.bufPrint(min_buf[0..], "{d:.3}", .{@as(f64, @floatFromInt(stat.min_ns)) / 1_000_000.0});
        };

        const max_str = if (stat.count == 0)
            "-"
        else blk: {
            break :blk try std.fmt.bufPrint(max_buf[0..], "{d:.3}", .{@as(f64, @floatFromInt(stat.max_ns)) / 1_000_000.0});
        };

        try duration_table.writeRow(&[_][]const u8{ op_name, count_str, avg_str, min_str, max_str });
    }

    try duration_table.finish();
    try stdout.writeByte('\n');

    var result_columns = [_]Tabular.Column{
        .{ .width = 10 },
        .{ .width = 10, .alignment = .right },
        .{ .width = 10, .alignment = .right },
    };

    try stdout.writeAll("Result Codes\n");
    var result_table = TabWriter.initWithOptions(stdout, result_columns[0..], .{});
    try result_table.writeHeader(&[_][]const u8{ "Operation", "Result", "Count" });
    try result_table.writeSeparator("-");

    const ResultEntry = struct { res: i32, count: usize };
    var result_entries = std.ArrayList(ResultEntry){};
    defer result_entries.deinit(allocator);

    for (&result_maps, 0..) |map, idx| {
        result_entries.clearRetainingCapacity();
        var it = map.iterator();
        while (it.next()) |entry| {
            try result_entries.append(allocator, .{ .res = entry.key_ptr.*, .count = entry.value_ptr.* });
        }

        std.sort.heap(ResultEntry, result_entries.items, {}, struct {
            fn lessThan(_: void, lhs: ResultEntry, rhs: ResultEntry) bool {
                return lhs.res < rhs.res;
            }
        }.lessThan);

        const op_name = switch (@as(Operation, @enumFromInt(idx))) {
            .open => "open",
            .stat => "stat",
        };

        for (result_entries.items) |entry| {
            var res_buf: [16]u8 = undefined;
            var cnt_buf: [16]u8 = undefined;
            const res_str = try std.fmt.bufPrint(res_buf[0..], "{d}", .{entry.res});
            const cnt_str = try std.fmt.bufPrint(cnt_buf[0..], "{d}", .{entry.count});
            try result_table.writeRow(&[_][]const u8{ op_name, res_str, cnt_str });
        }
    }

    try result_table.finish();
    try stdout.writeByte('\n');

    try stdout.writeAll("Fallbacks\n");
    var fallback_columns = [_]Tabular.Column{
        .{ .width = 10 },
        .{ .width = 20 },
        .{ .width = 10, .alignment = .right },
    };
    var fallback_table = TabWriter.initWithOptions(stdout, fallback_columns[0..], .{});
    try fallback_table.writeHeader(&[_][]const u8{ "Operation", "Reason", "Count" });
    try fallback_table.writeSeparator("-");

    const ReasonEntry = struct { reason: []const u8, count: usize };
    var reason_entries = std.ArrayList(ReasonEntry){};
    defer reason_entries.deinit(allocator);

    for (&fallback_maps, 0..) |map, idx| {
        reason_entries.clearRetainingCapacity();
        var it = map.iterator();
        while (it.next()) |entry| {
            try reason_entries.append(allocator, .{ .reason = entry.key_ptr.*, .count = entry.value_ptr.* });
        }

        std.sort.heap(ReasonEntry, reason_entries.items, {}, struct {
            fn lessThan(_: void, lhs: ReasonEntry, rhs: ReasonEntry) bool {
                return std.mem.lessThan(u8, lhs.reason, rhs.reason);
            }
        }.lessThan);

        const op_name = switch (@as(Operation, @enumFromInt(idx))) {
            .open => "open",
            .stat => "stat",
        };

        for (reason_entries.items) |entry| {
            var cnt_buf: [16]u8 = undefined;
            const cnt_str = try std.fmt.bufPrint(cnt_buf[0..], "{d}", .{entry.count});
            try fallback_table.writeRow(&[_][]const u8{ op_name, entry.reason, cnt_str });
        }
    }

    try fallback_table.finish();
    try stdout.writeByte('\n');

    if (drain_calls > 0) {
        const avg_wait = @as(f64, @floatFromInt(drain_wait_total)) / @as(f64, @floatFromInt(drain_calls));
        const avg_items = @as(f64, @floatFromInt(drain_items_total)) / @as(f64, @floatFromInt(drain_calls));
        const avg_foreign = @as(f64, @floatFromInt(drain_foreign_total)) / @as(f64, @floatFromInt(drain_calls));
        try stdout.print(
            "Drain stats: calls={d}, avg_wait={d:.2}, avg_items={d:.2}, avg_foreign={d:.2}, matched={d}, foreign_hits={d}\n",
            .{ drain_calls, avg_wait, avg_items, avg_foreign, drain_matched_total, drain_with_foreign },
        );
    } else {
        try stdout.writeAll("Drain stats: no drain events recorded\n");
    }

    try stdout.print("Cache hits: {d}\n", .{cache_hits});

    if (submit_map.count() != 0) {
        try stdout.print("Warning: {d} tokens without completion\n", .{submit_map.count()});
    }
}
