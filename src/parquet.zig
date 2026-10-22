//! Minimal Apache Parquet writer: flat schema, one row group per batch, one
//! PLAIN-encoded data page (v1) per column chunk, gzip-compressed. Metadata
//! is Thrift compact protocol, encoded by hand below.
const std = @import("std");
const table = @import("table.zig");

const Allocator = std.mem.Allocator;

// ===== Thrift compact protocol =====

const T = struct {
    const bool_true = 1;
    const bool_false = 2;
    const byte = 3;
    const i32_ = 5;
    const i64_ = 6;
    const binary = 8;
    const list = 9;
    const @"struct" = 12;
};

const Thrift = struct {
    a: Allocator,
    buf: std.ArrayList(u8) = .empty,
    last: [16]i16 = @splat(0),
    depth: usize = 0,

    fn varint(t: *Thrift, v: u64) !void {
        var x = v;
        while (x >= 0x80) : (x >>= 7) try t.buf.append(t.a, @as(u8, @truncate(x)) | 0x80);
        try t.buf.append(t.a, @truncate(x));
    }

    fn zigzag(t: *Thrift, v: i64) !void {
        try t.varint(@bitCast((v << 1) ^ (v >> 63)));
    }

    fn header(t: *Thrift, id: i16, ty: u8) !void {
        const delta = id - t.last[t.depth];
        if (delta > 0 and delta <= 15) {
            try t.buf.append(t.a, @as(u8, @intCast(delta)) << 4 | ty);
        } else {
            try t.buf.append(t.a, ty);
            try t.zigzag(id);
        }
        t.last[t.depth] = id;
    }

    fn i32f(t: *Thrift, id: i16, v: i32) !void {
        try t.header(id, T.i32_);
        try t.zigzag(v);
    }

    fn i64f(t: *Thrift, id: i16, v: i64) !void {
        try t.header(id, T.i64_);
        try t.zigzag(v);
    }

    fn boolf(t: *Thrift, id: i16, v: bool) !void {
        try t.header(id, if (v) T.bool_true else T.bool_false);
    }

    fn bytef(t: *Thrift, id: i16, v: i8) !void {
        try t.header(id, T.byte);
        try t.buf.append(t.a, @bitCast(v));
    }

    fn str(t: *Thrift, s: []const u8) !void {
        try t.varint(s.len);
        try t.buf.appendSlice(t.a, s);
    }

    fn strf(t: *Thrift, id: i16, s: []const u8) !void {
        try t.header(id, T.binary);
        try t.str(s);
    }

    fn listf(t: *Thrift, id: i16, elem: u8, n: usize) !void {
        try t.header(id, T.list);
        try t.listHeader(elem, n);
    }

    fn listHeader(t: *Thrift, elem: u8, n: usize) !void {
        if (n < 15) {
            try t.buf.append(t.a, @as(u8, @intCast(n)) << 4 | elem);
        } else {
            try t.buf.append(t.a, 0xF0 | elem);
            try t.varint(n);
        }
    }

    fn begin(t: *Thrift, id: i16) !void {
        try t.header(id, T.@"struct");
        t.beginElem();
    }

    /// Start a struct that is a list element (no field header).
    fn beginElem(t: *Thrift) void {
        t.depth += 1;
        t.last[t.depth] = 0;
    }

    fn end(t: *Thrift) !void {
        try t.buf.append(t.a, 0);
        t.depth -= 1;
    }
};

// ===== Parquet enums =====

const ptype_int32 = 1;
const ptype_int64 = 2;
const ptype_byte_array = 6;
const rep_required = 0;
const rep_optional = 1;
const conv_utf8 = 0;
const conv_timestamp_micros = 10;
const conv_uint_32 = 13;
const conv_int_64 = 18;
const enc_plain = 0;
const enc_rle = 3;
const codec_gzip = 2;

fn physicalType(ty: table.Type) i32 {
    return switch (ty) {
        .utf8 => ptype_byte_array,
        .u32 => ptype_int32,
        .i64, .timestamp_us => ptype_int64,
    };
}

const ColumnChunk = struct {
    num_values: i64,
    uncompressed: i64,
    compressed: i64,
    /// Relative to the row group's start while encoding, then absolute.
    page_offset: i64,
};

const RowGroup = struct {
    rows: i64,
    bytes: i64,
    cols: [table.columns.len]ColumnChunk,
};

/// One row group being encoded by a worker thread.
const Slot = struct {
    batch: table.Batch = .{},
    page: std.ArrayList(u8) = .empty,
    gz: ?std.Io.Writer.Allocating = null,
    window: []u8 = &.{},
    data: std.ArrayList(u8) = .empty,
    group: RowGroup = undefined,
    err: ?anyerror = null,

    fn deinit(s: *Slot, a: Allocator) void {
        s.batch.deinit(a);
        s.page.deinit(a);
        if (s.gz) |*g| g.deinit();
        a.free(s.window);
        s.data.deinit(a);
    }

    fn run(s: *Slot, a: Allocator, src: table.Source, start: usize, end: usize) void {
        s.err = null;
        s.encode(a, src, start, end) catch |err| {
            s.err = err;
        };
    }

    /// Fill the batch, then append each column's page header and gzipped
    /// page to `data`.
    fn encode(s: *Slot, a: Allocator, src: table.Source, start: usize, end: usize) !void {
        if (s.gz == null) {
            s.gz = try .initCapacity(a, 1024 * 1024);
            s.window = try a.alloc(u8, std.compress.flate.max_window_len);
        }
        try s.batch.fill(a, src, start, end);
        const batch = &s.batch;
        s.data.clearRetainingCapacity();
        s.group = .{ .rows = @intCast(batch.len), .bytes = 0, .cols = undefined };
        for (&batch.cols, table.columns, &s.group.cols) |*c, col, *chunk| {
            try encodePage(a, &s.page, batch.len, c, col);
            const uncompressed = s.page.items.len;

            const gz = &s.gz.?;
            gz.clearRetainingCapacity();
            var z = try std.compress.flate.Compress.init(&gz.writer, s.window, .gzip, .level_1);
            try z.writer.writeAll(s.page.items);
            try z.finish();
            const compressed = gz.written();

            var hdr: Thrift = .{ .a = a };
            defer hdr.buf.deinit(a);
            try hdr.i32f(1, 0); // DATA_PAGE
            try hdr.i32f(2, @intCast(uncompressed));
            try hdr.i32f(3, @intCast(compressed.len));
            try hdr.begin(5); // DataPageHeader
            try hdr.i32f(1, @intCast(batch.len));
            try hdr.i32f(2, enc_plain);
            try hdr.i32f(3, enc_rle);
            try hdr.i32f(4, enc_rle);
            try hdr.end();
            try hdr.buf.append(a, 0);

            chunk.* = .{
                .num_values = @intCast(batch.len),
                .page_offset = @intCast(s.data.items.len),
                .uncompressed = @intCast(hdr.buf.items.len + uncompressed),
                .compressed = @intCast(hdr.buf.items.len + compressed.len),
            };
            s.group.bytes += chunk.uncompressed;
            try s.data.appendSlice(a, hdr.buf.items);
            try s.data.appendSlice(a, compressed);
        }
    }
};

/// Page body: definition levels (optional columns only), then the
/// PLAIN-encoded non-null values.
fn encodePage(a: Allocator, page: *std.ArrayList(u8), n: usize, c: *const table.ColumnData, col: table.Column) !void {
    page.clearRetainingCapacity();
    if (col.nullable) {
        // RLE/bit-packing hybrid with bit width 1, length-prefixed.
        var levels: Thrift = .{ .a = a };
        defer levels.buf.deinit(a);
        if (c.null_count == 0) {
            try levels.varint(@as(u64, n) << 1); // one RLE run of 1s
            try levels.buf.append(a, 1);
        } else {
            // Arrow validity bits are LSB-first, exactly Parquet's bit-packed layout.
            const groups = (n + 7) / 8;
            try levels.varint(@as(u64, groups) << 1 | 1);
            try levels.buf.appendSlice(a, c.validity.items[0..groups]);
        }
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, @intCast(levels.buf.items.len), .little);
        try page.appendSlice(a, &len);
        try page.appendSlice(a, levels.buf.items);
    }
    const valid = struct {
        fn f(cd: *const table.ColumnData, i: usize) bool {
            return cd.null_count == 0 or (cd.validity.items[i / 8] >> @intCast(i % 8)) & 1 != 0;
        }
    }.f;
    switch (col.type) {
        .utf8 => for (0..n) |i| {
            if (!valid(c, i)) continue;
            const s = c.values.items[@intCast(c.offsets.items[i])..@intCast(c.offsets.items[i + 1])];
            var len: [4]u8 = undefined;
            std.mem.writeInt(u32, &len, @intCast(s.len), .little);
            try page.appendSlice(a, &len);
            try page.appendSlice(a, s);
        },
        inline else => |ty| {
            const width: usize = if (ty == .u32) 4 else 8;
            if (c.null_count == 0) {
                try page.appendSlice(a, c.values.items);
            } else for (0..n) |i| {
                if (valid(c, i)) try page.appendSlice(a, c.values.items[i * width ..][0..width]);
            }
        },
    }
}

/// Write the whole table, encoding up to `threads` row groups at a time.
pub fn write(a: Allocator, io: std.Io, out: *std.Io.Writer, src: table.Source, threads: usize) !void {
    var w: Writer = .{ .a = a, .out = out };
    defer w.groups.deinit(a);
    try w.raw("PAR1");

    const slots = try a.alloc(Slot, @max(threads, 1));
    defer a.free(slots);
    @memset(slots, .{});
    defer for (slots) |*s| s.deinit(a);

    const total = src.len();
    var start: usize = 0;
    while (start < total) {
        var group: std.Io.Group = .init;
        var used: usize = 0;
        for (slots) |*slot| {
            if (start >= total) break;
            const end = @min(start + table.batch_rows, total);
            try group.concurrent(io, Slot.run, .{ slot, a, src, start, end });
            start = end;
            used += 1;
        }
        try group.await(io);
        for (slots[0..used]) |*slot| {
            if (slot.err) |err| return err;
            for (&slot.group.cols) |*c| c.page_offset += @intCast(w.pos);
            try w.raw(slot.data.items);
            w.rows += slot.group.rows;
            try w.groups.append(a, slot.group);
        }
    }
    try w.finish();
}

const Writer = struct {
    a: Allocator,
    out: *std.Io.Writer,
    pos: u64 = 0,
    rows: i64 = 0,
    groups: std.ArrayList(RowGroup) = .empty,

    fn raw(w: *Writer, bytes: []const u8) !void {
        try w.out.writeAll(bytes);
        w.pos += bytes.len;
    }

    fn finish(w: *Writer) !void {
        var t: Thrift = .{ .a = w.a };
        defer t.buf.deinit(w.a);

        try t.i32f(1, 1); // version
        try t.listf(2, T.@"struct", table.columns.len + 1);
        t.beginElem(); // root
        try t.strf(4, "schema");
        try t.i32f(5, table.columns.len);
        try t.end();
        for (table.columns) |col| {
            t.beginElem();
            try t.i32f(1, physicalType(col.type));
            try t.i32f(3, if (col.nullable) rep_optional else rep_required);
            try t.strf(4, col.name);
            try t.i32f(6, switch (col.type) {
                .utf8 => conv_utf8,
                .u32 => conv_uint_32,
                .i64 => conv_int_64,
                .timestamp_us => conv_timestamp_micros,
            });
            try t.begin(10); // LogicalType union
            switch (col.type) {
                .utf8 => {
                    try t.begin(1); // STRING
                    try t.end();
                },
                .u32, .i64 => {
                    try t.begin(10); // INTEGER
                    try t.bytef(1, if (col.type == .u32) 32 else 64);
                    try t.boolf(2, col.type == .i64);
                    try t.end();
                },
                .timestamp_us => {
                    try t.begin(8); // TIMESTAMP
                    try t.boolf(1, true); // isAdjustedToUTC
                    try t.begin(2); // unit
                    try t.begin(2); // MICROS
                    try t.end();
                    try t.end();
                    try t.end();
                },
            }
            try t.end();
            try t.end();
        }
        try t.i64f(3, w.rows);
        try t.listf(4, T.@"struct", w.groups.items.len);
        for (w.groups.items) |g| {
            t.beginElem();
            try t.listf(1, T.@"struct", table.columns.len);
            for (g.cols, table.columns) |c, col| {
                t.beginElem();
                try t.i64f(2, c.page_offset);
                try t.begin(3); // ColumnMetaData
                try t.i32f(1, physicalType(col.type));
                try t.listf(2, T.i32_, 2);
                try t.zigzag(enc_plain);
                try t.zigzag(enc_rle);
                try t.listf(3, T.binary, 1);
                try t.str(col.name);
                try t.i32f(4, codec_gzip);
                try t.i64f(5, c.num_values);
                try t.i64f(6, c.uncompressed);
                try t.i64f(7, c.compressed);
                try t.i64f(9, c.page_offset);
                try t.end();
                try t.end();
            }
            try t.i64f(2, g.bytes);
            try t.i64f(3, g.rows);
            try t.end();
        }
        try t.strf(6, "wtfs");
        try t.buf.append(w.a, 0);

        try w.raw(t.buf.items);
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, @intCast(t.buf.items.len), .little);
        try w.raw(&len);
        try w.raw("PAR1");
        try w.out.flush();
    }
};
