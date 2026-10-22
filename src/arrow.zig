//! Apache Arrow IPC file writer (the format also known as Feather v2).
//!
//! Layout: "ARROW1\0\0", a schema message, one record batch message per
//! batch, an end-of-stream marker, then a footer that indexes the batches.
//! Messages are FlatBuffers; the small builder below writes them front to
//! back and patches forward offsets.
const std = @import("std");
const table = @import("table.zig");

const Allocator = std.mem.Allocator;

// ===== Minimal FlatBuffers builder =====

const Scalar = union(enum) { u8: u8, bool: bool, i16: i16, i32: i32, i64: i64 };

const Field = union(enum) {
    none,
    scalar: Scalar,
    child: *const Node,
};

const Node = union(enum) {
    table: []const Field,
    string: []const u8,
    tables: []const *const Node,
    /// Vector of fixed-size structs whose fields are all 8-byte aligned.
    structs: struct { bytes: []const u8, count: u32 },
};

const Fb = struct {
    a: Allocator,
    buf: std.ArrayList(u8) = .empty,

    fn finish(fb: *Fb, root: *const Node) ![]const u8 {
        try fb.buf.appendNTimes(fb.a, 0, 4);
        const pos = try fb.write(root);
        fb.patch(0, pos);
        return fb.buf.items;
    }

    fn pad(fb: *Fb, alignment: usize) !void {
        const n = std.mem.alignForward(usize, fb.buf.items.len, alignment) - fb.buf.items.len;
        try fb.buf.appendNTimes(fb.a, 0, n);
    }

    fn put(fb: *Fb, comptime T: type, v: T) !void {
        var b: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &b, v, .little);
        try fb.buf.appendSlice(fb.a, &b);
    }

    /// Store at `at` the forward offset from `at` to `target`.
    fn patch(fb: *Fb, at: usize, target: usize) void {
        std.mem.writeInt(u32, fb.buf.items[at..][0..4], @intCast(target - at), .little);
    }

    fn write(fb: *Fb, node: *const Node) Allocator.Error!usize {
        switch (node.*) {
            .string => |s| {
                try fb.pad(4);
                const pos = fb.buf.items.len;
                try fb.put(u32, @intCast(s.len));
                try fb.buf.appendSlice(fb.a, s);
                try fb.buf.append(fb.a, 0);
                return pos;
            },
            .structs => |v| {
                // Elements must be 8-aligned; the length prefix sits just before.
                while ((fb.buf.items.len + 4) % 8 != 0) try fb.buf.append(fb.a, 0);
                const pos = fb.buf.items.len;
                try fb.put(u32, v.count);
                try fb.buf.appendSlice(fb.a, v.bytes);
                return pos;
            },
            .tables => |items| {
                try fb.pad(4);
                const pos = fb.buf.items.len;
                try fb.put(u32, @intCast(items.len));
                const slots = fb.buf.items.len;
                try fb.buf.appendNTimes(fb.a, 0, 4 * items.len);
                for (items, 0..) |item, i| {
                    const child = try fb.write(item);
                    fb.patch(slots + 4 * i, child);
                }
                return pos;
            },
            .table => |fields| return fb.writeTable(fields),
        }
    }

    fn writeTable(fb: *Fb, fields: []const Field) !usize {
        // Inline layout: soffset, then each present field at its alignment.
        var offsets: [16]u16 = @splat(0);
        var size: usize = 4;
        for (fields, 0..) |f, i| {
            const w = fieldWidth(f);
            if (w == 0) continue;
            size = std.mem.alignForward(usize, size, w);
            offsets[i] = @intCast(size);
            size += w;
        }
        size = std.mem.alignForward(usize, size, 4);

        // vtable first, so the table's soffset (table - vtable) is positive.
        try fb.pad(2);
        const vtable = fb.buf.items.len;
        try fb.put(u16, @intCast(4 + 2 * fields.len));
        try fb.put(u16, @intCast(size));
        for (fields, 0..) |_, i| try fb.put(u16, offsets[i]);
        try fb.pad(8);
        const start = fb.buf.items.len;
        try fb.put(i32, @intCast(start - vtable));
        try fb.buf.appendNTimes(fb.a, 0, size - 4);

        for (fields, 0..) |f, i| {
            const at = start + offsets[i];
            const dst = fb.buf.items[at..];
            switch (f) {
                .none => {},
                .scalar => |s| switch (s) {
                    .u8 => |v| dst[0] = v,
                    .bool => |v| dst[0] = @intFromBool(v),
                    .i16 => |v| std.mem.writeInt(i16, dst[0..2], v, .little),
                    .i32 => |v| std.mem.writeInt(i32, dst[0..4], v, .little),
                    .i64 => |v| std.mem.writeInt(i64, dst[0..8], v, .little),
                },
                .child => {},
            }
        }
        for (fields, 0..) |f, i| switch (f) {
            .child => |c| {
                const child = try fb.write(c);
                fb.patch(start + offsets[i], child);
            },
            else => {},
        };
        return start;
    }

    fn fieldWidth(f: Field) usize {
        return switch (f) {
            .none => 0,
            .child => 4,
            .scalar => |s| switch (s) {
                .u8, .bool => 1,
                .i16 => 2,
                .i32 => 4,
                .i64 => 8,
            },
        };
    }
};

// ===== Arrow schema =====

const metadata_v5: i16 = 4;
const header_schema: u8 = 1;
const header_record_batch: u8 = 3;
const type_int: u8 = 2;
const type_utf8: u8 = 5;
const type_timestamp: u8 = 10;
const unit_microsecond: i16 = 2;

fn schemaNode(a: Allocator) !*const Node {
    const fields = try a.alloc(*const Node, table.columns.len);
    for (table.columns, fields) |col, *out| {
        const type_id: u8, const type_node: Node = switch (col.type) {
            .utf8 => .{ type_utf8, .{ .table = &.{} } },
            .i64 => .{ type_int, .{ .table = try a.dupe(Field, &.{ .{ .scalar = .{ .i32 = 64 } }, .{ .scalar = .{ .bool = true } } }) } },
            .u32 => .{ type_int, .{ .table = try a.dupe(Field, &.{ .{ .scalar = .{ .i32 = 32 } }, .{ .scalar = .{ .bool = false } } }) } },
            .timestamp_us => .{ type_timestamp, .{ .table = try a.dupe(Field, &.{
                .{ .scalar = .{ .i16 = unit_microsecond } },
                .{ .child = try create(a, .{ .string = "UTC" }) },
            }) } },
        };
        out.* = try create(a, .{
            .table = try a.dupe(Field, &.{
                .{ .child = try create(a, .{ .string = col.name }) }, // name
                .{ .scalar = .{ .bool = col.nullable } }, // nullable
                .{ .scalar = .{ .u8 = type_id } }, // type_type
                .{ .child = try create(a, type_node) }, // type
                .none, // dictionary
                .{ .child = try create(a, .{ .tables = &.{} }) }, // children
            }),
        });
    }
    return create(a, .{
        .table = try a.dupe(Field, &.{
            .{ .scalar = .{ .i16 = 0 } }, // endianness: little
            .{ .child = try create(a, .{ .tables = fields }) },
        }),
    });
}

fn create(a: Allocator, node: Node) !*const Node {
    const p = try a.create(Node);
    p.* = node;
    return p;
}

fn messageNode(a: Allocator, header_type: u8, header: *const Node, body_len: i64) !*const Node {
    return create(a, .{ .table = try a.dupe(Field, &.{
        .{ .scalar = .{ .i16 = metadata_v5 } },
        .{ .scalar = .{ .u8 = header_type } },
        .{ .child = header },
        .{ .scalar = .{ .i64 = body_len } },
    }) });
}

// ===== File writer =====

const Block = struct { offset: i64, meta_len: i32, body_len: i64 };

pub const Writer = struct {
    a: Allocator,
    out: *std.Io.Writer,
    pos: u64 = 0,
    blocks: std.ArrayList(Block) = .empty,
    arena: std.heap.ArenaAllocator,

    pub fn init(a: Allocator, out: *std.Io.Writer) !Writer {
        var w: Writer = .{ .a = a, .out = out, .arena = .init(a) };
        try w.raw("ARROW1\x00\x00");
        const schema = try schemaNode(w.arena.allocator());
        _ = try w.message(try messageNode(w.arena.allocator(), header_schema, schema, 0));
        return w;
    }

    pub fn deinit(w: *Writer) void {
        w.blocks.deinit(w.a);
        w.arena.deinit();
    }

    fn raw(w: *Writer, bytes: []const u8) !void {
        try w.out.writeAll(bytes);
        w.pos += bytes.len;
    }

    fn zeros(w: *Writer, n: usize) !void {
        const z: [8]u8 = @splat(0);
        var left = n;
        while (left > 0) {
            const k = @min(left, z.len);
            try w.raw(z[0..k]);
            left -= k;
        }
    }

    /// Write an encapsulated message (continuation, length, flatbuffer,
    /// padding); returns the total metadata length.
    fn message(w: *Writer, node: *const Node) !i32 {
        var fb: Fb = .{ .a = w.arena.allocator() };
        const bytes = try fb.finish(node);
        const padded = std.mem.alignForward(usize, bytes.len, 8);
        var hdr: [8]u8 = undefined;
        std.mem.writeInt(u32, hdr[0..4], 0xFFFFFFFF, .little);
        std.mem.writeInt(i32, hdr[4..8], @intCast(padded), .little);
        try w.raw(&hdr);
        try w.raw(bytes);
        try w.zeros(padded - bytes.len);
        return @intCast(8 + padded);
    }

    pub fn writeBatch(w: *Writer, batch: *const table.Batch) !void {
        const a = w.arena.allocator();
        defer _ = w.arena.reset(.retain_capacity);

        // Describe buffers: (validity, [offsets,] values) per column.
        var nodes: std.ArrayList(u8) = .empty;
        var bufs: std.ArrayList(u8) = .empty;
        var parts: std.ArrayList([]const u8) = .empty;
        var body_len: u64 = 0;
        for (&batch.cols, table.columns) |*c, col| {
            try appendI64s(a, &nodes, &.{ @intCast(batch.len), @intCast(c.null_count) });
            const validity: []const u8 = if (c.null_count > 0) c.validity.items else &.{};
            const offsets: []const u8 = std.mem.sliceAsBytes(c.offsets.items);
            const items: []const []const u8 = if (col.type == .utf8)
                &.{ validity, offsets, c.values.items }
            else
                &.{ validity, c.values.items };
            for (items) |buf| {
                try appendI64s(a, &bufs, &.{ @intCast(body_len), @intCast(buf.len) });
                try parts.append(a, buf);
                body_len += std.mem.alignForward(u64, buf.len, 8);
            }
        }

        const record_batch = try create(a, .{ .table = try a.dupe(Field, &.{
            .{ .scalar = .{ .i64 = @intCast(batch.len) } },
            .{ .child = try create(a, .{ .structs = .{ .bytes = nodes.items, .count = @intCast(nodes.items.len / 16) } }) },
            .{ .child = try create(a, .{ .structs = .{ .bytes = bufs.items, .count = @intCast(bufs.items.len / 16) } }) },
        }) });

        const offset = w.pos;
        const meta_len = try w.message(try messageNode(a, header_record_batch, record_batch, @intCast(body_len)));
        for (parts.items) |buf| {
            try w.raw(buf);
            try w.zeros(std.mem.alignForward(usize, buf.len, 8) - buf.len);
        }
        try w.blocks.append(w.a, .{ .offset = @intCast(offset), .meta_len = meta_len, .body_len = @intCast(body_len) });
    }

    pub fn finish(w: *Writer) !void {
        const a = w.arena.allocator();
        try w.raw(&.{ 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0 }); // end of stream

        var blocks: std.ArrayList(u8) = .empty;
        for (w.blocks.items) |b| {
            try appendI64s(a, &blocks, &.{b.offset});
            var mid: [8]u8 = @splat(0);
            std.mem.writeInt(i32, mid[0..4], b.meta_len, .little);
            try blocks.appendSlice(a, &mid);
            try appendI64s(a, &blocks, &.{b.body_len});
        }
        const footer = try create(a, .{ .table = try a.dupe(Field, &.{
            .{ .scalar = .{ .i16 = metadata_v5 } },
            .{ .child = try schemaNode(a) },
            .{ .child = try create(a, .{ .structs = .{ .bytes = &.{}, .count = 0 } }) },
            .{ .child = try create(a, .{ .structs = .{ .bytes = blocks.items, .count = @intCast(w.blocks.items.len) } }) },
        }) });
        var fb: Fb = .{ .a = a };
        const bytes = try fb.finish(footer);
        try w.raw(bytes);
        var len: [4]u8 = undefined;
        std.mem.writeInt(i32, &len, @intCast(bytes.len), .little);
        try w.raw(&len);
        try w.raw("ARROW1");
        try w.out.flush();
    }
};

fn appendI64s(a: Allocator, list: *std.ArrayList(u8), vals: []const i64) !void {
    for (vals) |v| {
        var b: [8]u8 = undefined;
        std.mem.writeInt(i64, &b, v, .little);
        try list.appendSlice(a, &b);
    }
}
