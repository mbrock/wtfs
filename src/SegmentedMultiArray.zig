const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ShiftInt = std.math.Log2Int(usize);
const MAX_SHELVES = @bitSizeOf(usize);
const builtin = @import("builtin");
const test_threads = @import("test_threading.zig");

/// Shared storage for fabricating pointers to zero-sized fields.
var zst_sentinel_byte: u8 = 0;

/// Segmented (paged) struct-of-arrays container tailored for append-heavy tables.
/// Shelves are grown geometrically which keeps pointers stable while providing
/// cache-friendly access to individual fields. `PREALLOC` may be 0 (no inline
/// storage) or a power of two describing the size of the first shelf.
pub fn SegmentedMultiArray(comptime T: type, comptime PREALLOC: usize) type {
    return struct {
        const Self = @This();

        // ----- Elem/Field machinery (mirrors std.MultiArrayList) -----
        const Elem = switch (@typeInfo(T)) {
            .@"struct" => T,
            .@"union" => |u| struct {
                pub const Bare = @Type(.{ .@"union" = .{
                    .layout = u.layout,
                    .tag_type = null,
                    .fields = u.fields,
                    .decls = &.{},
                } });
                pub const Tag = u.tag_type orelse
                    @compileError("SegmentedMultiArray does not support untagged unions");
                tags: Tag,
                data: Bare,

                pub fn fromT(outer: T) @This() {
                    const tag = meta.activeTag(outer);
                    return .{
                        .tags = tag,
                        .data = switch (tag) {
                            inline else => |t| @unionInit(Bare, @tagName(t), @field(outer, @tagName(t))),
                        },
                    };
                }

                pub fn toT(tag: Tag, bare: Bare) T {
                    return switch (tag) {
                        inline else => |t| @unionInit(T, @tagName(t), @field(bare, @tagName(t))),
                    };
                }
            },
            else => @compileError("SegmentedMultiArray only supports structs and tagged unions"),
        };

        pub const Field = meta.FieldEnum(Elem);
        const fields = meta.fields(Elem);

        fn FieldType(comptime f: Field) type {
            return @FieldType(Elem, @tagName(f));
        }
        fn FieldTypeI(comptime i: usize) type {
            return @FieldType(Elem, fields[i].name);
        }

        // ----- Storage: per-field shelves (each shelf is a contiguous []FieldType) -----
        const ShelfEntry = struct {
            addr: usize = 0,
        };

        shelves: [fields.len][MAX_SHELVES]ShelfEntry = mem.zeroes([fields.len][MAX_SHELVES]ShelfEntry),
        len: std.atomic.Value(usize) = .init(0), // logical rows used
        cap: usize = 0, // logical rows addressable (only grower mutates)
        shelf_count: std.atomic.Value(usize) = .init(0),
        grow_token: std.atomic.Value(u8) = .init(0), // 0 = free, 1 = owned

        pub const empty: Self = .{};

        /// Releases all allocated shelves using `alloc` and leaves `self` undefined.
        /// Existing field pointers become invalid once this returns.
        pub fn deinit(self: *Self, alloc: Allocator) void {
            const shelf_count = self.shelf_count.load(.acquire);
            inline for (fields, 0..) |f, fi| {
                if (@sizeOf(f.type) == 0) continue;
                for (0..shelf_count) |s| {
                    const addr = self.shelves[fi][s].addr;
                    if (addr == 0) continue;
                    const rows = shelfSizeByIndex(@intCast(s));
                    const base: [*]FieldTypeI(fi) = @as([*]FieldTypeI(fi), @ptrFromInt(addr));
                    alloc.free(base[0..rows]);
                    self.shelves[fi][s].addr = 0;
                }
            }
            self.* = undefined;
        }

        // ----------------- Capacity / growth -----------------

        /// Ensures that at least `new_cap` logical rows can be addressed without changing `len`.
        /// Returns `error.OutOfMemory` if a shelf allocation fails.
        pub fn ensureTotalCapacity(self: *Self, alloc: Allocator, new_cap: usize) !void {
            const cap_now = @atomicLoad(usize, &self.cap, .acquire);
            if (new_cap <= cap_now) return;
            try self.growTo(alloc, new_cap);
        }

        /// Ensures that at least `additional` unused rows are available past the current length.
        /// Returns `error.OutOfMemory` if new shelves cannot be allocated.
        pub fn ensureUnusedCapacity(self: *Self, alloc: Allocator, additional: usize) !void {
            const len_now = self.len.load(.acquire);
            const want = try std.math.add(usize, len_now, additional);
            try self.ensureTotalCapacity(alloc, want);
        }

        /// Grows or shrinks the logical length to `new_len`, allocating shelves when necessary.
        pub fn resize(self: *Self, alloc: Allocator, new_len: usize) !void {
            try self.ensureTotalCapacity(alloc, new_len);
            self.len.store(new_len, .release);
        }

        /// Appends a new row, extending capacity when required, and returns its index.
        /// Returns `error.OutOfMemory` when growth fails.
        pub fn addOne(self: *Self, alloc: Allocator) !usize {
            return self.reserveBlock(alloc, 1);
        }

        /// Reserve `count` consecutive rows and return the base index.
        /// Ensures capacity exactly once for the entire block.
        pub fn reserveBlock(self: *Self, alloc: Allocator, count: usize) !usize {
            std.debug.assert(count > 0);
            while (true) {
                const base = self.len.load(.acquire);
                const want = try std.math.add(usize, base, count);
                try self.ensureTotalCapacity(alloc, want);
                if (self.len.cmpxchgStrong(base, want, .acq_rel, .acquire) == null) {
                    return base;
                }
                std.atomic.spinLoopHint();
            }
        }

        /// Appends a new row assuming an unused slot is already available.
        /// The caller must have ensured `self.len < self.cap` via
        /// `ensureTotalCapacity`, `ensureUnusedCapacity`, or prior growth.
        pub fn addOneAssumeCapacity(self: *Self) usize {
            const idx = self.len.fetchAdd(1, .acq_rel);
            std.debug.assert(idx < @atomicLoad(usize, &self.cap, .acquire));
            return idx;
        }

        fn growTo(self: *Self, alloc: Allocator, want_cap: usize) !void {
            while (@atomicLoad(usize, &self.cap, .acquire) < want_cap) {
                if (self.grow_token.cmpxchgWeak(0, 1, .acq_rel, .acquire) == null) {
                    defer self.grow_token.store(0, .release);

                    while (self.cap < want_cap) {
                        try self.growOneShelf(alloc);
                    }
                } else {
                    const before = self.shelf_count.load(.acquire);
                    while (@atomicLoad(usize, &self.cap, .acquire) < want_cap and
                        self.shelf_count.load(.acquire) == before)
                    {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }

        fn growOneShelf(self: *Self, alloc: Allocator) !void {
            const s = self.shelf_count.load(.monotonic);
            if (s >= MAX_SHELVES) return error.OutOfMemory;

            const rows = shelfSizeByIndex(@intCast(s));
            inline for (fields, 0..) |f, fi| {
                if (@sizeOf(f.type) == 0) continue;
                const buf = try alloc.alloc(f.type, rows);
                self.shelves[fi][s].addr = @intFromPtr(buf.ptr);
            }

            self.cap += rows;
            self.shelf_count.store(s + 1, .release);
        }

        // ----------------- Indexing math (shared by all fields) -----------------

        inline fn log2_floor(x: usize) ShiftInt {
            // precondition: x > 0
            return @intCast(@bitSizeOf(usize) - 1 - @clz(x));
        }

        inline fn shelfIndex(idx: usize) ShiftInt {
            if (PREALLOC == 0) {
                // floor(log2(idx+1))
                return log2_floor(idx + 1);
            } else {
                const pre_log2: ShiftInt = std.math.log2_int(usize, PREALLOC);
                const units = (idx >> pre_log2) + 1;
                return log2_floor(units);
            }
        }

        inline fn boxIndex(idx: usize, shelf_idx: ShiftInt) usize {
            if (PREALLOC == 0) {
                // idx + 1 - 2^s
                const start = (@as(usize, 1) << shelf_idx) - 1;
                return idx - start;
            } else {
                const shelf_factor = (@as(usize, 1) << shelf_idx);
                const start = PREALLOC * (shelf_factor - 1);
                return idx - start;
            }
        }

        inline fn shelfSizeByIndex(shelf_idx: ShiftInt) usize {
            if (PREALLOC == 0) {
                return (@as(usize, 1) << shelf_idx);
            } else {
                return PREALLOC * (@as(usize, 1) << shelf_idx);
            }
        }

        // ----------------- Field access -----------------

        /// Pointer to field `field` at logical row `idx`.
        /// Returns a pointer to `field` at logical row `idx`.
        /// Pointers remain valid across future growth because shelves are never relocated.
        pub fn ptr(self: *Self, comptime field: Field, idx: usize) *FieldType(field) {
            std.debug.assert(idx < self.len.load(.acquire));
            const fi = @intFromEnum(field);
            const sb = shelfIndex(idx);
            const bi = boxIndex(idx, sb);

            if (@sizeOf(FieldType(field)) == 0) {
                // ZST: return address of a shared sentinel byte.
                return @ptrCast(&zst_sentinel_byte);
            }

            const shelf_pos: usize = @intCast(sb);
            const entry = self.shelves[fi][shelf_pos];
            std.debug.assert(entry.addr != 0);
            const base: [*]FieldType(field) = @as([*]FieldType(field), @ptrFromInt(entry.addr));
            return &base[bi];
        }

        /// Read the full element at `idx` by gathering from each field.
        /// Materializes the value at row `idx` by gathering every field into a `T`.
        pub fn get(self: *Self, idx: usize) T {
            std.debug.assert(idx < self.len.load(.acquire));
            var e: Elem = undefined;
            inline for (fields, 0..) |f, fi| {
                if (@sizeOf(f.type) != 0) {
                    const sb = shelfIndex(idx);
                    const bi = boxIndex(idx, sb);
                    const shelf_pos: usize = @intCast(sb);
                    const entry = self.shelves[fi][shelf_pos];
                    std.debug.assert(entry.addr != 0);
                    const base: [*]FieldTypeI(fi) = @as([*]FieldTypeI(fi), @ptrFromInt(entry.addr));
                    @field(e, f.name) = base[bi];
                } else {
                    // ZST: leave default
                }
            }
            return switch (@typeInfo(T)) {
                .@"struct" => e,
                .@"union" => Elem.toT(e.tags, e.data),
                else => unreachable,
            };
        }

        /// Write full element `elem` to logical row `idx`.
        /// Overwrites the row at `idx` with `elem`.
        /// Existing pointers to the same row remain valid.
        pub fn set(self: *Self, idx: usize, elem: T) void {
            std.debug.assert(idx < self.len.load(.acquire));
            const e = switch (@typeInfo(T)) {
                .@"struct" => elem,
                .@"union" => Elem.fromT(elem),
                else => unreachable,
            };
            const sb = shelfIndex(idx);
            const bi = boxIndex(idx, sb);
            inline for (fields, 0..) |f, fi| {
                if (@sizeOf(f.type) == 0) continue;
                const shelf_pos: usize = @intCast(sb);
                const entry = self.shelves[fi][shelf_pos];
                std.debug.assert(entry.addr != 0);
                const base: [*]FieldTypeI(fi) = @as([*]FieldTypeI(fi), @ptrFromInt(entry.addr));
                base[bi] = @field(e, f.name);
            }
        }

        /// A segmented accessor view to a single field (random-access + iterator).
        /// Returns a segmented view that allows random access and iteration over `field`.
        /// The view reflects subsequent writes made through the parent array.
        pub fn itemsSeg(self: *Self, comptime field: Field) SegView(field) {
            return .{ .parent = self };
        }

        pub fn slices(self: *Self) SliceView {
            const len = self.len.load(.acquire);
            return .{ .parent = self, .len = len };
        }

        /// Compact into a contiguous std.MultiArrayList(T).
        /// Copies all rows into a contiguous `std.MultiArrayList(T)`.
        /// Each shelf is copied in order; callers own the returned list and must `deinit` it.
        pub fn toMultiArrayList(self: *Self, alloc: Allocator) !std.MultiArrayList(T) {
            var mal: std.MultiArrayList(T) = .{};
            const len_snapshot = self.len.load(.acquire);
            try mal.ensureTotalCapacity(alloc, len_snapshot);
            mal.len = len_snapshot;
            var out = mal.slice();
            inline for (fields, 0..) |f, fi| {
                if (@sizeOf(f.type) == 0) continue;
                const dst = out.items(@enumFromInt(fi));
                const elem_sz = @sizeOf(f.type);
                const dst_bytes = mem.sliceAsBytes(dst);
                var off: usize = 0;
                const shelf_count = self.shelf_count.load(.acquire);
                for (0..shelf_count) |s| {
                    if (off >= len_snapshot) break;
                    const entry = self.shelves[fi][s];
                    if (entry.addr == 0) continue;
                    const rows = shelfSizeByIndex(@intCast(s));
                    const remaining = len_snapshot - off;
                    const take = @min(rows, remaining);
                    if (take == 0) continue;
                    const base: [*]FieldTypeI(fi) = @as([*]FieldTypeI(fi), @ptrFromInt(entry.addr));
                    const src = base[0..take];
                    const byte_off = off * elem_sz;
                    const byte_take = take * elem_sz;
                    const src_bytes = mem.sliceAsBytes(src)[0..byte_take];
                    @memcpy(dst_bytes[byte_off .. byte_off + byte_take], src_bytes);
                    off += take;
                }
            }
            return mal;
        }

        // ----------------- Segmented field view -----------------

        pub const SliceView = struct {
            parent: *Self,
            len: usize,
            data: [fields.len][MAX_SHELVES][]u8,

            pub fn init(self: *Self) @This() {
                inline for (0..fields.len) |f| {
                    for (0..Self.shelfIndex(self.len - 1) + 1) |s| {
                        const entry = self.parent.shelves[f][s];
                        if (entry.addr == 0 or @sizeOf(FieldTypeI(f)) == 0) {
                            self.data[f][s] = &.{};
                        } else {
                            const rows = Self.shelfSizeByIndex(@intCast(s));
                            const base: [*]FieldTypeI(f) = @as([*]FieldTypeI(f), @ptrFromInt(entry.addr));
                            self.data[f][s] = base[0..rows];
                        }
                    }
                }
            }

            /// Returns the number of logical elements visible to the view.
            pub fn lenItems(self: @This()) usize {
                return self.len;
            }

            /// Returns the segmented array as a slice of slices, one per shelf.
            pub fn shelves(self: @This(), comptime field: Field) [][]FieldType(field) {
                const FT = FieldType(field);
                if (@sizeOf(FT) == 0) {
                    return &.{};
                }

                var shelfslices: [MAX_SHELVES][]FT = undefined;
                for (0..Self.shelfIndex(self.len - 1)) |i| {
                    var entries: [fields.len]ShelfEntry = undefined;
                    inline for (0..fields.len) |fi| {
                        entries[fi] = self.parent.shelves[fi][i];
                    }
                    if (entries[0].addr == 0) {
                        shelfslices[i] = &.{};
                    } else {
                        const rows = Self.shelfSizeByIndex(@intCast(i));
                        const base: [*]T = @as([*]T, @ptrFromInt(entries[0].addr));
                        shelfslices[i] = base[0..rows];
                    }
                }
                return shelfslices[0 .. Self.shelfIndex(self.len - 1) + 1];
            }
        };

        /// Typed segmented field view used by `itemsSeg`.
        pub fn SegView(comptime field: Field) type {
            const ViewedFieldType = FieldType(field);
            const field_index = @intFromEnum(field);
            return struct {
                parent: *Self,

                /// Returns the number of logical elements visible to the view.
                pub fn lenItems(self: @This()) usize {
                    return self.parent.len.load(.acquire);
                }

                /// Returns the `idx`th element from the segmented field.
                /// Zero-sized field types read as their zero value.
                pub fn get(self: @This(), idx: usize) ViewedFieldType {
                    std.debug.assert(idx < self.parent.len.load(.acquire));
                    if (@sizeOf(ViewedFieldType) == 0) return mem.zeroes(ViewedFieldType);
                    const s = Self.shelfIndex(idx);
                    const b = Self.boxIndex(idx, s);
                    const entry = self.parent.shelves[field_index][@intCast(s)];
                    std.debug.assert(entry.addr != 0);
                    const base: [*]ViewedFieldType = @as([*]ViewedFieldType, @ptrFromInt(entry.addr));
                    return base[b];
                }

                /// Returns a pointer to the `idx`th element in the segmented field.
                /// Pointers remain valid for the lifetime of the parent array.
                pub fn ptr(self: @This(), idx: usize) *ViewedFieldType {
                    std.debug.assert(idx < self.parent.len.load(.acquire));
                    if (@sizeOf(ViewedFieldType) == 0) {
                        return @ptrCast(&zst_sentinel_byte);
                    }
                    const s = Self.shelfIndex(idx);
                    const b = Self.boxIndex(idx, s);
                    const entry = self.parent.shelves[field_index][@intCast(s)];
                    std.debug.assert(entry.addr != 0);
                    const base: [*]ViewedFieldType = @as([*]ViewedFieldType, @ptrFromInt(entry.addr));
                    return &base[b];
                }

                /// Returns an iterator that walks the view in row order across shelves.
                /// Zero-sized field types yield their zero value on each iteration.
                pub fn iterator(self: @This()) SegIter {
                    const len_snapshot = self.parent.len.load(.acquire);
                    return .{
                        .parent = self.parent,
                        .field_index = field_index,
                        .len = len_snapshot,
                        .i = 0,
                        .shelf_idx = 0,
                        .box_idx = 0,
                        .shelf_len = Self.shelfSizeByIndex(0),
                    };
                }

                /// Iterator over a segmented field view.
                pub const SegIter = struct {
                    parent: *Self,
                    field_index: usize,
                    len: usize,
                    i: usize,
                    shelf_idx: usize,
                    box_idx: usize,
                    shelf_len: usize,

                    /// Returns the next value or null when iteration completes.
                    pub fn next(it: *@This()) ?ViewedFieldType {
                        if (it.i >= it.len) return null;
                        const shelf_count = it.parent.shelf_count.load(.acquire);
                        std.debug.assert(it.shelf_idx < shelf_count or it.len == 0);
                        const val = if (@sizeOf(ViewedFieldType) == 0)
                            mem.zeroes(ViewedFieldType)
                        else blk: {
                            const entry = it.parent.shelves[it.field_index][it.shelf_idx];
                            std.debug.assert(entry.addr != 0);
                            const base: [*]ViewedFieldType = @as([*]ViewedFieldType, @ptrFromInt(entry.addr));
                            break :blk base[it.box_idx];
                        };

                        it.i += 1;
                        it.box_idx += 1;
                        if (it.box_idx == it.shelf_len) {
                            it.shelf_idx += 1;
                            it.box_idx = 0;
                            if (it.shelf_idx < shelf_count) {
                                it.shelf_len = Self.shelfSizeByIndex(@intCast(it.shelf_idx));
                            }
                        }
                        return val;
                    }
                };
            };
        }
    };
}

// ----------------- TESTS -----------------

test "SegmentedMultiArray basic struct usage, PREALLOC=0" {
    const Foo = struct { a: u32, b: u8 };
    var sma = SegmentedMultiArray(Foo, 0){};
    defer sma.deinit(testing.allocator);

    try sma.ensureTotalCapacity(testing.allocator, 2);
    {
        const idx0 = try sma.addOne(testing.allocator);
        sma.set(idx0, .{ .a = 1, .b = 2 });
        const idx1 = try sma.addOne(testing.allocator);
        sma.set(idx1, .{ .a = 3, .b = 4 });
    }

    try testing.expectEqual(@as(usize, 2), sma.len.load(.acquire));
    try testing.expectEqual(@as(u32, 1), sma.get(0).a);
    try testing.expectEqual(@as(u8, 4), sma.get(1).b);

    // Trigger growth by several shelves
    for (2..20) |_| {
        const i = try sma.addOne(testing.allocator);
        sma.set(i, .{ .a = @intCast(i + 10), .b = @intCast((i % 250) + 1) });
    }

    try testing.expectEqual(@as(usize, 20), sma.len.load(.acquire));
    try testing.expectEqual(@as(u32, 10 + 19), sma.get(19).a);

    // Field view
    var a_view = sma.itemsSeg(.a);
    var it = a_view.iterator();
    var sum: usize = 0;
    while (it.next()) |v| sum += v;
    // 1 + 3 + (10..29)
    try testing.expect(sum > 0);
}

test "SegmentedMultiArray PREALLOC=8 (power-of-two), set/get/ptr()" {
    const Bar = struct {
        x: u64,
        y: bool,
        z: u16,
    };
    var sma = SegmentedMultiArray(Bar, 8){};
    defer sma.deinit(testing.allocator);

    try sma.resize(testing.allocator, 12);
    // Write via ptr(.field, idx)
    for (0..12) |i| {
        sma.ptr(.x, i).* = @intCast(100 + i);
        sma.ptr(.y, i).* = (i % 2 == 0);
        sma.ptr(.z, i).* = @intCast(i * 7);
    }

    // spot checks
    try testing.expectEqual(@as(u64, 100), sma.get(0).x);
    try testing.expectEqual(true, sma.get(10).y);
    try testing.expectEqual(@as(u16, 77), sma.get(11).z);

    // Compact to std.MultiArrayList and verify
    var mal = try sma.toMultiArrayList(testing.allocator);
    defer mal.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 12), mal.items(.x).len);
    try testing.expectEqual(@as(u64, 100), mal.items(.x)[0]);
    try testing.expectEqual(@as(u16, 77), mal.items(.z)[11]);
}

test "SegmentedMultiArray growth by addOne, many rows" {
    const P = struct { a: u32, b: u32, c: u32, d: u8 };
    var sma = SegmentedMultiArray(P, 16){};
    defer sma.deinit(testing.allocator);

    for (0..1000) |i| {
        const idx = try sma.addOne(testing.allocator);
        sma.set(idx, .{
            .a = @intCast(i),
            .b = @intCast(2 * i),
            .c = @intCast(3 * i),
            .d = @intCast(@min(255, i)),
        });
    }

    try testing.expectEqual(@as(usize, 1000), sma.len.load(.acquire));
    try testing.expectEqual(@as(u32, 300), sma.get(100).c);
    try testing.expectEqual(@as(u8, 255), sma.get(999).d);
}

test "SegmentedMultiArray tagged union roundtrip" {
    const Message = union(enum) {
        value: u16,
        active: bool,
    };
    const SmaType = SegmentedMultiArray(Message, 0);
    const Tag = SmaType.Elem.Tag;
    var sma = SmaType{};
    defer sma.deinit(testing.allocator);

    const ix0 = try sma.addOne(testing.allocator);
    sma.set(ix0, .{ .value = 42 });
    const ix1 = try sma.addOne(testing.allocator);
    sma.set(ix1, .{ .active = true });

    try testing.expectEqual(@as(usize, 2), sma.len.load(.acquire));
    try testing.expectEqual(Tag.value, sma.ptr(.tags, ix0).*);
    try testing.expectEqual(Tag.active, sma.ptr(.tags, ix1).*);

    const first = sma.get(ix0);
    try testing.expectEqual(Tag.value, meta.activeTag(first));
    try testing.expectEqual(@as(u16, 42), switch (first) {
        .value => |payload| payload,
        else => unreachable,
    });
    const second = sma.get(ix1);
    try testing.expectEqual(Tag.active, meta.activeTag(second));
    try testing.expectEqual(true, switch (second) {
        .active => |flag| flag,
        else => unreachable,
    });

    var tags_view = sma.itemsSeg(.tags);
    try testing.expectEqual(@as(usize, 2), tags_view.lenItems());
    try testing.expectEqual(Tag.value, tags_view.get(0));
    try testing.expectEqual(Tag.active, tags_view.get(1));

    var mal = try sma.toMultiArrayList(testing.allocator);
    defer mal.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 2), mal.items(.tags).len);
    const mal_tags = mal.items(.tags);
    const mal_data = mal.items(.data);
    try testing.expectEqual(Tag.value, mal_tags[0]);
    const bare0 = switch (mal_tags[0]) {
        .value => @unionInit(SmaType.Elem.Bare, "value", mal_data[0].value),
        .active => @unionInit(SmaType.Elem.Bare, "active", mal_data[0].active),
    };
    const elem0 = SmaType.Elem.toT(mal_tags[0], bare0);
    try testing.expectEqual(@as(u16, 42), switch (elem0) {
        .value => |payload| payload,
        else => unreachable,
    });
    const bare1 = switch (mal_tags[1]) {
        .value => @unionInit(SmaType.Elem.Bare, "value", mal_data[1].value),
        .active => @unionInit(SmaType.Elem.Bare, "active", mal_data[1].active),
    };
    const elem1 = SmaType.Elem.toT(mal_tags[1], bare1);
    try testing.expectEqual(true, switch (elem1) {
        .active => |flag| flag,
        else => unreachable,
    });
}

test "SegmentedMultiArray zero-sized fields and segmented iteration" {
    const Stamp = struct {
        id: u64,
        marker: void,
        valid: bool,
    };
    var sma = SegmentedMultiArray(Stamp, 4){};
    defer sma.deinit(testing.allocator);

    try sma.resize(testing.allocator, 9);
    const len_snapshot = sma.len.load(.acquire);
    for (0..len_snapshot) |i| {
        sma.ptr(.id, i).* = @intCast(200 + i);
        sma.ptr(.valid, i).* = (i % 2 == 0);
        _ = sma.ptr(.marker, i);
    }

    for (0..len_snapshot) |i| {
        const row = sma.get(i);
        try testing.expectEqual(@as(u64, 200 + i), row.id);
        try testing.expectEqual((i % 2 == 0), row.valid);
        _ = row.marker;
    }

    var marker_view = sma.itemsSeg(.marker);
    try testing.expectEqual(len_snapshot, marker_view.lenItems());
    var marker_iter = marker_view.iterator();
    var count: usize = 0;
    while (marker_iter.next()) |_| count += 1;
    try testing.expectEqual(len_snapshot, count);

    var mal = try sma.toMultiArrayList(testing.allocator);
    defer mal.deinit(testing.allocator);
    try testing.expectEqual(len_snapshot, mal.items(.id).len);
    try testing.expectEqual(@as(u64, 200), mal.items(.id)[0]);
    try testing.expectEqual(len_snapshot, mal.items(.marker).len);
}

test "SegmentedMultiArray segmented view pointer edits" {
    const Row = struct { primary: i64, spare: u8 };
    var sma = SegmentedMultiArray(Row, 0){};
    defer sma.deinit(testing.allocator);

    try sma.ensureUnusedCapacity(testing.allocator, 24);
    for (0..24) |i| {
        const idx = sma.addOneAssumeCapacity();
        sma.set(idx, .{
            .primary = @intCast(i),
            .spare = @intCast(i % 200),
        });
    }

    var primary_view = sma.itemsSeg(.primary);
    const len_snapshot = sma.len.load(.acquire);
    try testing.expectEqual(len_snapshot, primary_view.lenItems());
    for (0..primary_view.lenItems()) |i| {
        primary_view.ptr(i).* += 1;
    }

    for (0..len_snapshot) |i| {
        const base: i64 = @as(i64, @intCast(i));
        try testing.expectEqual(base + 1, sma.get(i).primary);
    }

    var iter = primary_view.iterator();
    var sum: i128 = 0;
    while (iter.next()) |val| {
        sum += @as(i128, val);
    }
    try testing.expect(sum > 0);
}

test "SegmentedMultiArray concurrent reserveBlock" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const Row = struct { writer: usize, value: usize };
    var thread_safe_allocator = std.heap.ThreadSafeAllocator{
        .child_allocator = testing.allocator,
    };
    const alloc = thread_safe_allocator.allocator();

    var sma = SegmentedMultiArray(Row, 0){};
    defer sma.deinit(alloc);

    const thread_count = 6;
    const block = 1024;
    const total = thread_count * block;

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(
            thread_id: usize,
            array: *SegmentedMultiArray(Row, 0),
            allocator: Allocator,
            block_count: usize,
        ) void {
            const base = array.reserveBlock(allocator, block_count) catch |err| {
                std.debug.panic("reserveBlock failed: {s}", .{@errorName(err)});
            };

            for (0..block_count) |offset| {
                const idx = base + offset;
                array.ptr(.writer, idx).* = thread_id;
                array.ptr(.value, idx).* = idx;
            }
        }
    };

    try group.spawnMany(6, Worker.run, .{ &sma, alloc, block });
    group.wait();

    try testing.expectEqual(@as(usize, total), sma.len.load(.acquire));

    var seen = try testing.allocator.alloc(bool, total);
    defer testing.allocator.free(seen);
    @memset(seen, false);

    var per_writer = [_]usize{0} ** thread_count;

    for (0..total) |i| {
        const row = sma.get(i);
        try testing.expect(row.value < total);
        try testing.expect(!seen[row.value]);
        seen[row.value] = true;
        try testing.expect(row.writer < thread_count);
        per_writer[row.writer] += 1;
    }

    for (per_writer) |count| {
        try testing.expectEqual(block, count);
    }

    for (seen) |flag| try testing.expect(flag);
}

test "SegmentedMultiArray concurrent addOne" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const Row = struct { writer: usize, value: usize };
    var thread_safe_allocator = std.heap.ThreadSafeAllocator{
        .child_allocator = testing.allocator,
    };
    const alloc = thread_safe_allocator.allocator();

    var sma = SegmentedMultiArray(Row, 16){};
    defer sma.deinit(alloc);

    const thread_count = 4;
    const per_thread = 2000;
    const total = thread_count * per_thread;

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(
            thread_id: usize,
            array: *SegmentedMultiArray(Row, 16),
            allocator: Allocator,
            repeats: usize,
        ) void {
            for (0..repeats) |_| {
                const idx = array.addOne(allocator) catch |err| {
                    std.debug.panic("addOne failed: {s}", .{@errorName(err)});
                };
                array.ptr(.writer, idx).* = thread_id;
                array.ptr(.value, idx).* = idx;
            }
        }
    };

    try group.spawnMany(thread_count, Worker.run, .{
        &sma,
        alloc,
        per_thread,
    });

    group.wait();

    try testing.expectEqual(@as(usize, total), sma.len.load(.acquire));

    var seen = try testing.allocator.alloc(bool, total);
    defer testing.allocator.free(seen);
    @memset(seen, false);

    var per_writer = [_]usize{0} ** thread_count;

    for (0..total) |i| {
        const row = sma.get(i);
        try testing.expect(row.value < total);
        try testing.expect(!seen[row.value]);
        seen[row.value] = true;
        try testing.expect(row.writer < thread_count);
        per_writer[row.writer] += 1;
    }

    for (per_writer) |count| {
        try testing.expectEqual(per_thread, count);
    }

    for (seen) |flag| try testing.expect(flag);
}
