const std = @import("std");

pub const End = std.builtin.Endian;
const LE = End.little;

// ===== BTF parser (header + types + string table) =====

// extern → defined layout for takeStructPointer
const BtfHeader = extern struct {
    magic: u16, // 0xEB9F
    version: u8, // 1
    flags: u8,
    hdr_len: u32,
    type_off: u32,
    type_len: u32,
    str_off: u32,
    str_len: u32,
};

const BtfType = extern struct {
    name_off: u32,
    info: u32, // [31]=kind_flag, [30:24]=kind, [15:0]=vlen
    size: u32, // size or referenced type depending on kind
};

// AUX records (extern so takeStruct works)
const ArrayRec = extern struct { ty: u32, index_ty: u32, nelems: u32 };
const StructMember = extern struct { name_off: u32, ty: u32, bit_off: u32 };
const EnumVal32 = extern struct { name_off: u32, val: i32 };
const EnumVal64 = extern struct { name_off: u32, hi: i32, lo: u32 };
const FnParam = extern struct { name_off: u32, ty: u32 };
const DatasecVar = extern struct { ty: u32, offset: u32, size: u32 };

const Kind = enum(u32) {
    UNKN = 0,
    INT,
    PTR,
    ARRAY,
    STRUCT,
    UNION,
    ENUM,
    FWD,
    TYPEDEF,
    VOLATILE,
    CONST,
    RESTRICT,
    FUNC,
    FUNC_PROTO,
    VAR,
    DATASEC,
    FLOAT,
    DECL_TAG,
    TYPE_TAG,
    ENUM64,
};

inline fn infoKind(info: u32) Kind {
    return @enumFromInt((info >> 24) & 0x1f);
}
inline fn infoVlen(info: u32) u16 {
    return @intCast(info & 0xffff);
}
inline fn infoFlag(info: u32) bool {
    return ((info >> 31) & 1) == 1;
}

fn strz(strings: []const u8, off: u32) [:0]const u8 {
    if (off == 0 or off >= strings.len) return ""[0..0 :0];
    const s = strings[off..];
    const end = std.mem.indexOfScalar(u8, s, 0) orelse s.len;
    return s[0..end :0];
}

const SegmentedMultiArray = @import("SegmentedMultiArray.zig").SegmentedMultiArray;

const KStructInfo = struct {
    name_off: u24,
    name_len: u8,
    members_off: u24,
    members_len: u8,
    members_total: u24,
    size: u32,
    type_id: u32,
    is_union: bool,
    kind_flag: bool,
};

const StructTable = SegmentedMultiArray(KStructInfo, 512);
const MemberTable = SegmentedMultiArray(StructMember, 512);

const BtfData = struct {
    strings: []u8,
    structs: StructTable,
    members: MemberTable,

    fn deinit(self: *BtfData, alloc: std.mem.Allocator) void {
        self.structs.deinit(alloc);
        self.members.deinit(alloc);
        alloc.free(self.strings);
        self.* = undefined;
    }
};

fn loadKernelBtf(alloc: std.mem.Allocator) !BtfData {
    var file = try std.fs.cwd().openFile("/sys/kernel/btf/vmlinux", .{ .mode = .read_only });
    defer file.close();

    const reader_buffer = try alloc.alloc(u8, 4096);
    defer alloc.free(reader_buffer);

    var reader = file.reader(reader_buffer);
    var io_reader = &reader.interface;

    const hdr = try io_reader.takeStruct(BtfHeader, LE);
    if (hdr.magic != 0xEB9F) return error.BadMagic;

    const base: u64 = @as(u64, @intCast(hdr.hdr_len));
    const type_start: u64 = base + @as(u64, @intCast(hdr.type_off));
    const type_len: u64 = @as(u64, @intCast(hdr.type_len));
    const type_end = type_start + type_len;
    const str_start: u64 = base + @as(u64, @intCast(hdr.str_off));
    const str_len_u64: u64 = @as(u64, @intCast(hdr.str_len));
    const str_end = str_start + str_len_u64;

    if (type_end < type_start or str_end < str_start)
        return error.OutOfRange;

    const str_len = std.math.cast(usize, str_len_u64) orelse return error.StringTableTooBig;

    try reader.seekTo(str_start);

    const strings = try alloc.alloc(u8, str_len);
    errdefer alloc.free(strings);
    try io_reader.readSliceAll(strings);

    try reader.seekTo(type_start);

    var structs: StructTable = StructTable.empty;
    errdefer structs.deinit(alloc);

    var members: MemberTable = MemberTable.empty;
    errdefer members.deinit(alloc);

    var parser = Parser{
        .alloc = alloc,
        .io = io_reader,
        .remaining = type_len,
        .strings = strings,
        .structs = &structs,
        .members = &members,
        .type_id = 1,
    };

    try parser.parseAll();
    if (parser.remaining != 0)
        return error.OutOfRange;

    return .{
        .strings = strings,
        .structs = structs,
        .members = members,
    };
}

const Parser = struct {
    alloc: std.mem.Allocator,
    io: *std.Io.Reader,
    remaining: u64,
    strings: []const u8,
    structs: *StructTable,
    members: *MemberTable,
    type_id: u32,

    fn parseAll(self: *Parser) !void {
        const header_bytes = @sizeOf(BtfType);
        while (self.remaining >= header_bytes) {
            try self.parseOne();
            self.type_id += 1;
        }
        if (self.remaining != 0) {
            const leftover = std.math.cast(usize, self.remaining) orelse return error.OutOfRange;
            try self.skipBytes(leftover);
        }
    }

    fn parseOne(self: *Parser) !void {
        const ti = try self.readStruct(BtfType);
        const kind = infoKind(ti.info);
        const vlen = infoVlen(ti.info);
        const kflag = infoFlag(ti.info);

        switch (kind) {
            .UNKN => {},
            .STRUCT, .UNION => try self.readStructDef(kind, kflag, vlen, ti),
            .INT => {
                _ = try self.readInt(u32);
            },
            .PTR, .FWD, .TYPEDEF, .VOLATILE, .CONST, .RESTRICT, .FLOAT => {},
            .ARRAY => {
                _ = try self.readStruct(ArrayRec);
            },
            .ENUM => {
                try self.skipArray(EnumVal32, vlen);
            },
            .ENUM64 => {
                try self.skipArray(EnumVal64, vlen);
            },
            .FUNC => {
                _ = try self.readInt(u32);
            },
            .FUNC_PROTO => {
                try self.skipArray(FnParam, vlen);
            },
            .VAR => {
                _ = try self.readInt(u32);
            },
            .DATASEC => {
                try self.skipArray(DatasecVar, vlen);
            },
            .DECL_TAG, .TYPE_TAG => {
                _ = try self.readInt(u32);
            },
        }
    }

    fn readStructDef(self: *Parser, kind: Kind, kind_flag: bool, vlen: u16, ti: BtfType) !void {
        var name_off: u24 = 0;
        var name_len: u8 = 0;
        if (ti.name_off != 0) {
            const off_usize = std.math.cast(usize, ti.name_off) orelse return error.StringOffsetOverflow;
            if (off_usize < self.strings.len) {
                const slice = strz(self.strings, ti.name_off);
                if (slice.len > std.math.maxInt(u8))
                    return error.NameTooLong;
                name_off = @as(u24, @intCast(off_usize));
                name_len = @as(u8, @intCast(slice.len));
            }
        }

        var members_off: u24 = 0;
        var members_len: u8 = 0;
        var members_total: u24 = 0;
        if (vlen != 0) {
            const count = @as(usize, vlen);
            const need = count * @sizeOf(StructMember);
            if (need > self.remaining)
                return error.OutOfRange;
            const base = self.members.takeFromReader(self.alloc, self.io, count, LE) catch |err| switch (err) {
                error.EndOfStream => return error.OutOfRange,
                else => return err,
            };
            self.remaining -= need;
            members_off = @as(u24, @intCast(base));
            members_total = @as(u24, @intCast(count));
            const view_len = @min(count, std.math.maxInt(u8));
            members_len = @as(u8, @intCast(view_len));
        } else {
            members_total = 0;
        }

        const idx = try self.structs.addOne(self.alloc);
        const info = KStructInfo{
            .name_off = name_off,
            .name_len = name_len,
            .members_off = members_off,
            .members_len = members_len,
            .members_total = members_total,
            .size = ti.size,
            .type_id = self.type_id,
            .is_union = kind == .UNION,
            .kind_flag = kind_flag,
        };
        self.structs.set(idx, info);
    }

    fn readStruct(self: *Parser, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.remaining < size)
            return error.OutOfRange;
        const value = self.io.takeStruct(T, LE) catch |err| switch (err) {
            error.EndOfStream => return error.OutOfRange,
            else => return err,
        };
        self.remaining -= size;
        return value;
    }

    fn readInt(self: *Parser, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.remaining < size)
            return error.OutOfRange;
        const value = self.io.takeInt(T, LE) catch |err| switch (err) {
            error.EndOfStream => return error.OutOfRange,
            else => return err,
        };
        self.remaining -= size;
        return value;
    }

    fn skipArray(self: *Parser, comptime T: type, vlen: u16) !void {
        if (vlen == 0) return;
        const total = @sizeOf(T) * @as(usize, vlen);
        try self.skipBytes(total);
    }

    fn skipBytes(self: *Parser, count: usize) !void {
        if (count == 0) return;
        if (self.remaining < count)
            return error.OutOfRange;
        self.io.discardAll(count) catch |err| switch (err) {
            error.EndOfStream => return error.OutOfRange,
            else => return err,
        };
        self.remaining -= count;
    }
};

// ===== demo: print a few structs =====

var stdout_buffer: [4096]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;
const TabWriter = @import("TabWriter.zig");

fn dumpTableStats(comptime Table: type, label: []const u8, stats: Table.DebugStats) !void {
    try stdout.print(
        "{s} table: rows={d}, cap={d}, shelves={d}, bytes={d}\n",
        .{ label, stats.len, stats.cap, stats.shelf_count, stats.total_bytes },
    );

    var field_writer = TabWriter.TabWriter.init(stdout, &.{
        .{ .width = 12 },
        .{ .width = 12, .alignment = .right },
    });
    defer field_writer.finish() catch {};

    inline for (@typeInfo(Table.Field).@"enum".fields) |field_info| {
        const field = @as(Table.Field, @enumFromInt(field_info.value));
        const bytes = stats.bytesForField(field);
        if (bytes != 0) {
            try field_writer.printRow(.{ "{s}", "{d}" }, .{ .{field_info.name}, .{bytes} });
        }
    }

    var shelf_writer = TabWriter.TabWriter.init(stdout, &.{
        .{ .width = 8 },
        .{ .width = 10, .alignment = .right },
        .{ .width = 10, .alignment = .right },
    });
    defer shelf_writer.finish() catch {};

    var i: usize = 0;
    while (i < stats.shelf_count) : (i += 1) {
        const shelf = stats.shelves[i];
        try shelf_writer.printRow(.{ "{d}", "{d}", "{d}" }, .{ .{i}, .{shelf.used}, .{shelf.capacity} });
    }
    try stdout.print("\n", .{});
}

pub fn main() !void {
    const gpa = std.heap.page_allocator;

    var data = try loadKernelBtf(gpa);
    defer data.deinit(gpa);

    const strings: []const u8 = data.strings;
    var struct_view = data.structs.slices();
    const struct_total = struct_view.lenItems();
    const print_limit: usize = 20;

    var shown: usize = 0;
    var idx: usize = 0;
    while (idx < struct_total and shown < print_limit) : (idx += 1) {
        const info = struct_view.get(idx);
        if (info.name_len == 0) continue;

        const name_off = @as(usize, @intCast(info.name_off));
        const name_len = @as(usize, info.name_len);
        if (name_off + name_len > strings.len) continue;
        const name_slice = strings[name_off .. name_off + name_len];
        const label = if (info.is_union) "union" else "struct";

        const view_len = @as(usize, info.members_len);
        const member_total = @as(usize, @intCast(info.members_total));
        const truncated_suffix: []const u8 = if (member_total > view_len) "+" else "";

        try stdout.print("{s} {s} (members={d}{s}, size={d})\n", .{ label, name_slice, member_total, truncated_suffix, info.size });

        if (member_total != 0) {
            const member_start = @as(usize, @intCast(info.members_off));
            const limit = @min(member_total, 8);
            var m: usize = 0;
            while (m < limit) : (m += 1) {
                const member = data.members.get(member_start + m);
                const mname_z = strz(strings, member.name_off);
                const mname = mname_z[0..mname_z.len];
                try stdout.print("  .{s}  (ty_id={d}, bit_off={d})\n", .{ mname, member.ty, member.bit_off });
            }
            try stdout.print("\n", .{});
        } else {
            try stdout.print("\n", .{});
        }

        shown += 1;
    }

    const struct_stats = data.structs.debugStats();
    const member_stats = data.members.debugStats();

    try dumpTableStats(StructTable, "structs", struct_stats);
    try dumpTableStats(MemberTable, "members", member_stats);

    try stdout.flush();
}
