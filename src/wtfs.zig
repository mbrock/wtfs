// mac_bulk.zig — getattrlistbulk() with extern structs, no @cInclude.
const std = @import("std");
const posix = std.posix;

// ======== Darwin constants we need (from <sys/attr.h> / <sys/vnode.h>) ========

// FSOPT_* (getattrlistbulk options)
const FSOPT_NOFOLLOW: u32 = 0x0000_0001;
const FSOPT_REPORT_FULLSIZE: u32 = 0x0000_0004;
const FSOPT_PACK_INVAL_ATTRS: u32 = 0x0000_0008;

// attrlist bitmaps
const ATTR_BIT_MAP_COUNT: u16 = 5;

// Common attrs
const ATTR_CMN_NAME: u32 = 0x0000_0001;
const ATTR_CMN_FSID: u32 = 0x0000_0004;
const ATTR_CMN_OBJTYPE: u32 = 0x0000_0008;
const ATTR_CMN_FILEID: u32 = 0x0200_0000;
const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;

// File attrs
const ATTR_FILE_LINKCOUNT: u32 = 0x0000_0001;
const ATTR_FILE_TOTALSIZE: u32 = 0x0000_0002;
const ATTR_FILE_ALLOCSIZE: u32 = 0x0000_0004;

const CommonAttrMask = packed struct(u32) {
    name: bool = false,
    pad0: u1 = 0,
    fsid: bool = false,
    obj_type: bool = false,
    pad1: u21 = 0,
    file_id: bool = false,
    pad2: u5 = 0,
    returned_attrs: bool = false,
};

const DirAttrMask = packed struct(u32) {
    linkcount: bool = false,
    entrycount: bool = false,
    mountstatus: bool = false,
    allocsize: bool = false,
    ioblocksize: bool = false,
    datalength: bool = false,
    pad0: u26 = 0,
};

const FileAttrMask = packed struct(u32) {
    linkcount: bool = false,
    total_size: bool = false,
    alloc_size: bool = false,
    pad0: u29 = 0,
};

const AttrGroupMask = packed struct(u160) align(4) {
    common: CommonAttrMask = .{},
    vol: u32 = 0,
    dir: DirAttrMask = .{},
    file: FileAttrMask = .{},
    fork: u32 = 0,
};

const FsOptMask = packed struct(u32) {
    nofollow: bool = false,
    pad0: u1 = 0,
    report_fullsize: bool = false,
    pack_invalid_attrs: bool = false,
    pad1: u28 = 0,
};

inline fn maskValue(mask: anytype) u32 {
    return @as(u32, @bitCast(mask));
}

// vnode (object) types (from <sys/vnode.h>)
const VNON: u32 = 0;
const VREG: u32 = 1;
const VDIR: u32 = 2;
const VBLK: u32 = 3;
const VCHR: u32 = 4;
const VLNK: u32 = 5;
const VSOCK: u32 = 6;
const VFIFO: u32 = 7;
const VBAD: u32 = 8;
// (others exist; we only need the basics)

const AttrList = packed struct {
    bitmapcount: u16 = ATTR_BIT_MAP_COUNT,
    reserved: u16 = 0,
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
};

const AttributeSet = packed struct {
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
};

const AttrRef = packed struct {
    attr_dataoffset: i32,
    attr_length: u32,
};

const Fsid = packed struct { id0: i32, id1: i32 };

const Payload = packed struct {
    reclen: u32, // total length of this record in bytes
    returned: AttributeSet, // ATTR_CMN_RETURNED_ATTRS payload
    name_ref: AttrRef, // ATTR_CMN_NAME (actual string lives elsewhere in record)
    fsid: Fsid,
    objtype: u32,
    fileid: u64,
    linkcount: u32,
    total: u64,
    alloc: u64,
};

extern "c" fn getattrlistbulk(
    dirfd: std.posix.fd_t,
    alist: *const AttrList,
    attrbuf: *anyopaque,
    buflen: usize,
    options: u32,
) c_int;

// ======== Public API ========

pub const Kind = enum { file, dir, symlink, other };

pub const Entry = struct {
    name: [:0]const u8, // UTF-8
    kind: Kind,
    fsid: Fsid,
    fileid: u64,
    linkcount: u32,
    total: u64, // apparent bytes
    alloc: u64, // allocated/physical bytes
};

pub fn scanDirBulk(
    dir_fd: std.posix.fd_t,
    gpa: std.mem.Allocator,
    cb: anytype, // fn (e: Entry) void
) !usize {
    const requested_common = CommonAttrMask{
        .returned_attrs = true,
        .name = true,
        .obj_type = true,
        .fsid = true,
        .file_id = true,
    };
    const requested_file = FileAttrMask{
        .linkcount = true,
        .total_size = true,
        .alloc_size = true,
    };
    const opts_mask = FsOptMask{
        .nofollow = true,
        .report_fullsize = true,
        .pack_invalid_attrs = true,
    };

    var al = AttrList{
        .commonattr = maskValue(requested_common),
        .volattr = 0,
        .dirattr = 0,
        .fileattr = maskValue(requested_file),
        .forkattr = 0,
    };

    const buf = try gpa.alloc(u8, 128 * 1024);
    defer gpa.free(buf);

    const opts: u32 = maskValue(opts_mask);

    var total: usize = 0;
    while (true) {
        const n = getattrlistbulk(dir_fd, &al, buf.ptr, buf.len, opts);
        if (n <= 0) {
            if (n < 0) {
                const err = posix.errno(n);
                std.debug.print("getattrlistbulk errno={s}\n", .{@tagName(err)});
            }
            break;
        }

        var chunk_reader = std.io.Reader.fixed(buf);
        var record_index: usize = 0;
        while (record_index < @as(usize, @intCast(n))) : (record_index += 1) {
            const payload = try chunk_reader.peekStructPointer(Payload);
            try chunk_reader.discardAll(@as(usize, payload.reclen));

            // extract name
            const namerefptr = @as([*]u8, @ptrCast(&payload.name_ref));
            const namestart = if (payload.name_ref.attr_dataoffset < 0)
                namerefptr - @abs(payload.name_ref.attr_dataoffset)
            else
                namerefptr + @abs(payload.name_ref.attr_dataoffset);
            const name = namestart[0 .. payload.name_ref.attr_length - 1 :0];

            std.debug.print("record[{d}] {any}\n", .{ record_index, payload });

            // deliver
            cb(Entry{
                .name = name,
                .kind = switch (payload.objtype) {
                    VDIR => .dir,
                    VREG => .file,
                    VLNK => .symlink,
                    else => .other,
                },
                .fsid = payload.fsid,
                .fileid = payload.fileid,
                .linkcount = payload.linkcount,
                .total = payload.total,
                .alloc = payload.alloc,
            });

            total += 1;
        }
    }
    return total;
}
