const std = @import("std");

pub const ATTR_BIT_MAP_COUNT: u16 = 5;

pub const CommonAttrMask = packed struct(u32) {
    name: bool = false,
    devid: bool = false,
    fsid: bool = false,
    objtype: bool = false,
    objtag: bool = false,
    objid: bool = false,
    objpermanentid: bool = false,
    parobjid: bool = false,
    script: bool = false,
    crtime: bool = false,
    modtime: bool = false,
    chgtime: bool = false,
    acctime: bool = false,
    bkuptime: bool = false,
    fndrinfo: bool = false,
    ownerid: bool = false,
    groupid: bool = false,
    accessmask: bool = false,
    flags: bool = false,
    gen_count: bool = false,
    document_id: bool = false,
    useraccess: bool = false,
    extended_security: bool = false,
    uuid: bool = false,
    grpuuid: bool = false,
    fileid: bool = false,
    parentid: bool = false,
    fullpath: bool = false,
    addedtime: bool = false,
    @"error": bool = false,
    data_protect_flags: bool = false,
    returned_attrs: bool = true,
};

pub const DirAttrMask = packed struct(u32) {
    linkcount: bool = false,
    entrycount: bool = false,
    mountstatus: bool = false,
    allocsize: bool = false,
    ioblocksize: bool = false,
    datalength: bool = false,
    pad0: u26 = 0,
};

pub const FileAttrMask = packed struct(u32) {
    linkcount: bool = false,
    totalsize: bool = false,
    allocsize: bool = false,
    pad0: u29 = 0,
};

pub const AttrGroupMask = packed struct(u160) {
    common: CommonAttrMask = .{},
    vol: u32 = 0,
    dir: DirAttrMask = .{},
    file: FileAttrMask = .{},
    fork: u32 = 0,
};

pub const FsOptMask = packed struct(u32) {
    nofollow: bool = false,
    pad0: u1 = 0,
    report_fullsize: bool = false,
    pack_invalid_attrs: bool = false,
    pad1: u28 = 0,
};

pub const AttrList = packed struct {
    bitmapcount: u16,
    reserved: u16,
    attrs: AttrGroupMask,
};

pub const AttributeSet = packed struct {
    attrs: AttrGroupMask,
};

pub const AttrRef = packed struct {
    off: i32,
    len: u32,
};

pub const Fsid = packed struct {
    id0: i32,
    id1: i32,
};

pub extern "c" fn getattrlistbulk(
    dirfd: std.posix.fd_t,
    alist: *const AttrList,
    attrbuf: *anyopaque,
    buflen: usize,
    options: FsOptMask,
) c_int;
