pub const mac = @import("wtfs.zig");
pub const strpool = @import("pool.zig");

comptime {
    _ = @import("pool.zig");
    _ = @import("wtfs.zig");
}
