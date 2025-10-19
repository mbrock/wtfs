const std = @import("std");
const posix = std.posix;

pub const Config = struct {
    allocator: std.mem.Allocator,
    entries: ?u16 = null,
};

/// Placeholder type retained for compatibility with previous backends.
pub const StatRequest = void;

pub const Backend = SyncBackend;

const SyncBackend = struct {
    pub fn init(_: *SyncBackend, config: Config) !void {
        _ = config;
    }

    pub fn deinit(_: *SyncBackend) void {}

    pub fn openDirectory(
        _: *SyncBackend,
        parent_fd: posix.fd_t,
        name: [:0]const u8,
    ) posix.OpenError!posix.fd_t {
        return posix.openat(parent_fd, name, directoryFlags(), 0);
    }

    pub fn statAt(
        _: *SyncBackend,
        dir_fd: posix.fd_t,
        name: [:0]const u8,
    ) posix.FStatAtError!posix.Stat {
        return posix.fstatat(dir_fd, name, 0);
    }

    pub fn directoryFlags() posix.O {
        return posix.O{
            .NONBLOCK = true,
            .DIRECTORY = true,
            .NOFOLLOW = true,
        };
    }
};
