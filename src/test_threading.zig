const std = @import("std");
const builtin = @import("builtin");

const StartBarrier = struct {
    remaining: std.atomic.Value(usize),
    event: std.Io.Event = .unset,

    fn init(count: usize) StartBarrier {
        return .{ .remaining = std.atomic.Value(usize).init(count) };
    }

    fn wait(self: *StartBarrier, io: std.Io) void {
        if (self.remaining.fetchSub(1, .acq_rel) == 1) {
            self.event.set(io);
        }
        self.event.waitUncancelable(io);
    }
};

pub const ThreadTestGroup = struct {
    start_barrier: StartBarrier,
    threaded: std.Io.Threaded,
    group: std.Io.Group = .init,

    pub fn init(thread_count: usize) !ThreadTestGroup {
        if (builtin.single_threaded) {
            return error.ZigSkipTest;
        }
        return .{
            .start_barrier = StartBarrier.init(thread_count),
            // Every worker blocks on the start barrier, so they all need to be
            // running at once: `concurrent`, with room for all of them.
            .threaded = .init(std.testing.allocator, .{
                .concurrent_limit = .limited(thread_count),
            }),
        };
    }

    pub fn spawn(
        self: *ThreadTestGroup,
        tid: usize,
        comptime func: anytype,
        args: anytype,
    ) !void {
        const type_info = @typeInfo(@TypeOf(func));
        const ReturnType = switch (type_info) {
            .@"fn" => |fn_info| fn_info.return_type orelse void,
            else => @compileError("ThreadTestGroup.spawn expects a function"),
        };
        const ArgsType = @TypeOf(args);

        const Worker = struct {
            const Fn = func;
            const Ret = ReturnType;
            const Args = ArgsType;

            fn run(
                mytid: usize,
                io: std.Io,
                barrier: *StartBarrier,
                args_inner: Args,
            ) void {
                barrier.wait(io);
                const tidargs = .{mytid} ++ args_inner;
                if (@typeInfo(Ret) == .error_union) {
                    (@call(.auto, Fn, tidargs) catch |err| {
                        std.debug.panic(
                            "thread test worker error: {s}",
                            .{@errorName(err)},
                        );
                    });
                } else {
                    @call(.auto, Fn, tidargs);
                }
            }
        };

        const io = self.threaded.io();
        try self.group.concurrent(io, Worker.run, .{
            tid,
            io,
            &self.start_barrier,
            args,
        });
    }

    pub fn spawnMany(
        self: *ThreadTestGroup,
        n: comptime_int,
        comptime func: anytype,
        args: anytype,
    ) !void {
        for (0..n) |i| {
            try self.spawn(i, func, args);
        }
    }

    pub fn wait(self: *ThreadTestGroup) void {
        if (builtin.single_threaded) {
            return;
        }
        self.group.await(self.threaded.io()) catch {};
        self.threaded.deinit();
    }
};
