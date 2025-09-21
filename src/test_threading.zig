const std = @import("std");
const builtin = @import("builtin");

const StartBarrier = struct {
    remaining: std.atomic.Value(usize),
    event: std.Thread.ResetEvent = .{},

    fn init(count: usize) StartBarrier {
        return .{ .remaining = std.atomic.Value(usize).init(count) };
    }

    fn wait(self: *StartBarrier) void {
        if (self.remaining.fetchSub(1, .acq_rel) == 1) {
            self.event.set();
        }
        self.event.wait();
    }
};

pub const ThreadTestGroup = struct {
    start_barrier: StartBarrier,
    wg: std.Thread.WaitGroup = .{},

    pub fn init(thread_count: usize) !ThreadTestGroup {
        if (builtin.single_threaded) {
            return error.ZigSkipTest;
        }
        return .{ .start_barrier = StartBarrier.init(thread_count) };
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

        self.wg.start();

        const Worker = struct {
            const Fn = func;
            const Ret = ReturnType;
            const Args = ArgsType;

            return_value: ?Ret = null,

            fn run(
                mytid: usize,
                barrier: *StartBarrier,
                wg_ptr: *std.Thread.WaitGroup,
                args_inner: Args,
            ) void {
                defer wg_ptr.finish();
                barrier.wait();
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

        const thread = try std.Thread.spawn(
            .{},
            Worker.run,
            .{
                tid,
                &self.start_barrier,
                &self.wg,
                args,
            },
        );
        thread.detach();
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
        self.wg.wait();
    }
};
