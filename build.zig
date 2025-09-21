const std = @import("std");

const extra_targets = [_]std.Target.Query{
    .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl },
    // .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .musl },
    // .{ .cpu_arch = .x86_64, .os_tag = .freebsd },
    .{ .cpu_arch = .aarch64, .os_tag = .macos },
};

pub fn build(b: *std.Build) !void {
    // const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // const root_module = b.createModule(.{
    //     .root_source_file = b.path("src/main.zig"),
    //     .target = target,
    //     .optimize = optimize,
    //     .link_libc = true,
    // });

    // const exe = b.addExecutable(.{
    //     .name = "wtfs",
    //     .root_module = root_module,
    // });
    // b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    // const run_cmd = b.addRunArtifact(exe);
    // if (b.args) |args| {
    //     run_cmd.addArgs(args);
    // }
    // run_step.dependOn(&run_cmd.step);

    const test_step = b.step("test", "Run tests");
    inline for (extra_targets) |query| {
        const resolved = b.resolveTargetQuery(query);

        const cross_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = resolved,
            .optimize = optimize,
            .link_libc = true,
        });
        const cross_exe = b.addExecutable(.{
            .name = "wtfs",
            .root_module = cross_module,
        });

        const run_cross_step = b.addRunArtifact(cross_exe);
        if (b.args) |args| {
            run_cross_step.addArgs(args);
        }
        if (resolved.result.os.tag == b.resolveTargetQuery(.{}).result.os.tag) {
            run_step.dependOn(&run_cross_step.step);
        }

        const triple = try query.zigTriple(b.allocator);
        const install = b.addInstallArtifact(cross_exe, .{
            .dest_dir = .{ .override = .{ .custom = triple } },
        });
        b.getInstallStep().dependOn(&install.step);

        const tests = b.addTest(.{
            .root_module = cross_module,
        });
        const run_tests = b.addRunArtifact(tests);
        run_tests.skip_foreign_checks = true;

        test_step.dependOn(&run_tests.step);
        const stepname = std.fmt.allocPrint(b.allocator, "test-{s}", .{triple}) catch unreachable;
        const xtest_step = b.step(stepname, stepname);
        xtest_step.dependOn(&run_tests.step);
    }
}
