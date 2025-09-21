const std = @import("std");

const extra_targets = [_]std.Target.Query{
    .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl },
    // .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .musl },
    // .{ .cpu_arch = .x86_64, .os_tag = .freebsd },
    .{ .cpu_arch = .aarch64, .os_tag = .macos },
};

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const exe = b.addExecutable(.{
        .name = "wtfs",
        .root_module = root_module,
    });
    b.installArtifact(exe);

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

    _ = b.step("run", "Run the app");
}
