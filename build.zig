const std = @import("std");

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

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const test_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const tests = b.addTest(.{ .root_module = test_module });
    const run_tests = b.addRunArtifact(tests);
    run_tests.skip_foreign_checks = true;

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);

    const mac_target = b.resolveTargetQuery(.{ .cpu_arch = .aarch64, .os_tag = .macos });
    const mac_step = b.step("mac", "Check macOS cross-compilation");

    const mac_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = mac_target,
        .optimize = optimize,
        .link_libc = true,
    });
    const mac_exe = b.addExecutable(.{
        .name = "wtfs",
        .root_module = mac_module,
    });
    mac_step.dependOn(&mac_exe.step);

    const mac_test_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = mac_target,
        .optimize = optimize,
        .link_libc = true,
    });
    const mac_tests = b.addTest(.{ .root_module = mac_test_module });
    const run_mac_tests = b.addRunArtifact(mac_tests);
    run_mac_tests.skip_foreign_checks = true;
    mac_step.dependOn(&run_mac_tests.step);
}
