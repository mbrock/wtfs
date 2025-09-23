const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod_wtfs = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .link_libc = true,
    });

    const mod_wtfscan = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    mod_wtfscan.addImport("wtfs", mod_wtfs);

    const exe_wtfscan = b.addExecutable(.{
        .name = "wtfs",
        .root_module = mod_wtfscan,
    });

    const run_cmd = b.addRunArtifact(exe_wtfscan);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const run_dumpstruct = b.addRunArtifact(exe_wtfscan);
    run_dumpstruct.addArg("--dump-structs");
    const structfile = run_dumpstruct.captureStdOut();
    const install_structfile = b.addInstallFile(structfile, "wtfstructs.txt");

    b.getInstallStep().dependOn(&install_structfile.step);

    const mod_test_wtfs = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const exe_tests = b.addTest(.{
        .root_module = mod_test_wtfs,
        .name = "wtfs-test",
    });

    const run_tests = b.addRunArtifact(exe_tests);
    run_tests.skip_foreign_checks = true;

    const step_test = b.step("test", "Run tests");
    step_test.dependOn(&run_tests.step);

    const mac_target = b.resolveTargetQuery(.{
        .cpu_arch = .aarch64,
        .os_tag = .macos,
    });

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

    b.installArtifact(exe_wtfscan);
    b.installArtifact(exe_tests);
}
