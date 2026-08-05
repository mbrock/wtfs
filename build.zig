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
    run_cmd.addPassthruArgs();

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const run_dumpstruct = b.addRunArtifact(exe_wtfscan);
    run_dumpstruct.addArg("--dump-structs");
    const structfile = run_dumpstruct.captureStdOut(.{});
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

    // Shared library for Python integration (Phase 0: hello world test)
    const mod_hello = b.createModule(.{
        .root_source_file = b.path("src/hello_lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_hello = b.addLibrary(.{
        .name = "wtfs_hello",
        .root_module = mod_hello,
        .linkage = .dynamic,
    });

    b.installArtifact(lib_hello);

    // Scanner library for Python integration (Phase 1: real scanner)
    const mod_scanner = b.createModule(.{
        .root_source_file = b.path("src/scanner_lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        // .single_threaded = true, // Enable threading to show the crash
    });

    mod_scanner.addImport("DiskScan", mod_wtfs);

    const lib_scanner = b.addLibrary(.{
        .name = "wtfs_scanner",
        .root_module = mod_scanner,
        .linkage = .dynamic,
    });

    b.installArtifact(lib_scanner);
    b.installArtifact(exe_wtfscan);

    // Cross-compilation targets for Python package wheels
    const python_targets = [_]std.Target.Query{
        .{ .cpu_arch = .x86_64, .os_tag = .linux },
        .{ .cpu_arch = .aarch64, .os_tag = .linux },
        .{ .cpu_arch = .x86_64, .os_tag = .macos },
        .{ .cpu_arch = .aarch64, .os_tag = .macos },
        // Windows requires fixing compile-time stderr() evaluation
        // .{ .cpu_arch = .x86_64, .os_tag = .windows },
    };

    const python_step = b.step("python", "Build binaries for Python package (all platforms)");

    inline for (python_targets) |target_query| {
        const cross_target = b.resolveTargetQuery(target_query);

        const cross_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = cross_target,
            .optimize = .ReleaseFast,
        });
        cross_module.addImport("wtfs", mod_wtfs);

        const cross_exe = b.addExecutable(.{
            .name = "wtfs",
            .root_module = cross_module,
        });

        // Generate platform-specific name
        const os_name = @tagName(target_query.os_tag.?);
        const arch_name = @tagName(target_query.cpu_arch.?);
        const platform_name = b.fmt("wtfs-{s}-{s}", .{ os_name, arch_name });

        const install_bin = b.addInstallBinFile(
            cross_exe.getEmittedBin(),
            platform_name,
        );

        python_step.dependOn(&install_bin.step);
    }
}
