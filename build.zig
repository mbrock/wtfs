const std = @import("std");
const zon = @import("build.zig.zon");

/// Release binaries. Linux builds use raw syscalls and no libc, so they are
/// fully static; macOS builds link libSystem as every macOS binary must.
const release_targets = [_]std.Target.Query{
    .{ .cpu_arch = .x86_64, .os_tag = .linux },
    .{ .cpu_arch = .aarch64, .os_tag = .linux },
    .{ .cpu_arch = .x86_64, .os_tag = .macos },
    .{ .cpu_arch = .aarch64, .os_tag = .macos },
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const options = b.addOptions();
    options.addOption([]const u8, "version", zon.version);

    const exe = addWtfs(b, target, optimize, options);
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.addPassthruArgs();
    b.step("run", "Run wtfs").dependOn(&run.step);

    const tests = b.addTest(.{ .root_module = exe.root_module });
    const run_tests = b.addRunArtifact(tests);
    b.step("test", "Run unit tests").dependOn(&run_tests.step);

    // `zig build release` writes zig-out/bin/wtfs-<os>-<arch> for every
    // release target. The Python wheel bundles the same files.
    const release = b.step("release", "Cross-compile release binaries for all platforms");
    for (release_targets) |query| {
        const cross = addWtfs(b, b.resolveTargetQuery(query), .safe, options);
        const name = b.fmt("wtfs-{t}-{t}", .{ query.os_tag.?, query.cpu_arch.? });
        release.dependOn(&b.addInstallBinFile(cross.getEmittedBin(), name).step);
    }
    b.step("python", "Alias for release (binaries bundled into the Python wheel)").dependOn(release);
}

fn addWtfs(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    options: *std.Build.Step.Options,
) *std.Build.Step.Compile {
    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .strip = optimize != .debug,
    });
    mod.addOptions("build_options", options);
    return b.addExecutable(.{ .name = "wtfs", .root_module = mod });
}
