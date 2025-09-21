const std = @import("std");

const extra_targets = [_]std.Target.Query{
    .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl },
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
    }

    _ = b.step("run", "Run the app");
}
