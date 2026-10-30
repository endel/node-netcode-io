const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    // Releases ship without debug info: 11 of a ReleaseFast addon's 13 MB.
    const strip = b.option(bool, "strip", "Omit the addon's debug info");

    const quic_dep = b.dependency("quic", .{ .target = target, .optimize = optimize });
    const quic_mod = quic_dep.module("quic");

    const addon = b.addLibrary(.{
        .name = "netcode_io",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/addon.zig"),
            .target = target,
            .optimize = optimize,
            .strip = strip,
            .link_libc = true,
            .imports = &.{.{ .name = "quic", .module = quic_mod }},
        }),
    });
    // Node-API symbols resolve against the host process when it loads us.
    addon.linker_allow_shlib_undefined = true;

    const install = b.addInstallFileWithDir(addon.getEmittedBin(), .lib, "netcode_io.node");
    b.getInstallStep().dependOn(&install.step);

    const wt_client = b.addExecutable(.{
        .name = "wt-test-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/wt_client.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "quic", .module = quic_mod }},
        }),
    });
    b.installArtifact(wt_client);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wire.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.step("test", "Run the wire format tests").dependOn(&b.addRunArtifact(tests).step);
}
