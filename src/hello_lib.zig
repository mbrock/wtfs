/// Minimal C ABI test library for Python integration
const std = @import("std");

/// Simple function to test basic calling
export fn wtfs_hello() void {
    std.debug.print("Hello from Zig! 🚀\n", .{});
}

/// Test function with parameters and return value
export fn wtfs_add(a: i32, b: i32) i32 {
    return a + b;
}

/// Test string handling
export fn wtfs_greet(name: [*:0]const u8) void {
    std.debug.print("Hello, {s}! Welcome to wtfs.\n", .{name});
}

/// Test return value with error condition
export fn wtfs_divide(a: i32, b: i32, result: *i32) bool {
    if (b == 0) {
        return false; // Error: division by zero
    }
    result.* = @divTrunc(a, b);
    return true;
}
