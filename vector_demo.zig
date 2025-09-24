const std = @import("std");
const SegmentedStringBuffer = @import("src/SegmentedStringBuffer.zig").SegmentedStringBuffer;

// Small prealloc to force many geometric shelves for demonstration
const Buffer = SegmentedStringBuffer(.{
    .prealloc = 16,
    .shelf_bits = 6,
    .offset_bits = 26,
});

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len != 3) {
        std.debug.print("Usage: {s} <input_file> <reader_buffer_size>\n", .{args[0]});
        std.debug.print("Example: {s} medium.txt 1024\n", .{args[0]});
        return;
    }

    const input_filename = args[1];
    const reader_buf_size = try std.fmt.parseInt(usize, args[2], 10);

    std.debug.print("=== SegmentedStringBuffer Vectored I/O Demo ===\n", .{});
    std.debug.print("Input: {s}, Reader buffer: {} bytes\n", .{ input_filename, reader_buf_size });

    // Read file into SegmentedStringBuffer
    const input_data = try std.fs.cwd().readFileAlloc(allocator, input_filename, 1024 * 1024);
    defer allocator.free(input_data);

    var buffer = Buffer.empty;
    defer buffer.deinit(allocator);

    const result = try buffer.append(allocator, input_data);
    std.debug.print("Stored {} bytes in SegmentedStringBuffer with geometric shelves\n", .{input_data.len});

    // Stream to output using SliceReader with specified buffer size
    const output_file = try std.fs.cwd().createFile("vector_output.txt", .{});
    defer output_file.close();

    var file_writer_buffer: [4096]u8 = undefined;
    var file_writer = output_file.writer(&file_writer_buffer);

    const reader_buffer = try allocator.alloc(u8, reader_buf_size);
    defer allocator.free(reader_buffer);

    var slice_reader = result.slice.reader(&buffer, reader_buffer);

    std.debug.print("\nStreaming with SliceReader (watch for pwritev syscalls):\n", .{});
    const bytes_streamed = try slice_reader.reader.streamRemaining(&file_writer.interface);
    try file_writer.interface.flush();
    try output_file.sync();

    std.debug.print("Successfully streamed {} bytes using vectored I/O\n", .{bytes_streamed});

    // Verify correctness
    const output_data = try std.fs.cwd().readFileAlloc(allocator, "vector_output.txt", input_data.len * 2);
    defer allocator.free(output_data);

    if (std.mem.eql(u8, input_data, output_data)) {
        std.debug.print("✓ Verification: Files are identical!\n", .{});
    } else {
        std.debug.print("✗ Error: Files differ! Input: {} bytes, Output: {} bytes\n", .{ input_data.len, output_data.len });
    }

    std.debug.print("\nTo see vectored I/O syscalls, run:\n", .{});
    std.debug.print("strace -e trace=pwritev ./vector_demo {s} {}\n", .{ input_filename, reader_buf_size });
}
