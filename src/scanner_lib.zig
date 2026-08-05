/// C ABI wrapper for DiskScan - Phase 1: Real Scanner Integration
///
/// This provides a C-compatible API for the Zig DiskScan functionality,
/// allowing Python (and other languages) to call it directly.
const std = @import("std");
const DiskScan = @import("DiskScan.zig");

/// Opaque handle to a scanner instance (hides Zig implementation details)
pub const ScannerHandle = opaque {};

/// A scanner plus the I/O implementation it owns. Unlike the CLI, a shared
/// library has no `main` to hand us an `Io`, so each handle brings its own.
const Scanner = struct {
    threaded: std.Io.Threaded,
    scan: DiskScan,
};

/// C-compatible scan options
pub const CScanOptions = extern struct {
    skip_hidden: bool,
    large_file_threshold: u64,
};

/// C-compatible scan results
pub const CScanResults = extern struct {
    total_dirs: u64,
    total_files: u64,
    total_bytes: u64,
    elapsed_ns: u64,
};

/// Create a new scanner instance
///
/// Returns: Opaque handle to scanner, or null on allocation failure
export fn wtfs_scanner_create() ?*ScannerHandle {
    const allocator = std.heap.c_allocator;

    const scanner = allocator.create(Scanner) catch return null;
    scanner.threaded = .init(allocator, .{ .stack_size = 32 * 1024 * 1024 });
    scanner.scan = DiskScan{
        .allocator = allocator,
        .io = scanner.threaded.io(),
    };

    return @ptrCast(scanner);
}

/// Run a scan on the specified directory
///
/// Args:
///   handle: Scanner instance
///   path: Null-terminated path string
///   options: Scan configuration options
///   results: Output parameter for scan results
///
/// Returns: true on success, false on error
export fn wtfs_scanner_scan(
    handle: *ScannerHandle,
    path: [*:0]const u8,
    options: *const CScanOptions,
    results: *CScanResults,
) bool {
    const scanner: *Scanner = @ptrCast(@alignCast(handle));
    const scan = &scanner.scan;

    // Configure scan
    scan.skip_hidden = options.skip_hidden;
    scan.large_file_threshold = options.large_file_threshold;
    scan.root = std.mem.span(path);

    // Initialize minimal progress tracking (required by gatherPhase)
    var progress = std.Progress.start(scan.io, .{
        .root_name = "wtfs",
        .disable_printing = true,
    });
    defer progress.end();
    scan.progress_root = progress;

    // Call performScan directly (now public) to avoid the reporting overhead
    const scan_results = scan.performScan() catch return false;

    // Copy results directly from performScan return value
    results.* = .{
        .total_dirs = scan_results.totals.directories,
        .total_files = scan_results.totals.files,
        .total_bytes = scan_results.totals.bytes,
        .elapsed_ns = scan_results.elapsed_ns,
    };

    return true;
}

/// Write scan results to binary dump file
///
/// NOTE: Must be called AFTER wtfs_scanner_scan()
///
/// Args:
///   handle: Scanner instance (with scan already performed)
///   path: Null-terminated path to output file
///
/// Returns: true on success, false on error
export fn wtfs_scanner_write_dump(
    handle: *ScannerHandle,
    path: [*:0]const u8,
) bool {
    const scanner: *Scanner = @ptrCast(@alignCast(handle));
    const scan = &scanner.scan;
    const file_path = std.mem.span(path);

    // Open file for writing
    var file = std.Io.Dir.cwd().createFile(scan.io, file_path, .{
        .truncate = true,
        .read = false,
    }) catch return false;
    defer file.close(scan.io);

    // Create buffered writer
    var buffer: [16 * 1024]u8 = undefined;
    var file_writer = file.writer(scan.io, &buffer);

    // Build results from current scan data
    const results = DiskScan.ScanResults{
        .elapsed_ns = 0,
        .totals = .{
            .directories = scan.directories.len(),
            .files = if (scan.directories.len() > 0)
                scan.directories.ptr(.total_files, 0).*
            else
                0,
            .bytes = if (scan.directories.len() > 0)
                scan.directories.ptr(.total_size, 0).*
            else
                0,
        },
    };

    // Write the results
    scan.writeBinaryResults(results, &file_writer.interface) catch return false;
    file_writer.end() catch return false;

    return true;
}

/// Get the number of large files found
///
/// Args:
///   handle: Scanner instance
///
/// Returns: Number of large files, or 0 if handle is invalid
export fn wtfs_scanner_large_file_count(handle: *ScannerHandle) u64 {
    const scanner: *Scanner = @ptrCast(@alignCast(handle));
    const scan = &scanner.scan;
    return scan.large_files.len;
}

/// Get information about a specific large file
///
/// Args:
///   handle: Scanner instance
///   index: Index of large file (0 to count-1)
///   path_buffer: Buffer to receive path (null-terminated)
///   path_buffer_size: Size of path buffer
///   size: Output parameter for file size
///
/// Returns: true on success, false if index out of bounds or buffer too small
export fn wtfs_scanner_get_large_file(
    handle: *ScannerHandle,
    index: u64,
    path_buffer: [*]u8,
    path_buffer_size: u64,
    size: *u64,
) bool {
    const scanner: *Scanner = @ptrCast(@alignCast(handle));
    const scan = &scanner.scan;

    if (index >= scan.large_files.len) return false;

    // Access large file data using MultiArrayList API
    const large_file_sizes = scan.large_files.items(.size);
    const large_file_dirs = scan.large_files.items(.directory_index);
    const large_file_names = scan.large_files.items(.name_slice);

    size.* = large_file_sizes[index];

    // Build full path
    const dir_idx = large_file_dirs[index];
    const dir_name_slice = scan.directories.ptr(.name_slice, dir_idx).*;
    const dir_path = scan.names.get(dir_name_slice);

    const file_name_slice = large_file_names[index];
    const file_name = scan.names.get(file_name_slice);

    // Calculate required size
    const required_size = dir_path.len + 1 + file_name.len + 1; // dir + "/" + name + "\0"
    if (required_size > path_buffer_size) return false;

    // Build path: dir/file
    var pos: usize = 0;
    @memcpy(path_buffer[pos..][0..dir_path.len], dir_path);
    pos += dir_path.len;
    path_buffer[pos] = '/';
    pos += 1;
    @memcpy(path_buffer[pos..][0..file_name.len], file_name);
    pos += file_name.len;
    path_buffer[pos] = 0; // null terminator

    return true;
}

/// Destroy scanner instance and free all resources
///
/// Args:
///   handle: Scanner instance to destroy
export fn wtfs_scanner_destroy(handle: *ScannerHandle) void {
    const scanner: *Scanner = @ptrCast(@alignCast(handle));
    const scan = &scanner.scan;
    const allocator = scan.allocator;

    // Cleanup scan resources
    scan.freeNameBuffers();
    scan.directories.deinit(allocator);
    scan.large_files.deinit(allocator);
    scanner.threaded.deinit();

    // Free the scanner itself
    allocator.destroy(scanner);
}
