# wtfs

Zig bindings for the `getattrlistbulk` syscall on macOS (and potentially FreeBSD).

**Status**: Early stage, not well-tested. API subject to change.

## What is this?

A library that provides efficient bulk retrieval of file attributes using the native `getattrlistbulk` syscall. This allows fetching multiple file attributes for directory entries in a single system call, which is significantly more efficient than individual `stat` calls.

The library uses Zig's comptime features to generate type-safe structs based on the attributes you request, ensuring you only pay for what you use.

## Installation

Requires Zig 0.15.1 or later.

```bash
zig fetch --save git+https://github.com/mbrock/wtfs
```

Then in your `build.zig`:
```zig
const wtfs = b.dependency("wtfs", .{});
exe.root_module.addImport("wtfs", wtfs.module("wtfs"));
```

## API Usage

### Basic Example

```zig
const std = @import("std");
const wtfs = @import("wtfs");

// Open directory with .iterate flag (required for getattrlistbulk)
const dir = try std.fs.cwd().openDir(".", .{ .iterate = true });
defer dir.close();

// Configure which attributes to retrieve at compile time
const mask = wtfs.AttrGroupMask{
    .common = .{ 
        .name = true,        // File/directory name
        .obj_type = true,    // Object type (file/dir/symlink)
        .file_id = true,     // inode number
    },
    .dir = .{ 
        .entry_count = true,  // Number of entries in directory
    },
    .file = .{ 
        .total_size = true,   // Logical file size
        .alloc_size = true,   // Allocated size on disk
    },
};

// The scanner type is generated at compile time based on your mask
const Scanner = wtfs.DirScanner(mask);

// Provide your own buffer for the syscall results
var buffer: [16384]u8 = undefined;
var scanner = Scanner.init(dir.handle, &buffer);

// Iterate through entries in batches
while (true) {
    if (!(try scanner.fill())) break;

    while (try scanner.next()) |entry| {
        std.debug.print("{s}: ", .{entry.name});

        switch (entry.kind) {
            .dir => {
                std.debug.print("directory with {} entries\n", .{entry.details.dir.entrycount});
            },
            .file => {
                std.debug.print("file, {}B ({}B allocated)\n", .{
                    entry.details.file.totalsize,
                    entry.details.file.allocsize,
                });
            },
            .symlink => std.debug.print("symlink\n", .{}),
            .other => std.debug.print("other\n", .{}),
        }
    }
}
```

On non-macOS targets `next()` will fetch entries on demand, so the outer
`fill()` loop is optional. macOS callers must keep the `fill()` call because it
performs the underlying `getattrlistbulk` syscall.

### Compile-Time Type Generation

The key feature is that the `EntryFor(mask)` function generates a struct at compile time with only the fields you've requested:

```zig
// Request only name and type
const minimal_mask = wtfs.AttrGroupMask{
    .common = .{ .name = true, .obj_type = true },
};

// Entry will only have .name and .kind fields
const MinimalEntry = wtfs.EntryFor(minimal_mask);

// Request everything
const full_mask = wtfs.AttrGroupMask{
    .common = .{ 
        .name = true, 
        .obj_type = true,
        .file_id = true,
        .fsid = true,
    },
    .dir = .{ 
        .linkcount = true,
        .entrycount = true,
        .allocsize = true,
    },
    .file = .{ 
        .linkcount = true,
        .totalsize = true,
        .allocsize = true,
    },
};

// Entry will have all requested fields
const FullEntry = wtfs.EntryFor(full_mask);
```

### Re-iterating a Directory

If you need to iterate the same directory again, you must seek back to the beginning:

```zig
const dir = try std.fs.cwd().openDir(".", .{ .iterate = true });
defer dir.close();

var scanner = Scanner.init(dir.handle, &buffer);

// First iteration
while (try scanner.next()) |entry| {
    // process entries
}

// Seek back to start for another iteration
try std.posix.lseek_SET(dir.handle, 0);

// Re-initialize scanner
scanner = Scanner.init(dir.handle, &buffer);

// Second iteration
while (try scanner.next()) |entry| {
    // process entries again
}
```

## Example Program

A basic example program is included that demonstrates the library:

```bash
zig build
./zig-out/bin/wtfs .
```

Output:
```
* .zig-cache                        x192
- LICENSE                           1.05KiB       (4.00KiB)
- build.zig                         8.28KiB       (12.00KiB)
* src                               x96
```

## License

MIT License - See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

## Author

Mikael Brockman ([@mbrock](https://github.com/mbrock))
