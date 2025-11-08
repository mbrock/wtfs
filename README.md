# wtfs

Fast disk usage analyzer with beautiful output and efficient file scanning.

**Status**: Early stage, not well-tested. API subject to change.

## What is this?

`wtfs` is a fast directory tree analyzer that shows disk usage with a clean, tabular interface. It includes:

- **Fast scanning**: Uses efficient bulk attribute retrieval (`getattrlistbulk` on macOS, optimized fallbacks on Linux)
- **Beautiful output**: Formatted tables showing directory sizes, file counts, and largest files
- **Binary snapshots**: Save and load scan results for later analysis or comparison
- **Zig library**: The underlying library provides type-safe, efficient file attribute scanning

## Quick Start

```bash
# Build
zig build

# Scan current directory
./zig-out/bin/wtfs .

# Save scan to file
./zig-out/bin/wtfs --binary-output scan.bin .

# Load and view saved scan
./zig-out/bin/wtfs --binary-input scan.bin

# Show files larger than 1MB
./zig-out/bin/wtfs --large-file-threshold 1M .
```

## Installation

Requires Zig 0.15.1 or later.

### With Nix (recommended)

```bash
# Run directly
nix run github:mbrock/wtfs -- ~

# Install to profile
nix profile install github:mbrock/wtfs
```

### Build from source

```bash
git clone https://github.com/mbrock/wtfs
cd wtfs
zig build
./zig-out/bin/wtfs --help
```

### Use as a library

```bash
zig fetch --save git+https://github.com/mbrock/wtfs
```

Then in your `build.zig`:
```zig
const wtfs = b.dependency("wtfs", .{});
exe.root_module.addImport("wtfs", wtfs.module("wtfs"));
```

## Command Line Usage

```
usage: wtfs [--skip-hidden] [--large-file-threshold SIZE] [--binary-output PATH] [--binary-input PATH] [--dump-structs] [dir]
       SIZE accepts optional K/M/G/T suffix (base 1024)
       --binary-input: load scan results from file instead of scanning
```

### Example Output

```
.: 243 dirs, 497 files, 113.6MiB total

Top-level directories by total size:

┌──────────────┬─────────────┬─────────┬──────────────┬─────────────┐
│ Directory    │        Size │   Share │        Files │        Dirs │
├──────────────┼─────────────┼─────────┼──────────────┼─────────────┤
│ ./.zig-cache │    92.0 MiB │   81.0% │           42 │          20 │
│ ./zig-out    │    14.6 MiB │   12.9% │            3 │           1 │
│ ./.git       │     5.5 MiB │    4.9% │          411 │         215 │
│ ./src        │   200.0 KiB │    0.2% │           14 │           0 │
└──────────────┴─────────────┴─────────┴──────────────┴─────────────┘

Heaviest directories in tree:

┌────────────────────────────────────────┬─────────────┬─────────┐
│ Directory                              │        Size │   Share │
├────────────────────────────────────────┼─────────────┼─────────┤
│ .                                      │   113.6 MiB │  100.0% │
│   .zig-cache                           │    92.0 MiB │   81.0% │
│     o                                  │    91.3 MiB │   80.4% │
│       6463279db290e53d329669813725e8cb │    21.6 MiB │   19.0% │
└────────────────────────────────────────┴─────────────┴─────────┘
```

### Binary Snapshots

Save scan results to a compact binary format for later analysis:

```bash
# Save snapshot
./zig-out/bin/wtfs --binary-output snapshot.bin /large/directory

# Load and analyze later (instant - no rescanning)
./zig-out/bin/wtfs --binary-input snapshot.bin
```

Binary format is `wtfsdumpv1` - a compact representation that preserves:
- Complete directory tree structure with **aggregated totals**
- File and directory counts per directory
- Size information
- Large file tracking (>= 100 MB by default)

Typical compression: 200+ directories → ~9 KB binary file.

**Python package available**: Uses [`uv`](https://docs.astral.sh/uv/) for fast, reliable package management. See [`tools/README.md`](tools/README.md) for details.

```bash
# Install dependencies
uv sync

# Use the Scanner to scan and analyze
from wtfs import Scanner, dump

scanner = Scanner()
results = scanner.scan('.')  # Uses wtfs binary with full threading
print(f"Found {results['directories']} directories")

# Analyze the results with rich output
data = dump.load(results['dump_file'])
# ... beautiful tables and analysis ...

# Or use the CLI directly
uv run wtfsdump scan.bin --top 50
uv run wtfsdump scan.bin --find docker
```


## Library API Usage

For advanced users who want to use wtfs as a library for custom file scanning:

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

## License

MIT License - See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

## Author

Mikael Brockman ([@mbrock](https://github.com/mbrock))
