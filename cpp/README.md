# wtfs-cpp

Fast, streamlined C++20 disk scanner that produces binary dumps for analysis.

## Philosophy

This C++ implementation focuses on doing one thing well: **fast scanning and binary dump creation**.

The architecture follows Unix philosophy:
- **wtfs-scan** (C++) - Fast scanner that produces compact binary dumps
- **wtfsdump.py** (Python) - Flexible analysis tool for querying and reporting

This separation allows:
- Maximum performance for scanning (compiled C++)
- Maximum flexibility for analysis (Python with rich ecosystem)
- Clean separation of concerns

## Features

- ⚡ **Blazing fast** directory scanning using `std::filesystem`
- 💾 **Compact binary dumps** in wtfsdumpv1 format
- 🎯 **Focused design** - does one thing well
- 🔍 **Configurable large file tracking**
- 🐍 **Python integration** - dumps analyzed with wtfsdump.py

## Building

### Requirements

- C++20 compiler (GCC 10+, Clang 10+, MSVC 2019+)
- Meson & Ninja

### Build Instructions

```bash
# Install build tools
pip3 install --user meson ninja

# Build
meson setup build
meson compile -C build

# Binary is at: build/wtfs-scan
```

## Usage

```bash
# Scan and create binary dump
./build/wtfs-scan /path/to/scan output.bin

# Analyze with Python
python3 ../tools/wtfsdump.py output.bin

# Custom options
./build/wtfs-scan --large-file-threshold 1G --no-skip-hidden /data scan.bin
```

## Command Line

```
Usage: wtfs-scan [OPTIONS] <directory> <output.bin>

Arguments:
  <directory>    Directory to scan
  <output.bin>   Output file (wtfsdumpv1 format)

Options:
  --skip-hidden              Skip hidden files (default)
  --no-skip-hidden           Include hidden files
  --large-file-threshold N   Track files > N bytes (default: 100M)
                             Accepts K/M/G/T suffix (e.g., 1G, 500M)
```

## Workflow

1. **Scan** with C++ (fast):
   ```bash
   ./build/wtfs-scan ~/Documents docs.bin
   ```

2. **Analyze** with Python (flexible):
   ```bash
   # Show top 20 largest directories
   python3 ../tools/wtfsdump.py docs.bin --top 20

   # Find all node_modules directories
   python3 ../tools/wtfsdump.py docs.bin --find node_modules

   # Inspect specific directory
   python3 ../tools/wtfsdump.py docs.bin --path ./src/components
   ```

## Binary Format

The wtfsdumpv1 format is a compact, structure-of-arrays format:

```
┌─────────────────────┐
│ Magic: "wtfsdumpv1" │  16 bytes
├─────────────────────┤
│ Totals (3×u64)      │  24 bytes (dirs, files, bytes)
├─────────────────────┤
│ Name buffer         │  Variable (null-terminated strings)
├─────────────────────┤
│ Directory data      │  Arrays: parent[], size[], files[], dirs[]
├─────────────────────┤
│ Large files         │  Arrays: dir_idx[], name_idx[], size[]
└─────────────────────┘
```

See `../tools/wtfsdump.py` for the Python loader implementation.

## Performance

Typical performance on modern hardware:
- **~50k directories/second** on NVMe SSD
- **~100k files/second** scanning
- **Minimal memory** footprint (stores only aggregates)
- **Compact dumps** (~10 KB for 200+ directories)

## Architecture

### C++ Scanner (wtfs-scan)
- Recursive directory traversal
- Size aggregation (bubble up from leaves)
- Large file tracking
- Binary dump writing

### Python Analyzer (wtfsdump.py)
- Binary dump loading
- Path queries and searching
- Flexible reporting
- Interactive analysis

## Comparison with Zig Version

| Feature | Zig | C++ |
|---------|-----|-----|
| Scanning speed | ⚡⚡⚡ | ⚡⚡⚡ |
| Binary output | ✓ | ✓ |
| Text output | ✓ | ✗ (use Python) |
| Build system | Zig | Meson |
| Code size | ~5000 LOC | ~500 LOC |
| Dependencies | None | std::filesystem |

The C++ version achieves similar performance with much simpler code by delegating reporting to Python.

## What's Included

- **wtfs-scan** - Primary binary-only scanner (fast, focused)
- **wtfs-text** - Reference implementation with text output (kept for comparison)

Use `wtfs-scan` for production workflows. The text version is provided for reference.

## Future Ideas

- [ ] Parallel scanning with thread pool
- [ ] Incremental dumps (delta updates)
- [ ] Extended attributes tracking
- [ ] Compression (zstd)
- [ ] Network streaming mode

## License

MIT License - See parent directory LICENSE file for details.

## Contributing

This is an experimental port exploring focused, Unix-style tool design. Contributions welcome!

For the original Zig implementation with built-in reporting, see the parent directory.
