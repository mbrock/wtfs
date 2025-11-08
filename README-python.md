# Python Package

The `wtfs` Python package provides a clean interface to the fast Zig scanner with beautiful output.

## Installation

```bash
# From the repo
uv sync

# The package bundles the wtfs binary, so users don't need Zig installed
```

## Usage

### Scanner API

Scan directories programmatically:

```python
from wtfs import Scanner, dump

# Create scanner
scanner = Scanner(
    skip_hidden=False,
    large_file_threshold=50 * 1024 * 1024,  # 50 MB
)

# Scan a directory
results = scanner.scan('/home/user')

print(f"Directories: {results['directories']:,}")
print(f"Files: {results['files']:,}")
print(f"Total: {results['bytes']:,} bytes")

# The scan results are saved to a binary dump
dump_file = results['dump_file']

# Load and analyze the dump
data = dump.load(dump_file)

# Find largest directories
largest = sorted(data.directories, key=lambda d: d.total_size, reverse=True)[:10]
for d in largest:
    print(f"{d.path}: {dump.format_size(d.total_size)}")
```

### CLI Tool

Analyze existing dumps with beautiful output:

```bash
# Scan and save
./zig-out/bin/wtfs --binary-output scan.bin /home/user

# Analyze with rich output
uv run wtfsdump scan.bin --top 50
uv run wtfsdump scan.bin --find docker
uv run wtfsdump scan.bin --path ./var
```

## How it Works

The Python package bundles the compiled `wtfs` binary and:

1. **Scanner**: Runs `wtfs --binary-output` as a subprocess
2. **Full Threading**: Uses Zig's thread pool for parallel scanning
3. **Dump Format**: Saves results in compact binary format
4. **Rich Output**: Beautiful tables and colors for analysis

This approach provides:
- ✓ Native Zig performance
- ✓ Full threading support
- ✓ Process isolation (can't crash Python)
- ✓ Simple, clean integration
- ✓ Platform-specific wheels

## Package Structure

```
wtfs/
├── __init__.py          # Package exports
├── scanner.py           # Scanner class (subprocess-based)
├── dump.py              # Dump loader and CLI
└── _bin/
    └── wtfs             # Bundled binary (platform-specific)
```

## Building Platform-Specific Wheels

The package includes the native binary, so wheels are platform-specific:

```bash
# Build for current platform
uv build

# For cross-compilation, use Zig's excellent cross-compile support:
# zig build -Dtarget=x86_64-linux -Doptimize=ReleaseFast
# zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast
# etc.
```

See `build_binary.py` for the build hook that compiles and bundles the binary.

