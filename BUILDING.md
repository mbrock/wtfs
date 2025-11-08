# Building Python Wheels

## Simple Workflow

### 1. Cross-compile ALL binaries with one command

```bash
zig build python
```

This leverages Zig's excellent cross-compilation to build for all platforms:
- `wtfs-linux-x86_64` (3.1 MB)
- `wtfs-linux-aarch64` (3.3 MB)
- `wtfs-macos-x86_64` (372 KB)
- `wtfs-macos-aarch64` (358 KB)

### 2. Build one universal wheel

```bash
uv build --wheel
```

The build hook (`hatch_build.py`) bundles ALL binaries into one wheel.

### 3. Result

- **One wheel**: `dist/wtfs-0.1.0-py3-none-any.whl` (~2.1 MB compressed)
- **Works everywhere**: Linux x86_64/ARM64, macOS x86_64/ARM64
- **Runtime selection**: Python detects platform and uses the right binary
- **Simple distribution**: Single `pip install wtfs` works on all platforms!

## Development Workflow

For development (editable install):

```bash
# Build the default binary (native platform)
zig build

# Install in development mode
uv sync

# Python scanner will find zig-out/bin/wtfs automatically
uv run python3 -c "from wtfs import Scanner; Scanner().scan('.')"
```

## The Magic: Zig Cross-Compilation

Build ALL platforms from ONE machine (Linux, macOS, doesn't matter):

```bash
zig build python
# ✓ Compiles for Linux x86_64, ARM64
# ✓ Compiles for macOS x86_64, ARM64 (Apple Silicon)
# ✓ All in parallel, all from your laptop!
```

Then bundle into one wheel that works everywhere:

```bash
uv build --wheel
# Result: One 2.1 MB wheel for all platforms 🎉
```

## Why This is Awesome

- **One build machine**: No need for macOS, ARM servers, etc.
- **Fast**: Zig cross-compiles in seconds
- **Simple distribution**: One wheel on PyPI works everywhere
- **Small**: 2.1 MB for 4 platform binaries (compressed)
- **Smart**: Python picks the right binary at runtime
- **Zero user dependencies**: Users just `pip install wtfs`

