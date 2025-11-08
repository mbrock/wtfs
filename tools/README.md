# wtfsdump - Python Loader for wtfs Binary Dumps

Python library and CLI tool for reading wtfs binary dumps with beautiful output.

## Installation

Requires Python 3.11+ and uses [`uv`](https://docs.astral.sh/uv/) for package management.

```bash
# Install the wtfs package (from repo root)
uv sync

# Activate the virtual environment
source .venv/bin/activate

# Or run directly without activating
uv run wtfsdump scan.bin

# Or use the standalone script (no dependencies)
python3 tools/wtfsdump.py scan.bin
```

## Command Line Usage

```bash
# Show top 20 largest directories
wtfsdump scan.bin

# Show top 50
wtfsdump scan.bin --top 50

# Inspect specific directory
wtfsdump scan.bin --path ./home/user

# Find directories matching pattern
wtfsdump scan.bin --find docker
```

### Example Output

The tool provides beautiful, color-coded tables with:

- **Scan Summary Panel**: Shows total directories, files, and size at a glance
- **Largest Directories Table**: Lists directories sorted by size with columns for:
  - Path (cyan)
  - Size (yellow, human-readable)
  - File count (green)
  - Subdirectory count (blue)
  - Share percentage (magenta)
- **Large Files Table**: Shows individual large files (≥100 MB)
- **Directory Details**: When using `--path`, shows a clean summary with children

All output uses Rich for beautiful formatting with colors, borders, and proper alignment.

## Library Usage

```python
from wtfs import dump

# Load dump
data = dump.load('scan.bin')

# Access totals
print(f"Total: {data.totals.directories} dirs, {data.totals.files} files")
print(f"Size: {dump.format_size(data.totals.bytes)}")

# Iterate all directories
for dir in data.directories:
    print(f"{dir.path}: {dir.total_size} bytes, {dir.total_files} files")

# Get specific directory
home = data.get_directory('/home/user')
if home:
    print(f"Home size: {home.total_size}")

# Get children
children = data.get_children('/home/user')
for child in children:
    print(f"  {child.name}: {child.total_size} bytes")

# Find directories
docker_dirs = data.find_directories('docker')
for dir in docker_dirs:
    print(f"{dir.path}: {dir.total_size} bytes")

# Access large files
for lf in data.large_files:
    print(f"{lf.path}: {lf.size} bytes")

# Find biggest directory (excluding root)
biggest = max(data.directories, key=lambda d: d.total_size if d.index != 0 else 0)
print(f"Biggest: {biggest.path} - {dump.format_size(biggest.total_size)}")
```

## Data Model

### WtfsDump
- `totals: DirectoryTotals` - Global totals
- `directories: List[Directory]` - All directories
- `large_files: List[LargeFile]` - Large files (>= 100 MB by default)
- `get_directory(path: str) -> Directory` - Look up by path
- `get_children(path: str) -> List[Directory]` - Get immediate children
- `find_directories(pattern: str) -> List[Directory]` - Substring search

### DirectoryTotals
- `directories: int` - Total directory count
- `files: int` - Total file count
- `bytes: int` - Total size in bytes

### Directory
- `index: int` - Index in directory array
- `parent_index: int` - Parent directory index
- `name: str` - Directory name (basename)
- `path: str` - Full path
- `total_size: int` - Aggregated size (includes subdirs)
- `total_files: int` - Aggregated file count
- `total_dirs: int` - Aggregated directory count

### LargeFile
- `directory_index: int` - Parent directory index
- `name: str` - File name
- `size: int` - File size in bytes
- `path: str` - Full path

## Notes

- **Aggregated totals**: All directory sizes include subdirectories. This is computed during scan and saved to the binary, so no recomputation is needed.
- **Large files only**: Only files >= 100 MB threshold are individually tracked. All files are scanned and contribute to directory totals.
- **Fast loading**: Typical load time for 1.3M dirs is ~0.1s. With zstd compression, dumps are 85% smaller.

## Examples

### Find space hogs in /var

```python
from wtfs import dump

data = dump.load('root.bin')
var = data.get_directory('./var')
children = sorted(data.get_children('./var'), key=lambda d: d.total_size, reverse=True)

print(f"/var: {dump.format_size(var.total_size)}")
for child in children[:10]:
    pct = 100 * child.total_size / var.total_size
    print(f"  {child.name:20} {dump.format_size(child.total_size):>10} ({pct:4.1f}%)")
```

### Analyze Docker overlay directories

```python
from wtfs import dump

data = dump.load('root.bin')
docker_dirs = data.find_directories('overlay2')

total = sum(d.total_size for d in docker_dirs)
print(f"Docker overlay2 total: {dump.format_size(total)}")
print(f"Found {len(docker_dirs)} overlay directories")

# Top 10 largest
for dir in sorted(docker_dirs, key=lambda d: d.total_size, reverse=True)[:10]:
    print(f"  {dump.format_size(dir.total_size):>10} {dir.path}")
```

### Compare snapshots

```python
from wtfs import dump

old = dump.load('scan-2024-01.bin')
new = dump.load('scan-2024-02.bin')

print(f"Files: {old.totals.files:,} → {new.totals.files:,} ({new.totals.files - old.totals.files:+,})")
print(f"Size: {dump.format_size(old.totals.bytes)} → {dump.format_size(new.totals.bytes)}")

# Find directories that grew
for old_dir, new_dir in zip(old.directories, new.directories):
    if old_dir.path == new_dir.path:
        growth = new_dir.total_size - old_dir.total_size
        if growth > 1024 * 1024 * 100:  # > 100 MB
            print(f"  {old_dir.path}: +{dump.format_size(growth)}")
```

## Binary Format

See main README for wtfsdumpv1 format specification.
