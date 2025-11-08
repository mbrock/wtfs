# wtfsdump.py - Python Loader for wtfs Binary Dumps

Python library and CLI tool for reading wtfs binary dumps.

## Installation

No dependencies required - just Python 3.7+

```bash
# Copy to your project
cp tools/wtfsdump.py your_project/

# Or use directly
python3 tools/wtfsdump.py scan.bin
```

## Command Line Usage

```bash
# Show top 20 largest directories
python3 wtfsdump.py scan.bin

# Show top 50
python3 wtfsdump.py scan.bin --top 50

# Inspect specific directory
python3 wtfsdump.py scan.bin --path ./home/user

# Find directories matching pattern
python3 wtfsdump.py scan.bin --find docker
```

## Library Usage

```python
import wtfsdump

# Load dump
dump = wtfsdump.load('scan.bin')

# Access totals
print(f"Total: {dump.totals.directories} dirs, {dump.totals.files} files")
print(f"Size: {wtfsdump.format_size(dump.totals.bytes)}")

# Iterate all directories
for dir in dump.directories:
    print(f"{dir.path}: {dir.total_size} bytes, {dir.total_files} files")

# Get specific directory
home = dump.get_directory('/home/user')
if home:
    print(f"Home size: {home.total_size}")

# Get children
children = dump.get_children('/home/user')
for child in children:
    print(f"  {child.name}: {child.total_size} bytes")

# Find directories
docker_dirs = dump.find_directories('docker')
for dir in docker_dirs:
    print(f"{dir.path}: {dir.total_size} bytes")

# Access large files
for lf in dump.large_files:
    print(f"{lf.path}: {lf.size} bytes")

# Find biggest directory (excluding root)
biggest = max(dump.directories, key=lambda d: d.total_size if d.index != 0 else 0)
print(f"Biggest: {biggest.path} - {wtfsdump.format_size(biggest.total_size)}")
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
dump = wtfsdump.load('root.bin')
var = dump.get_directory('./var')
children = sorted(dump.get_children('./var'), key=lambda d: d.total_size, reverse=True)

print(f"/var: {wtfsdump.format_size(var.total_size)}")
for child in children[:10]:
    pct = 100 * child.total_size / var.total_size
    print(f"  {child.name:20} {wtfsdump.format_size(child.total_size):>10} ({pct:4.1f}%)")
```

### Analyze Docker overlay directories

```python
dump = wtfsdump.load('root.bin')
docker_dirs = dump.find_directories('overlay2')

total = sum(d.total_size for d in docker_dirs)
print(f"Docker overlay2 total: {wtfsdump.format_size(total)}")
print(f"Found {len(docker_dirs)} overlay directories")

# Top 10 largest
for dir in sorted(docker_dirs, key=lambda d: d.total_size, reverse=True)[:10]:
    print(f"  {wtfsdump.format_size(dir.total_size):>10} {dir.path}")
```

### Compare snapshots

```python
old = wtfsdump.load('scan-2024-01.bin')
new = wtfsdump.load('scan-2024-02.bin')

print(f"Files: {old.totals.files:,} → {new.totals.files:,} ({new.totals.files - old.totals.files:+,})")
print(f"Size: {wtfsdump.format_size(old.totals.bytes)} → {wtfsdump.format_size(new.totals.bytes)}")

# Find directories that grew
for old_dir, new_dir in zip(old.directories, new.directories):
    if old_dir.path == new_dir.path:
        growth = new_dir.total_size - old_dir.total_size
        if growth > 1024 * 1024 * 100:  # > 100 MB
            print(f"  {old_dir.path}: +{wtfsdump.format_size(growth)}")
```

## Binary Format

See main README for wtfsdumpv1 format specification.
