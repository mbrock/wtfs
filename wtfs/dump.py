#!/usr/bin/env python3
"""
Python loader for wtfs binary dump format (wtfsdumpv1).

Usage:
    import wtfsdump

    dump = wtfsdump.load('scan.bin')

    # Access totals
    print(f"Total: {dump.totals.directories} dirs, {dump.totals.files} files")

    # Iterate directories
    for dir in dump.directories:
        print(f"{dir.path}: {dir.total_size} bytes, {dir.total_files} files")

    # Get specific directory
    home = dump.get_directory('/home/user')

    # List large files
    for lf in dump.large_files:
        print(f"{lf.path}: {lf.size} bytes")
"""

import struct
from dataclasses import dataclass
from typing import List, Optional, Dict


@dataclass
class DirectoryTotals:
    """Global totals for the entire scan."""
    directories: int
    files: int
    bytes: int


@dataclass
class Directory:
    """A directory node with aggregated statistics."""
    index: int
    parent_index: int
    name: str
    total_size: int
    total_files: int
    total_dirs: int

    # Computed fields
    path: Optional[str] = None


@dataclass
class LargeFile:
    """A large file entry."""
    directory_index: int
    name: str
    size: int

    # Computed field
    path: Optional[str] = None


class WtfsDump:
    """Loaded wtfs binary dump."""

    def __init__(self):
        self.totals: Optional[DirectoryTotals] = None
        self.directories: List[Directory] = []
        self.large_files: List[LargeFile] = []
        self._dir_by_path: Dict[str, Directory] = {}

    def _build_paths(self):
        """Build full paths for all directories."""
        # Build paths bottom-up
        paths = [''] * len(self.directories)

        def build_path(idx: int) -> str:
            if paths[idx]:
                return paths[idx]

            dir = self.directories[idx]
            if idx == 0:
                paths[idx] = '.'
            else:
                parent_path = build_path(dir.parent_index)
                if parent_path == '.':
                    paths[idx] = f'./{dir.name}'
                else:
                    paths[idx] = f'{parent_path}/{dir.name}'

            return paths[idx]

        for i in range(len(self.directories)):
            path = build_path(i)
            self.directories[i].path = path
            self._dir_by_path[path] = self.directories[i]

        # Build paths for large files
        for lf in self.large_files:
            dir_path = self.directories[lf.directory_index].path
            lf.path = f'{dir_path}/{lf.name}'

    def get_directory(self, path: str) -> Optional[Directory]:
        """Get a directory by path."""
        return self._dir_by_path.get(path)

    def get_children(self, path: str) -> List[Directory]:
        """Get immediate children of a directory."""
        dir = self.get_directory(path)
        if not dir:
            return []

        return [d for d in self.directories if d.parent_index == dir.index]

    def find_directories(self, pattern: str) -> List[Directory]:
        """Find directories matching a pattern (simple substring match)."""
        return [d for d in self.directories if pattern in d.path]


def load(filename: str) -> WtfsDump:
    """Load a wtfs binary dump file."""
    dump = WtfsDump()

    with open(filename, 'rb') as f:
        # Read magic header (16 bytes)
        magic = f.read(16)
        if not magic.startswith(b'wtfsdumpv1'):
            raise ValueError(f'Invalid magic header: {magic[:10]!r}')

        # Read totals (3 x u64 = 24 bytes)
        dirs, files, total_bytes = struct.unpack('<QQQ', f.read(24))
        dump.totals = DirectoryTotals(dirs, files, total_bytes)

        # Read name buffer
        name_len, = struct.unpack('<Q', f.read(8))
        name_buffer = f.read(name_len)

        # Parse names (null-terminated strings)
        names = []
        start = 0
        for i in range(len(name_buffer)):
            if name_buffer[i] == 0:
                names.append(name_buffer[start:i].decode('utf-8', errors='replace'))
                start = i + 1

        # Read directory count
        dir_count, = struct.unpack('<Q', f.read(8))

        # Read directory data
        parents = struct.unpack(f'<{dir_count}I', f.read(dir_count * 4))
        name_slices = struct.unpack(f'<{dir_count}I', f.read(dir_count * 4))
        total_sizes = struct.unpack(f'<{dir_count}Q', f.read(dir_count * 8))
        total_files = struct.unpack(f'<{dir_count}Q', f.read(dir_count * 8))
        total_dirs = struct.unpack(f'<{dir_count}Q', f.read(dir_count * 8))

        # Build directory objects
        for i in range(dir_count):
            # name_slices[i] is an old slice index we ignore
            # Instead we use sequential indices since names are in order
            name = names[i] if i < len(names) else ''

            dump.directories.append(Directory(
                index=i,
                parent_index=parents[i],
                name=name,
                total_size=total_sizes[i],
                total_files=total_files[i],
                total_dirs=total_dirs[i],
            ))

        # Read large files
        large_count, = struct.unpack('<Q', f.read(8))

        if large_count > 0:
            dir_indices = struct.unpack(f'<{large_count}Q', f.read(large_count * 8))
            lf_name_slices = struct.unpack(f'<{large_count}I', f.read(large_count * 4))
            sizes = struct.unpack(f'<{large_count}Q', f.read(large_count * 8))

            # Large file names come after directory names
            for i in range(large_count):
                name_idx = dir_count + i
                name = names[name_idx] if name_idx < len(names) else ''

                dump.large_files.append(LargeFile(
                    directory_index=dir_indices[i],
                    name=name,
                    size=sizes[i],
                ))

        dump._build_paths()

    return dump


def format_size(size: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if size < 1024:
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} PiB'


def main():
    """Command-line interface for inspecting dumps."""
    import sys
    import argparse
    from rich.console import Console

    from .display import (
        display_totals_panel,
        display_top_directories,
        display_large_files,
        display_directory_info,
    )

    parser = argparse.ArgumentParser(description='Inspect wtfs binary dumps')
    parser.add_argument('dump', help='Binary dump file')
    parser.add_argument('--top', type=int, default=20, help='Show top N largest directories')
    parser.add_argument('--find', help='Find directories matching pattern')
    parser.add_argument('--path', help='Show specific directory')

    args = parser.parse_args()

    console = Console()

    with console.status(f'[bold cyan]Loading {args.dump}...'):
        dump = load(args.dump)

    # Show totals in a nice panel
    display_totals_panel(console, dump.totals, title="Scan Summary")

    if args.path:
        dir = dump.get_directory(args.path)
        if dir:
            children = dump.get_children(args.path)
            display_directory_info(console, dir, children)
        else:
            console.print(f'[bold red]Directory not found:[/bold red] {args.path}')

    elif args.find:
        dirs = dump.find_directories(args.find)
        title = f'Found {len(dirs)} directories matching "{args.find}"'
        display_top_directories(
            console,
            dirs,
            dump.totals.bytes,
            limit=args.top,
            title=title,
        )

    else:
        # Show top directories
        display_top_directories(
            console,
            dump.directories,
            dump.totals.bytes,
            limit=args.top,
            title=f"Top {args.top} Largest Directories",
            show_subdirs=True,
        )

    # Always show large files if available
    display_large_files(console, dump.large_files, limit=10)


if __name__ == '__main__':
    main()
