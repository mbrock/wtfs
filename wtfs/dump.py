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
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box

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
    totals_text = Text()
    totals_text.append(f"{dump.totals.directories:,}", style="bold cyan")
    totals_text.append(" directories  ")
    totals_text.append(f"{dump.totals.files:,}", style="bold green")
    totals_text.append(" files  ")
    totals_text.append(f"{format_size(dump.totals.bytes)}", style="bold yellow")

    console.print(Panel(totals_text, title="[bold]Scan Summary", border_style="blue"))

    if args.path:
        dir = dump.get_directory(args.path)
        if dir:
            # Show directory details
            info_table = Table(show_header=False, box=box.SIMPLE, padding=(0, 2))
            info_table.add_column("Property", style="cyan")
            info_table.add_column("Value", style="yellow")

            info_table.add_row("Path", dir.path)
            info_table.add_row("Size", format_size(dir.total_size))
            info_table.add_row("Files", f"{dir.total_files:,}")
            info_table.add_row("Subdirectories", f"{dir.total_dirs:,}")

            console.print(f"\n[bold]Directory Info[/bold]")
            console.print(info_table)

            children = dump.get_children(args.path)
            if children:
                children_table = Table(title="Top Children", box=box.ROUNDED)
                children_table.add_column("Name", style="cyan", no_wrap=False)
                children_table.add_column("Size", justify="right", style="yellow")
                children_table.add_column("Files", justify="right", style="green")

                for child in sorted(children, key=lambda d: d.total_size, reverse=True)[:10]:
                    children_table.add_row(
                        child.name,
                        format_size(child.total_size),
                        f"{child.total_files:,}",
                    )

                console.print(children_table)
        else:
            console.print(f'[bold red]Directory not found:[/bold red] {args.path}')

    elif args.find:
        dirs = dump.find_directories(args.find)

        table = Table(
            title=f'Found {len(dirs)} directories matching "{args.find}"',
            box=box.ROUNDED,
        )
        table.add_column("Path", style="cyan", no_wrap=False)
        table.add_column("Size", justify="right", style="yellow")
        table.add_column("Files", justify="right", style="green")
        table.add_column("Share", justify="right", style="magenta")

        for dir in sorted(dirs, key=lambda d: d.total_size, reverse=True)[:args.top]:
            pct = 100 * dir.total_size / dump.totals.bytes if dump.totals.bytes else 0
            table.add_row(
                dir.path,
                format_size(dir.total_size),
                f"{dir.total_files:,}",
                f"{pct:.1f}%",
            )

        console.print(table)

    else:
        # Show top directories
        top_dirs = sorted(dump.directories, key=lambda d: d.total_size, reverse=True)

        table = Table(
            title=f"Top {args.top} Largest Directories",
            box=box.ROUNDED,
        )
        table.add_column("Path", style="cyan", no_wrap=False)
        table.add_column("Size", justify="right", style="yellow")
        table.add_column("Files", justify="right", style="green")
        table.add_column("Subdirs", justify="right", style="blue")
        table.add_column("Share", justify="right", style="magenta")

        for dir in top_dirs[:args.top]:
            pct = 100 * dir.total_size / dump.totals.bytes if dump.totals.bytes else 0
            table.add_row(
                dir.path,
                format_size(dir.total_size),
                f"{dir.total_files:,}",
                f"{dir.total_dirs:,}",
                f"{pct:.1f}%",
            )

        console.print(table)

    if dump.large_files:
        lf_table = Table(
            title=f"Large Files ({len(dump.large_files)} total, showing top 10)",
            box=box.ROUNDED,
        )
        lf_table.add_column("Path", style="cyan", no_wrap=False)
        lf_table.add_column("Size", justify="right", style="yellow")

        for lf in sorted(dump.large_files, key=lambda f: f.size, reverse=True)[:10]:
            lf_table.add_row(lf.path, format_size(lf.size))

        console.print(lf_table)


if __name__ == '__main__':
    main()
