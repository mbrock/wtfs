"""Shared display utilities for wtfs using Rich formatting."""

from typing import List, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

from .dump import WtfsDump, Directory, LargeFile, DirectoryTotals, format_size


def display_totals_panel(
    console: Console,
    totals: DirectoryTotals,
    title: str = "Scan Results",
):
    """Display summary totals in a formatted panel."""
    totals_text = Text()
    totals_text.append(f"{totals.directories:,}", style="bold cyan")
    totals_text.append(" directories  ")
    totals_text.append(f"{totals.files:,}", style="bold green")
    totals_text.append(" files  ")
    totals_text.append(f"{format_size(totals.bytes)}", style="bold yellow")

    console.print(Panel(totals_text, title=f"[bold]{title}", border_style="blue"))


def display_top_directories(
    console: Console,
    directories: List[Directory],
    total_bytes: int,
    limit: int = 10,
    title: Optional[str] = None,
    show_subdirs: bool = False,
):
    """Display top directories by size in a formatted table."""
    if title is None:
        title = f"Top {limit} Directories"

    table = Table(title=title, box=box.ROUNDED)
    table.add_column("Path", style="cyan", no_wrap=False)
    table.add_column("Size", justify="right", style="yellow")
    table.add_column("Files", justify="right", style="green")

    if show_subdirs:
        table.add_column("Subdirs", justify="right", style="blue")

    table.add_column("Share", justify="right", style="magenta")

    top_dirs = sorted(directories, key=lambda d: d.total_size, reverse=True)[:limit]

    for dir in top_dirs:
        pct = 100 * dir.total_size / total_bytes if total_bytes else 0
        row = [
            dir.path,
            format_size(dir.total_size),
            f"{dir.total_files:,}",
        ]

        if show_subdirs:
            row.append(f"{dir.total_dirs:,}")

        row.append(f"{pct:.1f}%")
        table.add_row(*row)

    console.print(table)


def display_large_files(
    console: Console,
    large_files: List[LargeFile],
    limit: int = 10,
):
    """Display large files in a formatted table."""
    if not large_files:
        return

    lf_table = Table(
        title=f"Large Files ({len(large_files)} total, showing top {limit})",
        box=box.ROUNDED,
    )
    lf_table.add_column("Path", style="cyan", no_wrap=False)
    lf_table.add_column("Size", justify="right", style="yellow")

    for lf in sorted(large_files, key=lambda f: f.size, reverse=True)[:limit]:
        lf_table.add_row(lf.path, format_size(lf.size))

    console.print(lf_table)


def display_directory_info(
    console: Console,
    directory: Directory,
    children: Optional[List[Directory]] = None,
):
    """Display detailed information about a specific directory."""
    # Show directory details
    info_table = Table(show_header=False, box=box.SIMPLE, padding=(0, 2))
    info_table.add_column("Property", style="cyan")
    info_table.add_column("Value", style="yellow")

    info_table.add_row("Path", directory.path)
    info_table.add_row("Size", format_size(directory.total_size))
    info_table.add_row("Files", f"{directory.total_files:,}")
    info_table.add_row("Subdirectories", f"{directory.total_dirs:,}")

    console.print(f"\n[bold]Directory Info[/bold]")
    console.print(info_table)

    # Show children if provided
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


def display_full_summary(
    console: Console,
    data: WtfsDump,
    top_n: int = 10,
    large_files_limit: int = 5,
):
    """Display a complete scan summary with totals, top directories, and large files."""
    display_totals_panel(console, data.totals)
    display_top_directories(console, data.directories, data.totals.bytes, limit=top_n)
    display_large_files(console, data.large_files, limit=large_files_limit)

