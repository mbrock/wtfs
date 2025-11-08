"""Python CLI for wtfs - scan and analyze directories."""

import sys
import argparse
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

from .scanner import Scanner
from . import dump


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fast directory scanner with beautiful output',
        epilog='Examples:\n'
               '  wtfs /home/user              # Scan and show results\n'
               '  wtfs --save scan.bin ~       # Scan and save to file\n'
               '  wtfs --load scan.bin         # Load and display saved scan\n',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument('path', nargs='?', default='.', help='Directory to scan (default: current)')
    parser.add_argument('--save', metavar='FILE', help='Save binary dump to file')
    parser.add_argument('--load', metavar='FILE', help='Load and display existing dump')
    parser.add_argument('--skip-hidden', action='store_true', help='Skip hidden files/directories')
    parser.add_argument('--threshold', type=str, default='100M', 
                       help='Large file threshold (default: 100M)')
    parser.add_argument('--top', type=int, default=10, help='Show top N directories (default: 10)')
    
    args = parser.parse_args()
    console = Console()
    
    try:
        if args.load:
            # Load existing dump
            data = dump.load(args.load)
            show_results(console, data, args.top)
        else:
            # Scan directory
            threshold = parse_size(args.threshold)
            scanner = Scanner(
                skip_hidden=args.skip_hidden,
                large_file_threshold=threshold,
            )
            
            with console.status(f"[bold cyan]Scanning {args.path}..."):
                results = scanner.scan(args.path, output_file=args.save)
            
            # Load and display
            data = dump.load(results['dump_file'])
            show_results(console, data, args.top)
            
            if args.save:
                console.print(f"\n[dim]Saved to: {args.save}[/dim]")
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}", file=sys.stderr)
        sys.exit(1)


def show_results(console: Console, data: dump.WtfsDump, top_n: int):
    """Display scan results with rich formatting."""
    # Summary panel
    totals_text = Text()
    totals_text.append(f"{data.totals.directories:,}", style="bold cyan")
    totals_text.append(" directories  ")
    totals_text.append(f"{data.totals.files:,}", style="bold green")
    totals_text.append(" files  ")
    totals_text.append(f"{dump.format_size(data.totals.bytes)}", style="bold yellow")
    
    console.print(Panel(totals_text, title="[bold]Scan Results", border_style="blue"))
    
    # Top directories
    table = Table(title=f"Top {top_n} Directories", box=box.ROUNDED)
    table.add_column("Path", style="cyan", no_wrap=False)
    table.add_column("Size", justify="right", style="yellow")
    table.add_column("Files", justify="right", style="green")
    table.add_column("Share", justify="right", style="magenta")
    
    top_dirs = sorted(data.directories, key=lambda d: d.total_size, reverse=True)[:top_n]
    for dir in top_dirs:
        pct = 100 * dir.total_size / data.totals.bytes if data.totals.bytes else 0
        table.add_row(
            dir.path,
            dump.format_size(dir.total_size),
            f"{dir.total_files:,}",
            f"{pct:.1f}%",
        )
    
    console.print(table)
    
    # Large files
    if data.large_files:
        lf_table = Table(
            title=f"Large Files ({len(data.large_files)} total, showing top 5)",
            box=box.ROUNDED,
        )
        lf_table.add_column("Path", style="cyan", no_wrap=False)
        lf_table.add_column("Size", justify="right", style="yellow")
        
        for lf in sorted(data.large_files, key=lambda f: f.size, reverse=True)[:5]:
            lf_table.add_row(lf.path, dump.format_size(lf.size))
        
        console.print(lf_table)


def parse_size(size_str: str) -> int:
    """Parse size string like '100M' to bytes."""
    size_str = size_str.upper().strip()
    
    multipliers = {
        'K': 1024,
        'M': 1024**2,
        'G': 1024**3,
        'T': 1024**4,
    }
    
    if size_str[-1] in multipliers:
        return int(size_str[:-1]) * multipliers[size_str[-1]]
    else:
        return int(size_str)


if __name__ == '__main__':
    main()

