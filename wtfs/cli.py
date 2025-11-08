"""Python CLI for wtfs - scan and analyze directories."""

import sys
import argparse
from pathlib import Path
from rich.console import Console

from .scanner import Scanner
from . import dump
from .display import display_full_summary


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fast directory scanner with beautiful output',
        epilog='Examples:\n'
               '  wtfs /home/user              # Scan and show results\n'
               '  wtfs -i .                    # Scan and launch interactive browser\n'
               '  wtfs --save scan.bin ~       # Scan and save to file\n'
               '  wtfs --load scan.bin -i      # Load and browse interactively\n',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('path', nargs='?', default='.', help='Directory to scan (default: current)')
    parser.add_argument('--save', metavar='FILE', help='Save binary dump to file')
    parser.add_argument('--load', metavar='FILE', help='Load and display existing dump')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Launch interactive TUI browser')
    parser.add_argument('--skip-hidden', action='store_true', help='Skip hidden files/directories')
    parser.add_argument('--threshold', type=str, default='100M',
                       help='Large file threshold (default: 100M)')
    parser.add_argument('--top', type=int, default=10, help='Show top N directories (default: 10)')

    args = parser.parse_args()
    console = Console()

    try:
        if args.load:
            # Load existing dump
            if args.interactive:
                from .tui import run_interactive
                run_interactive(args.load)
            else:
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

            # Launch interactive or display results
            if args.interactive:
                from .tui import run_interactive
                run_interactive(results['dump_file'])
            else:
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
    display_full_summary(console, data, top_n=top_n, large_files_limit=5)


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

