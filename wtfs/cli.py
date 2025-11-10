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
               '  wtfs -i .                    # Scan and launch interactive TUI\n'
               '  wtfs --webui .               # Scan and launch web interface\n'
               '  wtfs --save scan.bin ~       # Scan and save to file\n'
               '  wtfs --load scan.bin -i      # Load and browse interactively\n',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('path', nargs='?', default='.', help='Directory to scan (default: current)')
    parser.add_argument('--save', metavar='FILE', help='Save binary dump to file')
    parser.add_argument('--load', metavar='FILE', help='Load and display existing dump')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Launch interactive TUI browser')
    parser.add_argument('--webui', '-w', action='store_true',
                       help='Launch web interface')
    parser.add_argument('--host', default='127.0.0.1',
                       help='Host for web interface (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8765,
                       help='Port for web interface (default: 8765)')
    parser.add_argument('--skip-hidden', action='store_true',
                       help='Skip hidden files/directories (default: include them)')
    parser.add_argument('--threshold', type=str, default='100M',
                       help='Large file threshold (default: 100M)')
    parser.add_argument('--top', type=int, default=10, help='Show top N directories (default: 10)')
    parser.add_argument('--force', action='store_true',
                       help='Bypass safety checks (e.g., scanning / on macOS)')

    args = parser.parse_args()
    console = Console()

    try:
        if args.load:
            # Load existing dump
            data = dump.load(args.load)

            if args.interactive:
                from .tui import run_interactive
                run_interactive(args.load)
            elif args.webui:
                launch_web_ui(console, data, ".", args.host, args.port)
            else:
                show_results(console, data, args.top)
        else:
            # Scan directory
            threshold = parse_size(args.threshold)
            scanner = Scanner(
                skip_hidden=args.skip_hidden,
                large_file_threshold=threshold,
            )

            with console.status(f"[bold cyan]Scanning {args.path}..."):
                results = scanner.scan(args.path, output_file=args.save, force=args.force)

            # Load scan data
            data = dump.load(results['dump_file'])

            # Launch interactive, web UI, or display results
            if args.interactive:
                from .tui import run_interactive
                run_interactive(results['dump_file'], root_path=args.path)
            elif args.webui:
                launch_web_ui(console, data, args.path, args.host, args.port)
            else:
                # Display in terminal
                show_results(console, data, args.top)

                if args.save:
                    console.print(f"\n[dim]Saved to: {args.save}[/dim]")

    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted[/dim]")
        sys.exit(0)
    except Exception as e:
        import traceback
        console.print(f"[bold red]Error:[/bold red] {e}")
        if "--debug" in sys.argv:
            traceback.print_exc()
        sys.exit(1)


def launch_web_ui(console: Console, data: dump.WtfsDump, scan_path: str, host: str, port: int):
    """Launch the web interface."""
    import uvicorn

    from .web import create_app, make_routes, format_size, format_number

    # Show scan summary
    console.print("[bold green]✓[/bold green] Scan complete!")
    console.print(f"  [dim]→[/dim] {format_number(data.totals.directories)} directories")
    console.print(f"  [dim]→[/dim] {format_number(data.totals.files)} files")
    console.print(f"  [dim]→[/dim] {format_size(data.totals.bytes)} total")
    console.print()

    # Create app with scan data
    app = create_app(data, scan_path)
    make_routes(app)

    # Start server
    url = f"http://{host}:{port}"
    console.print(f"[bold blue]→[/bold blue] Server running at [link={url}]{url}[/link]")
    console.print("  [dim]Press Ctrl+C to stop[/dim]")
    console.print()

    # Run server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )


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

