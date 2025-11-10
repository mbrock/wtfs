# wtfs Web Interface

A beautiful web interface for viewing wtfs scan results, built with [tagflow](https://github.com/lessrest/tagflow) and [htmx](https://htmx.org).

## Usage

Just add `--webui` (or `-w`) to scan and serve:

```bash
# Scan current directory and serve at http://localhost:8765
wtfs --webui

# Scan a specific directory
wtfs --webui /home/user/projects

# Customize host and port
wtfs --webui ~/Documents --host 0.0.0.0 --port 3000

# Skip hidden files
wtfs --webui . --skip-hidden

# Adjust large file threshold
wtfs --webui /var/log --threshold 10M

# Load existing scan and serve it
wtfs --load scan.bin --webui
```

## Features

- **Clean, modern UI** - Built with Tailwind CSS
- **Interactive sorting** - Sort directories by size, file count, or subdirectory count
- **Visual progress bars** - See space distribution at a glance
- **Large file tracking** - Quickly identify the biggest files
- **Responsive design** - Works on desktop and mobile

## How it Works

1. **Scan** - wtfs-web performs a filesystem scan using the fast Zig scanner
2. **Serve** - Starts a local web server with the results
3. **Browse** - View and sort results in your browser

The scan happens once at startup, so the results are instant when you navigate the web interface.

## Technology Stack

- **FastAPI** - Modern Python web framework
- **tagflow** - HTML generation using Python context managers
- **Tailwind CSS** - Utility-first CSS framework
- **uvicorn** - Lightning-fast ASGI server

## Example Output

```bash
$ wtfs --webui ~/projects/myapp

✓ Scan complete!
  → 1,234 directories
  → 45,678 files
  → 12.3 GiB total

→ Server running at http://127.0.0.1:8765
  Press Ctrl+C to stop
```

Then open http://127.0.0.1:8765 in your browser to explore the results!

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `path` | Directory to scan | Current directory |
| `--host` | Host to bind to | 127.0.0.1 |
| `--port` | Port to bind to | 8765 |
| `--skip-hidden` | Skip hidden files/directories | Include hidden |
| `--threshold` | Large file threshold (e.g., `100M`) | 100M |
| `--force` | Bypass safety checks | Off |

## Comparison with TUI

wtfs provides three ways to view scan results:

- **Terminal output** (default) - Quick summary with top directories
  - Fastest for quick checks
  - Pipes well with other tools
  - Uses Rich for beautiful formatting

- **TUI** (`wtfs -i` or `wtfs-tui`) - Terminal UI using [Textual](https://textual.textualize.io/)
  - Great for terminal users
  - Keyboard-driven navigation
  - Works over SSH
  - Full-featured browsing

- **Web UI** (`wtfs --webui`) - Web interface using tagflow + htmx
  - Beautiful modern UI
  - Click-based navigation
  - Easy to share (just send the URL)
  - Works on mobile

Use whichever fits your workflow!

