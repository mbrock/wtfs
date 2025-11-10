"""
Web interface for wtfs scanner using tagflow and htmx.
Berkeley Graphics / USGC style: dense, tabular, semantic HTML.
"""

from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from tagflow import (
    DocumentMiddleware,
    TagResponse,
    tag,
    text,
)

from . import dump

# Global state - the loaded scan data
_scan_data: dump.WtfsDump | None = None
_scan_path: str | None = None


def format_size(size: int) -> str:
    """Format size in human-readable format."""
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PiB"


def format_number(n: int) -> str:
    """Format number with thousands separators."""
    return f"{n:,}"


def create_app(scan_data: dump.WtfsDump, scan_path: str) -> FastAPI:
    """Create FastAPI app with the scan data."""
    global _scan_data, _scan_path
    _scan_data = scan_data
    _scan_path = scan_path

    app = FastAPI(
        title="wtfs - Web Directory Scanner",
        description="Fast directory scanning with dense tabular visualization",
        default_response_class=TagResponse,
    )
    app.add_middleware(DocumentMiddleware)
    return app


@contextmanager
def layout(title: str):
    """Common layout wrapper for all pages."""
    with tag.html(lang="en"):
        with tag.head():
            with tag.meta(charset="utf-8"):
                pass
            with tag.meta(name="viewport", content="width=device-width, initial-scale=1"):
                pass
            with tag.title():
                text(f"{title} - wtfs")

            # htmx
            with tag.script(
                src="https://unpkg.com/htmx.org@1.9.10",
                integrity="sha384-D1Kt99CQMDuVetoL1lrYwg5t+9QdHe7NLX/SoJYkXDFfX37iInKRy5xLSi8nO7UC",
                crossorigin="anonymous",
            ):
                pass

            # Dense, tabular CSS
            with tag.style():
                text("""
                    * { margin: 0; padding: 0; box-sizing: border-box; }

                    :root {
                        color-scheme: light dark;
                    }

                    body {
                        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
                        font-size: 13px;
                        line-height: 1.4;
                        background: #fff;
                        color: #000;
                    }

                    @media (prefers-color-scheme: dark) {
                        body {
                            background: #000;
                            color: #fff;
                        }
                    }

                    header {
                        border-bottom: 2px solid #000;
                        padding: 8px 12px;
                        display: flex;
                        align-items: baseline;
                        gap: 16px;
                    }

                    @media (prefers-color-scheme: dark) {
                        header {
                            border-color: #fff;
                        }
                    }

                    h1 {
                        font-size: 16px;
                        font-weight: 700;
                        letter-spacing: 0.02em;
                    }

                    .scan-path {
                        font-size: 11px;
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        opacity: 0.7;
                    }

                    main {
                        padding: 12px;
                    }

                    table {
                        width: 100%;
                        border-collapse: collapse;
                        border: 1px solid #000;
                        font-variant-numeric: tabular-nums;
                        margin-bottom: 12px;
                    }

                    @media (prefers-color-scheme: dark) {
                        table {
                            border-color: #fff;
                        }
                    }

                    th, td {
                        border: 1px solid #000;
                        padding: 3px 6px;
                        text-align: left;
                    }

                    @media (prefers-color-scheme: dark) {
                        th, td {
                            border-color: #fff;
                        }
                    }

                    th {
                        font-weight: 700;
                        font-size: 11px;
                        text-transform: uppercase;
                        letter-spacing: 0.05em;
                        background: #f5f5f5;
                    }

                    @media (prefers-color-scheme: dark) {
                        th {
                            background: #1a1a1a;
                        }
                    }

                    td.num {
                        text-align: right;
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        font-size: 12px;
                    }

                    td.path {
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        font-size: 12px;
                    }

                    .stats {
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                        gap: 12px;
                        margin-bottom: 12px;
                    }

                    .stat-box {
                        border: 1px solid #000;
                        padding: 6px 8px;
                    }

                    @media (prefers-color-scheme: dark) {
                        .stat-box {
                            border-color: #fff;
                        }
                    }

                    .stat-label {
                        font-size: 10px;
                        font-weight: 700;
                        text-transform: uppercase;
                        letter-spacing: 0.05em;
                        margin-bottom: 2px;
                    }

                    .stat-value {
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        font-size: 18px;
                        font-weight: 700;
                    }

                    details {
                        margin: 0;
                    }

                    details details {
                        margin-left: 20px;
                    }

                    summary {
                        cursor: pointer;
                        user-select: none;
                        list-style: none;
                        padding: 3px 6px;
                        border: 1px solid #000;
                        border-bottom: none;
                        display: grid;
                        grid-template-columns: 2fr 100px 100px 80px 60px;
                        gap: 8px;
                        align-items: center;
                        font-size: 12px;
                    }

                    @media (prefers-color-scheme: dark) {
                        summary {
                            border-color: #fff;
                        }
                    }

                    summary::-webkit-details-marker {
                        display: none;
                    }

                    summary:hover {
                        background: #f5f5f5;
                    }

                    @media (prefers-color-scheme: dark) {
                        summary:hover {
                            background: #1a1a1a;
                        }
                    }

                    summary::before {
                        content: "▸";
                        display: inline-block;
                        width: 12px;
                        transition: transform 0.15s;
                    }

                    details[open] > summary::before {
                        transform: rotate(90deg);
                    }

                    .dir-name {
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                    }

                    .dir-files, .dir-dirs, .dir-size, .dir-pct {
                        text-align: right;
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                    }

                    .dir-pct {
                        font-size: 11px;
                        opacity: 0.7;
                    }

                    .leaf {
                        padding: 3px 6px;
                        border: 1px solid #000;
                        border-bottom: none;
                        display: grid;
                        grid-template-columns: 2fr 100px 100px 80px 60px;
                        gap: 8px;
                        align-items: center;
                        font-size: 12px;
                    }

                    @media (prefers-color-scheme: dark) {
                        .leaf {
                            border-color: #fff;
                        }
                    }

                    details:last-child > summary,
                    .leaf:last-child {
                        border-bottom: 1px solid #000;
                    }

                    @media (prefers-color-scheme: dark) {
                        details:last-child > summary,
                        .leaf:last-child {
                            border-color: #fff;
                        }
                    }

                    section {
                        margin-bottom: 16px;
                    }

                    h2 {
                        font-size: 11px;
                        font-weight: 700;
                        text-transform: uppercase;
                        letter-spacing: 0.05em;
                        margin-bottom: 4px;
                        padding: 4px 6px;
                        border: 1px solid #000;
                        border-bottom: 2px solid #000;
                        background: #f5f5f5;
                    }

                    @media (prefers-color-scheme: dark) {
                        h2 {
                            border-color: #fff;
                            background: #1a1a1a;
                        }
                    }

                    .file-row {
                        display: grid;
                        grid-template-columns: 3fr 120px 80px;
                        gap: 8px;
                        padding: 3px 6px;
                        border: 1px solid #000;
                        border-top: none;
                        font-size: 12px;
                    }

                    @media (prefers-color-scheme: dark) {
                        .file-row {
                            border-color: #fff;
                        }
                    }

                    .file-row:first-of-type {
                        border-top: 1px solid #000;
                    }

                    @media (prefers-color-scheme: dark) {
                        .file-row:first-of-type {
                            border-color: #fff;
                        }
                    }

                    .file-path {
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        overflow: hidden;
                        text-overflow: ellipsis;
                        white-space: nowrap;
                    }

                    .file-size {
                        text-align: right;
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                    }

                    .file-pct {
                        text-align: right;
                        font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, Consolas, monospace;
                        font-size: 11px;
                        opacity: 0.7;
                    }

                    @media (min-width: 1200px) {
                        .two-col {
                            display: grid;
                            grid-template-columns: 2fr 1fr;
                            gap: 12px;
                        }
                    }
                """)

        with tag.body():
            # Header
            with tag.header():
                with tag.h1():
                    text("WTFS")
                if _scan_path:
                    with tag.span("scan-path"):
                        text(_scan_path)

            # Main content
            with tag.main():
                yield


def make_routes(app: FastAPI):
    """Register routes on the app."""

    @app.get("/")
    async def home():
        """Display scan results."""
        if not _scan_data:
            with layout("No Data"):
                with tag.div():
                    with tag.p():
                        text("No scan data loaded")
            return

        with layout("Scan Results"):
            # Stats
            with tag.div("stats"):
                with tag.div("stat-box"):
                    with tag.div("stat-label"):
                        text("Directories")
                    with tag.div("stat-value"):
                        text(format_number(_scan_data.totals.directories))

                with tag.div("stat-box"):
                    with tag.div("stat-label"):
                        text("Files")
                    with tag.div("stat-value"):
                        text(format_number(_scan_data.totals.files))

                with tag.div("stat-box"):
                    with tag.div("stat-label"):
                        text("Total Size")
                    with tag.div("stat-value"):
                        text(format_size(_scan_data.totals.bytes))

            with tag.div("two-col"):
                # Directory tree
                with tag.section():
                    with tag.h2():
                        text("Directory Breakdown")

                    # Get children of root, skip root itself
                    root = _scan_data.directories[0]
                    children = [d for d in _scan_data.directories if d.parent_index == 0]
                    children.sort(key=lambda d: d.total_size, reverse=True)

                    for child in children[:100]:
                        directory_row(child, _scan_data.totals.bytes)

                # Large files
                if _scan_data.large_files:
                    with tag.section():
                        with tag.h2():
                            text("Largest Files")

                        large_files = sorted(
                            _scan_data.large_files,
                            key=lambda f: f.size,
                            reverse=True,
                        )

                        for lf in large_files[:50]:
                            file_row(lf, _scan_data.totals.bytes)

    @app.get("/dir/{dir_index}")
    async def get_directory_children(dir_index: int):
        """Return complete details element with children loaded."""
        if not _scan_data:
            return ""

        dir_entry = _scan_data.directories[dir_index]
        directory_row_expanded(dir_entry, _scan_data.totals.bytes)


def directory_row(dir_entry: dump.Directory, total_bytes: int):
    """Display a directory row with htmx lazy-loading."""
    percent = (dir_entry.total_size / total_bytes * 100) if total_bytes > 0 else 0
    has_children = dir_entry.total_dirs > 0

    if not has_children:
        # Leaf directory
        with tag.div("leaf"):
            with tag.span("dir-name"):
                text(dir_entry.name or ".")
            with tag.span("dir-files"):
                text(format_number(dir_entry.total_files))
            with tag.span("dir-dirs"):
                text(format_number(dir_entry.total_dirs) if dir_entry.total_dirs > 0 else "—")
            with tag.span("dir-size"):
                text(format_size(dir_entry.total_size))
            with tag.span("dir-pct"):
                text(f"{percent:.1f}%")
    else:
        # Expandable directory with htmx lazy loading
        with tag.details(
            hx_get=f"/dir/{dir_entry.index}",
            hx_trigger="toggle once",
            hx_swap="outerHTML",
        ):
            with tag.summary():
                with tag.span("dir-name"):
                    text(dir_entry.name or ".")
                with tag.span("dir-files"):
                    text(format_number(dir_entry.total_files))
                with tag.span("dir-dirs"):
                    text(format_number(dir_entry.total_dirs))
                with tag.span("dir-size"):
                    text(format_size(dir_entry.total_size))
                with tag.span("dir-pct"):
                    text(f"{percent:.1f}%")


def directory_row_expanded(dir_entry: dump.Directory, total_bytes: int):
    """Display a directory with its children already rendered."""
    percent = (dir_entry.total_size / total_bytes * 100) if total_bytes > 0 else 0

    with tag.details(open=True):
        with tag.summary():
            with tag.span("dir-name"):
                text(dir_entry.name or ".")
            with tag.span("dir-files"):
                text(format_number(dir_entry.total_files))
            with tag.span("dir-dirs"):
                text(format_number(dir_entry.total_dirs))
            with tag.span("dir-size"):
                text(format_size(dir_entry.total_size))
            with tag.span("dir-pct"):
                text(f"{percent:.1f}%")

        # Render children sorted by size
        children = [d for d in _scan_data.directories if d.parent_index == dir_entry.index]
        children.sort(key=lambda d: d.total_size, reverse=True)

        for child in children[:100]:
            directory_row(child, total_bytes)


def file_row(lf: dump.LargeFile, total_bytes: int):
    """Display a large file row."""
    percent = (lf.size / total_bytes * 100) if total_bytes > 0 else 0

    with tag.div("file-row"):
        with tag.span("file-path"):
            text(lf.path or lf.name)
        with tag.span("file-size"):
            text(format_size(lf.size))
        with tag.span("file-pct"):
            text(f"{percent:.2f}%")
