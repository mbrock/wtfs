"""Interactive TUI for wtfs using Textual."""

from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass
from collections import deque

from textual.app import App, ComposeResult
from textual.widgets import Tree
from textual.widgets.tree import TreeNode
from textual.binding import Binding

from .dump import WtfsDump, Directory, LargeFile, format_size


class DirectoryTreeView(Tree):
    """Hierarchical tree view of directories."""

    MIN_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10 MiB

    def __init__(self, data: WtfsDump, **kwargs):
        super().__init__("", **kwargs)
        self.data = data
        self.show_root = False

        # Pre-compute children mapping for fast lookup
        self.children_by_parent = {}
        for dir in self.data.directories[1:]:
            parent_idx = dir.parent_index
            if parent_idx not in self.children_by_parent:
                self.children_by_parent[parent_idx] = []
            self.children_by_parent[parent_idx].append(dir)

        # Sort each group by size (largest first)
        for parent_idx in self.children_by_parent:
            self.children_by_parent[parent_idx].sort(
                key=lambda d: d.total_size,
                reverse=True,
            )

    def on_mount(self) -> None:
        """Build the root node when mounted."""
        # Only create the root node initially
        root_dir = self.data.directories[0]
        label = self._format_label(root_dir)
        root_node = self.root.add(label, data=root_dir, allow_expand=True)

        # Add children to root immediately so it can be expanded
        self._add_children(root_node, 0)

        # Expand the root to show top-level directories
        root_node.expand()

    def _add_children(self, parent_node: TreeNode, parent_idx: int):
        """Lazily add children to a node, filtering out small directories."""
        if parent_idx in self.children_by_parent:
            for child_dir in self.children_by_parent[parent_idx]:
                # Skip directories smaller than threshold
                if child_dir.total_size < self.MIN_SIZE_THRESHOLD:
                    continue

                label = self._format_label(child_dir)
                # Only create the node, don't add its children yet
                has_children = child_dir.index in self.children_by_parent
                parent_node.add(label, data=child_dir, allow_expand=has_children)

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Load children when a node is expanded."""
        node = event.node

        # Only load children if not already loaded
        if not node.children and node.data:
            dir_idx = node.data.index
            self._add_children(node, dir_idx)

    def _format_label(self, directory: Directory) -> str:
        """Format a directory label with aligned columns."""
        # Show absolute path for root if available
        if directory.index == 0 and self.data.root_path:
            name = str(self.data.root_path)
        else:
            name = directory.name or '.'
        size_str = format_size(directory.total_size)
        files = directory.total_files

        # Use fixed widths for alignment
        # Format: name (40 chars) | size (10 chars right-aligned) | files (right-aligned)
        return f"[cyan]{name:<40}[/] [yellow]{size_str:>10}[/] [dim]{files:>8,} files[/]"


class WtfsApp(App):
    """Interactive TUI for browsing wtfs scan results."""

    CSS = """
    Screen {
        background: $surface;
    }

    DirectoryTreeView {
        height: 1fr;
        width: 1fr;
    }

    DirectoryTreeView > .tree--cursor {
        background: rgba(255, 255, 255, 0.05);
        color: $text;
    }

    DirectoryTreeView > .tree--highlight {
        background: transparent;
    }

    DirectoryTreeView > .tree--highlight-line {
        background: transparent;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", show=False),
        Binding("ctrl+c", "quit", "Quit", show=False),
    ]

    def __init__(self, data: WtfsDump):
        super().__init__()
        self.data = data

    def compose(self) -> ComposeResult:
        """Create UI layout."""
        yield DirectoryTreeView(self.data)


def run_interactive(dump_file: str, root_path: str = None) -> None:
    """Run the interactive TUI application."""
    from . import dump

    # Load data before starting the app
    data = dump.load(dump_file)

    # Set the root path if provided
    if root_path:
        data.root_path = Path(root_path).resolve()

    app = WtfsApp(data)
    app.run()


def main():
    """CLI entry point for interactive mode."""
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description='Interactive TUI for wtfs scan results',
    )
    parser.add_argument('dump', help='Binary dump file to browse')

    args = parser.parse_args()

    if not Path(args.dump).exists():
        print(f"Error: File not found: {args.dump}", file=sys.stderr)
        sys.exit(1)

    run_interactive(args.dump)


if __name__ == '__main__':
    main()

