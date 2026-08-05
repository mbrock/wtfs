"""Interactive TUI for wtfs using Textual."""

import shutil
from pathlib import Path
from typing import Iterable

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Static, Tree
from textual.widgets.tree import TreeNode
from textual.binding import Binding
from rich.markup import escape

from .dump import WtfsDump, Directory, format_size


def deletion_path(root_path: Path, directory: Directory) -> Path:
    """Return a safe absolute deletion path for a directory in a dump."""
    if directory.index == 0 or directory.path in (None, "."):
        raise ValueError("the scan root cannot be deleted")

    root = root_path.resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"scan root is not a directory: {root}")

    relative = Path(directory.path)
    if relative.is_absolute():
        raise ValueError(f"dump path is unexpectedly absolute: {directory.path}")
    if ".." in relative.parts:
        raise ValueError(f"directory escapes the scan root: {directory.path}")

    target = root
    for part in relative.parts:
        target /= part
        if target.is_symlink():
            raise ValueError(f"refusing to follow a symbolic link: {target}")

    resolved_target = target.resolve(strict=False)
    if resolved_target == root or not resolved_target.is_relative_to(root):
        raise ValueError(f"directory escapes the scan root: {directory.path}")
    return target


def outermost_directories(
    directories: Iterable[Directory],
    all_directories: list[Directory],
) -> list[Directory]:
    """Drop marked descendants when one of their ancestors is also marked."""
    marked = {directory.index: directory for directory in directories}
    outermost = []
    for directory in marked.values():
        parent_index = directory.parent_index
        seen = {directory.index}
        has_marked_ancestor = False
        while parent_index not in seen and 0 <= parent_index < len(all_directories):
            if parent_index in marked:
                has_marked_ancestor = True
                break
            if parent_index == 0:
                break
            seen.add(parent_index)
            parent_index = all_directories[parent_index].parent_index

        if not has_marked_ancestor:
            outermost.append(directory)
    return sorted(outermost, key=lambda directory: directory.path or "")


class ConfirmDeletion(ModalScreen[bool]):
    """Confirmation dialog for permanent directory deletion."""

    BINDINGS = [
        Binding("y", "confirm", "Delete", show=False),
        Binding("n", "cancel", "Cancel", show=False),
        Binding("escape", "cancel", "Cancel", show=False),
    ]

    CSS = """
    ConfirmDeletion {
        align: center middle;
    }

    ConfirmDeletion > Vertical {
        width: 72;
        max-width: 95%;
        height: auto;
        max-height: 90%;
        padding: 1 2;
        border: thick $error;
        background: $panel;
    }

    ConfirmDeletion .paths {
        height: auto;
        max-height: 16;
        margin: 1 0;
        overflow-y: auto;
    }

    ConfirmDeletion Horizontal {
        width: 100%;
        height: auto;
        align-horizontal: right;
    }

    ConfirmDeletion Button {
        margin-left: 1;
    }
    """

    def __init__(self, directories: list[Directory]):
        super().__init__()
        self.directories = directories

    def compose(self) -> ComposeResult:
        total_size = sum(directory.total_size for directory in self.directories)
        paths = "\n".join(
            f"  {escape(directory.path or directory.name)}"
            for directory in self.directories
        )
        count = len(self.directories)
        noun = "directory" if count == 1 else "directories"
        with Vertical():
            yield Static(
                f"[bold red]Permanently delete {count} {noun}?[/bold red]\n"
                f"Snapshot size: [yellow]{format_size(total_size)}[/yellow]"
            )
            yield Static(paths, classes="paths")
            yield Static("This cannot be undone. Press [bold]y[/bold] to confirm.")
            with Horizontal():
                yield Button("Cancel", id="cancel")
                yield Button("Delete", variant="error", id="delete")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "delete")


class DirectoryTreeView(Tree):
    """Hierarchical tree view of directories."""

    MIN_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10 MiB

    def __init__(self, data: WtfsDump, marked: set[int], **kwargs):
        super().__init__("", **kwargs)
        self.data = data
        self.marked = marked
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
        marker = (
            "[bold red][delete][/bold red] "
            if directory.index in self.marked
            else "         "
        )
        return (
            f"{marker}[cyan]{escape(name):<40}[/] "
            f"[yellow]{size_str:>10}[/] [dim]{files:>8,} files[/]"
        )


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
        Binding("m", "mark", "Mark/unmark"),
        Binding("d", "delete_marked", "Delete marked"),
        Binding("q", "quit", "Quit", show=False),
        Binding("ctrl+c", "quit", "Quit", show=False),
    ]

    def __init__(self, data: WtfsDump, root_path: Path | None = None):
        super().__init__()
        self.data = data
        self.root_path = root_path.resolve() if root_path is not None else None
        self.marked: set[int] = set()

    def compose(self) -> ComposeResult:
        """Create UI layout."""
        yield DirectoryTreeView(self.data, self.marked)
        yield Footer()

    def action_mark(self) -> None:
        tree = self.query_one(DirectoryTreeView)
        node = tree.cursor_node
        directory = node.data if node is not None else None
        if not isinstance(directory, Directory):
            return
        if self.root_path is None:
            self.notify(
                "Deletion is disabled: reopen this dump with --root PATH.",
                severity="warning",
            )
            return
        if directory.index == 0:
            self.notify(
                "The scan root cannot be marked for deletion.",
                severity="warning",
            )
            return

        if directory.index in self.marked:
            self.marked.remove(directory.index)
        else:
            try:
                deletion_path(self.root_path, directory)
            except (OSError, ValueError) as error:
                self.notify(str(error), severity="error")
                return
            self.marked.add(directory.index)
        node.set_label(tree._format_label(directory))

    def action_delete_marked(self) -> None:
        if not self.marked:
            self.notify("No directories are marked for deletion.")
            return
        if self.root_path is None:
            self.notify(
                "Deletion is disabled: reopen with --root PATH.",
                severity="warning",
            )
            return

        directories = outermost_directories(
            (self.data.directories[index] for index in self.marked),
            self.data.directories,
        )
        self.push_screen(
            ConfirmDeletion(directories),
            lambda confirmed: self._delete_marked(directories) if confirmed else None,
        )

    def _delete_marked(self, directories: list[Directory]) -> None:
        assert self.root_path is not None
        deleted = []
        failures = []
        for directory in directories:
            try:
                target = deletion_path(self.root_path, directory)
                shutil.rmtree(target)
            except (OSError, ValueError) as error:
                failures.append(f"{directory.path}: {error}")
            else:
                deleted.append(directory)

        deleted_indexes = {directory.index for directory in deleted}
        for directory in self.data.directories:
            parent_index = directory.index
            seen = set()
            while parent_index not in seen and 0 <= parent_index < len(self.data.directories):
                seen.add(parent_index)
                if parent_index in deleted_indexes:
                    self.marked.discard(directory.index)
                    break
                parent_index = self.data.directories[parent_index].parent_index

        tree = self.query_one(DirectoryTreeView)
        nodes = [tree.root]
        for node in nodes:
            nodes.extend(node.children)
        for node in nodes:
            if isinstance(node.data, Directory) and node.data.index in deleted_indexes:
                node.remove()

        if deleted:
            count = len(deleted)
            self.notify(f"Deleted {count} director{'y' if count == 1 else 'ies'}.")
        if failures:
            self.notify("\n".join(failures), severity="error", timeout=10)


def run_interactive(dump_file: str, root_path: str = None) -> None:
    """Run the interactive TUI application."""
    from . import dump

    # Load data before starting the app
    data = dump.load(dump_file)

    # Set the root path if provided
    if root_path:
        data.root_path = Path(root_path).resolve()

    app = WtfsApp(data, Path(root_path) if root_path else None)
    app.run()


def main():
    """CLI entry point for interactive mode."""
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description='Interactive TUI for wtfs scan results',
    )
    parser.add_argument('dump', help='Binary dump file to browse')
    parser.add_argument(
        '--root',
        help='Scanned root directory (required to enable deletion)',
    )

    args = parser.parse_args()

    if not Path(args.dump).exists():
        print(f"Error: File not found: {args.dump}", file=sys.stderr)
        sys.exit(1)

    run_interactive(args.dump, root_path=args.root)


if __name__ == '__main__':
    main()
