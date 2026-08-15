"""Interactive TUI for wtfs using Textual."""

import os
import shlex
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Footer,
    Input,
    LoadingIndicator,
    ProgressBar,
    Select,
    Static,
    Tree,
)
from textual.widgets.tree import TreeNode
from textual.binding import Binding
from rich.markup import escape

from .archive import (
    ArchiveFailure,
    ArchiveItem,
    ArchiveResult,
    archive_item,
    create_archive_root,
    mounted_volumes,
    write_manifest,
)
from .deletion import DeletionFailure, DeletionResult, delete_tree
from .dump import Directory, LargeFile, WtfsDump, format_size


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


class DeletionProgress(ModalScreen[None]):
    """Non-dismissible progress display while filesystem deletion is active."""

    CSS = """
    DeletionProgress {
        align: center middle;
    }

    DeletionProgress > Vertical {
        width: 72;
        max-width: 95%;
        height: auto;
        padding: 1 2;
        border: thick $warning;
        background: $panel;
    }

    DeletionProgress LoadingIndicator {
        height: 3;
    }

    DeletionProgress ProgressBar {
        margin: 1 0;
    }
    """

    def __init__(self, directories: list[Directory]):
        super().__init__()
        self.directories = directories
        self.expected_entries = sum(
            max(1, directory.total_files + directory.total_dirs)
            for directory in directories
        )
        self.entries_deleted = 0
        self.targets_finished = 0

    def compose(self) -> ComposeResult:
        count = len(self.directories)
        yield Vertical(
            Static(
                f"[bold]Deleting {count} "
                f"director{'y' if count == 1 else 'ies'}…[/bold]"
            ),
            LoadingIndicator(),
            ProgressBar(total=self.expected_entries, show_eta=False),
            Static("Starting…", id="deletion-status"),
        )

    def advance_entries(self, target: Path, count: int) -> None:
        """Record deleted filesystem entries on the UI thread."""
        self.entries_deleted += count
        progress = self.query_one(ProgressBar)
        if self.entries_deleted > self.expected_entries:
            progress.update(total=self.entries_deleted)
        progress.update(progress=self.entries_deleted)
        self.query_one("#deletion-status", Static).update(
            f"Removed {self.entries_deleted:,} entries · {escape(str(target))}"
        )

    def target_finished(self, target: Path) -> None:
        """Record completion of one independently selected target."""
        self.targets_finished += 1
        self.query_one("#deletion-status", Static).update(
            f"Finished {self.targets_finished}/{len(self.directories)} targets · "
            f"{self.entries_deleted:,} entries removed · {escape(str(target))}"
        )


@dataclass(frozen=True)
class DirectoryDeletionResult:
    directory: Directory
    result: DeletionResult


class DeletionReport(ModalScreen[None]):
    """Durable summary explaining partial deletion failures and next steps."""

    BINDINGS = [
        Binding("enter", "close", "Close", show=False),
        Binding("escape", "close", "Close", show=False),
    ]

    CSS = """
    DeletionReport {
        align: center middle;
    }

    DeletionReport > Vertical {
        width: 84;
        max-width: 96%;
        height: auto;
        max-height: 92%;
        padding: 1 2;
        border: thick $error;
        background: $panel;
    }

    DeletionReport .details {
        height: auto;
        max-height: 24;
        margin: 1 0;
        overflow-y: auto;
    }

    DeletionReport Button {
        align-horizontal: right;
    }
    """

    def __init__(self, results: list[DirectoryDeletionResult]):
        super().__init__()
        self.results = results

    @staticmethod
    def _failure_text(failure: DeletionFailure) -> str:
        operation = escape(failure.operation)
        path = escape(str(failure.path))
        message = escape(str(failure.error))
        lines = [f"[bold red]{operation} failed[/bold red]: {path}", f"  {message}"]
        if failure.is_permission_error:
            inspect_command = escape(f"ls -ldeO {shlex.quote(str(failure.path))}")
            lines.extend(
                [
                    "  wtfs already retried after adding owner access bits inside the target.",
                    f"  Inspect ownership, ACLs, and flags: [bold]{inspect_command}[/bold]",
                    "  Fix the owning directory's permissions or ACLs. On macOS, protected",
                    "  locations may require Full Disk Access for the terminal application.",
                ]
            )
        return "\n".join(lines)

    def compose(self) -> ComposeResult:
        succeeded = sum(result.result.deleted for result in self.results)
        failed = len(self.results) - succeeded
        details = "\n\n".join(
            self._failure_text(result.result.failure)
            for result in self.results
            if result.result.failure is not None
        )
        with Vertical():
            yield Static(
                f"[bold red]Deletion incomplete[/bold red]\n"
                f"Deleted {succeeded} targets; {failed} failed."
            )
            yield Static(details, classes="details")
            yield Static(
                "Failed targets remain marked. Correct the problem, then press "
                "[bold]d[/bold] to retry. Some contents may already be gone."
            )
            yield Button("Close", variant="primary", id="close")

    def action_close(self) -> None:
        self.dismiss()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


class ArchiveDestination(ModalScreen[Path | None]):
    """Choose a mounted volume or existing directory for a new archive."""

    BINDINGS = [Binding("escape", "cancel", "Cancel", show=False)]

    CSS = """
    ArchiveDestination {
        align: center middle;
    }

    ArchiveDestination > Vertical {
        width: 76;
        max-width: 95%;
        height: auto;
        padding: 1 2;
        border: thick $accent;
        background: $panel;
    }

    ArchiveDestination Select, ArchiveDestination Input {
        margin: 1 0;
    }

    ArchiveDestination .error {
        color: $error;
        height: auto;
    }

    ArchiveDestination Horizontal {
        width: 100%;
        height: auto;
        align-horizontal: right;
    }

    ArchiveDestination Button {
        margin-left: 1;
    }
    """

    def __init__(self, volumes: list[Path]):
        super().__init__()
        self.volumes = volumes

    def compose(self) -> ComposeResult:
        default = str(self.volumes[0]) if self.volumes else str(Path.home())
        options = [(f"{volume.name} — {volume}", str(volume)) for volume in self.volumes]
        with Vertical():
            yield Static(
                "[bold]Archive marked directories[/bold]\n"
                "Choose a mounted volume or enter any existing destination folder. "
                "wtfs will create a timestamped archive root there."
            )
            if options:
                yield Select(
                    options,
                    value=default,
                    allow_blank=False,
                    id="archive-volume",
                )
            yield Input(default, placeholder="Destination folder", id="archive-path")
            yield Static("", id="archive-destination-error", classes="error")
            with Horizontal():
                yield Button("Cancel", id="cancel")
                yield Button("Archive", variant="primary", id="archive")

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "archive-volume" and isinstance(event.value, str):
            self.query_one("#archive-path", Input).value = event.value

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return

        raw_path = self.query_one("#archive-path", Input).value.strip()
        destination = Path(raw_path).expanduser()
        error = self.query_one("#archive-destination-error", Static)
        if not raw_path:
            error.update("Enter a destination folder.")
        elif not destination.exists():
            error.update(f"Destination does not exist: {escape(str(destination))}")
        elif not destination.is_dir():
            error.update(f"Destination is not a directory: {escape(str(destination))}")
        else:
            self.dismiss(destination.resolve())


class ArchiveProgressScreen(ModalScreen[None]):
    """Non-dismissible byte and target progress for an archive operation."""

    CSS = """
    ArchiveProgressScreen {
        align: center middle;
    }

    ArchiveProgressScreen > Vertical {
        width: 76;
        max-width: 95%;
        height: auto;
        padding: 1 2;
        border: thick $accent;
        background: $panel;
    }

    ArchiveProgressScreen LoadingIndicator {
        height: 3;
    }

    ArchiveProgressScreen ProgressBar {
        margin: 1 0;
    }
    """

    def __init__(self, directories: list[Directory], destination: Path):
        super().__init__()
        self.directories = directories
        self.destination = destination
        self.expected_bytes = max(1, sum(item.total_size for item in directories))
        self.bytes_copied = 0
        self.entries_copied = 0
        self.targets_finished = 0

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static(
                f"[bold]Archiving {len(self.directories)} marked "
                f"director{'y' if len(self.directories) == 1 else 'ies'}…[/bold]"
            ),
            LoadingIndicator(),
            ProgressBar(total=self.expected_bytes, show_eta=False),
            Static(
                f"Preparing archive in {escape(str(self.destination))}",
                id="archive-status",
            ),
        )

    def set_archive_root(self, archive_root: Path) -> None:
        self.query_one("#archive-status", Static).update(
            f"Archive root: {escape(str(archive_root))}"
        )

    def advance(self, path: Path, byte_count: int, entry_count: int) -> None:
        self.bytes_copied += byte_count
        self.entries_copied += entry_count
        progress = self.query_one(ProgressBar)
        if self.bytes_copied > self.expected_bytes:
            progress.update(total=self.bytes_copied)
        progress.update(progress=self.bytes_copied)
        self.query_one("#archive-status", Static).update(
            f"{format_size(self.bytes_copied)} · {self.entries_copied:,} files · "
            f"{escape(str(path))}"
        )

    def target_finished(self, path: Path) -> None:
        self.targets_finished += 1
        self.query_one("#archive-status", Static).update(
            f"Finished {self.targets_finished}/{len(self.directories)} targets · "
            f"{format_size(self.bytes_copied)} · {escape(str(path))}"
        )


@dataclass(frozen=True)
class DirectoryArchiveResult:
    directory: Directory
    result: ArchiveResult


@dataclass(frozen=True)
class ArchiveBatchResult:
    requested_destination: Path
    archive_root: Path | None
    results: list[DirectoryArchiveResult]
    failure: ArchiveFailure | None = None


class ArchiveReport(ModalScreen[None]):
    """Persistent archive destination and partial-failure report."""

    BINDINGS = [
        Binding("enter", "close", "Close", show=False),
        Binding("escape", "close", "Close", show=False),
    ]

    CSS = """
    ArchiveReport {
        align: center middle;
    }

    ArchiveReport > Vertical {
        width: 86;
        max-width: 96%;
        height: auto;
        max-height: 92%;
        padding: 1 2;
        border: thick $accent;
        background: $panel;
    }

    ArchiveReport .details {
        height: auto;
        max-height: 25;
        margin: 1 0;
        overflow-y: auto;
    }
    """

    def __init__(self, batch: ArchiveBatchResult):
        super().__init__()
        self.batch = batch

    @staticmethod
    def _failure_text(failure: ArchiveFailure) -> str:
        lines = [
            f"[bold red]{escape(failure.phase)} failed[/bold red]: "
            f"{escape(str(failure.path))}",
            f"  {escape(str(failure.error))}",
        ]
        if failure.is_permission_error:
            command = escape(f"ls -ldeO {shlex.quote(str(failure.path))}")
            lines.extend(
                [
                    f"  Inspect ownership, ACLs, and flags: [bold]{command}[/bold]",
                    "  Fix access or grant the terminal Full Disk Access on macOS, then retry.",
                ]
            )
        return "\n".join(lines)

    def compose(self) -> ComposeResult:
        moved = sum(item.result.moved for item in self.batch.results)
        failed_results = [item for item in self.batch.results if not item.result.moved]
        root = self.batch.archive_root or self.batch.requested_destination
        detail_parts = []
        if self.batch.failure is not None:
            detail_parts.append(self._failure_text(self.batch.failure))
        for item in failed_results:
            if item.result.failure is not None:
                detail_parts.append(self._failure_text(item.result.failure))
                if item.result.copied:
                    detail_parts.append(
                        "  A complete copy exists at "
                        f"{escape(str(item.result.destination))}, but the source remains."
                    )
            if not item.result.copied and item.result.destination.exists():
                detail_parts.append(
                    f"Partial destination retained: {escape(str(item.result.destination))}"
                )

        with Vertical():
            complete = not failed_results and self.batch.failure is None
            heading = "Archive complete" if complete else "Archive incomplete"
            heading_style = "bold green" if complete else "bold red"
            yield Static(
                f"[{heading_style}]{heading}[/]\n"
                f"Moved {moved}/{len(self.batch.results)} targets.\n"
                f"Archive root: [bold]{escape(str(root))}[/bold]"
            )
            if detail_parts:
                yield Static("\n\n".join(detail_parts), classes="details")
                yield Static(
                    "Failed sources remain marked. Fix the problem and press "
                    "[bold]a[/bold] to create a new archive attempt."
                )
            elif self.batch.archive_root is not None:
                yield Static("A manifest.json file records the original paths and outcomes.")
            yield Button("Close", variant="primary", id="close")

    def action_close(self) -> None:
        self.dismiss()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


class DirectoryTreeView(Tree):
    """Hierarchical tree view of directories and their recorded large files."""

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

        self.large_files_by_directory = {}
        for large_file in self.data.large_files:
            self.large_files_by_directory.setdefault(
                large_file.directory_index,
                [],
            ).append(large_file)

        for directory_idx in self.large_files_by_directory:
            self.large_files_by_directory[directory_idx].sort(
                key=lambda large_file: large_file.size,
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
        """Lazily add large files and non-trivial child directories."""
        entries = [
            (child_dir.total_size, child_dir)
            for child_dir in self.children_by_parent.get(parent_idx, [])
            if child_dir.total_size >= self.MIN_SIZE_THRESHOLD
        ]
        entries.extend(
            (large_file.size, large_file)
            for large_file in self.large_files_by_directory.get(parent_idx, [])
        )
        entries.sort(key=lambda entry: entry[0], reverse=True)

        for _, entry in entries:
            if isinstance(entry, Directory):
                child_dir = entry
                label = self._format_label(child_dir)
                # Only create the node, don't add its children yet
                has_children = (
                    child_dir.index in self.children_by_parent
                    or child_dir.index in self.large_files_by_directory
                )
                parent_node.add(label, data=child_dir, allow_expand=has_children)
            else:
                parent_node.add(
                    self._format_large_file_label(entry),
                    data=entry,
                    allow_expand=False,
                )

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Load children when a node is expanded."""
        node = event.node

        # Only load children if not already loaded
        if not node.children and isinstance(node.data, Directory):
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
            "[bold yellow][marked][/bold yellow] "
            if directory.index in self.marked
            else "         "
        )
        return (
            f"{marker}[cyan]{escape(name):<40}[/] "
            f"[yellow]{size_str:>10}[/] [dim]{files:>8,} files[/]"
        )

    def _format_large_file_label(self, large_file: LargeFile) -> str:
        """Format a recorded large file as a leaf in its owning directory."""
        size_str = format_size(large_file.size)
        return (
            f"         [bright_magenta]{escape(large_file.name):<40}[/] "
            f"[yellow]{size_str:>10}[/] [dim]large file[/]"
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
        Binding("a", "archive_marked", "Archive marked"),
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

    def action_archive_marked(self) -> None:
        if not self.marked:
            self.notify("No directories are marked for archiving.")
            return
        if self.root_path is None:
            self.notify(
                "Archiving is disabled: reopen with --root PATH.",
                severity="warning",
            )
            return

        directories = outermost_directories(
            (self.data.directories[index] for index in self.marked),
            self.data.directories,
        )
        self.push_screen(
            ArchiveDestination(mounted_volumes()),
            lambda destination: (
                self._start_archive(directories, destination)
                if destination is not None
                else None
            ),
        )

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
            lambda confirmed: self._start_deletion(directories) if confirmed else None,
        )

    def _start_deletion(self, directories: list[Directory]) -> None:
        progress = DeletionProgress(directories)
        self.push_screen(progress)
        self.call_after_refresh(self._launch_deletion_worker, directories, progress)

    def _launch_deletion_worker(
        self,
        directories: list[Directory],
        progress: DeletionProgress,
    ) -> None:
        self.run_worker(
            lambda: self._delete_in_background(directories, progress),
            name="delete-marked-directories",
            group="deletion",
            exclusive=True,
            thread=True,
        )

    def _delete_in_background(
        self,
        directories: list[Directory],
        progress: DeletionProgress,
    ) -> None:
        assert self.root_path is not None
        results: list[DirectoryDeletionResult] = []
        pending = {}

        worker_count = min(4, len(directories), os.cpu_count() or 1)
        executor = ThreadPoolExecutor(
            max_workers=max(1, worker_count),
            thread_name_prefix="wtfs-delete",
        )
        try:
            for directory in directories:
                try:
                    target = deletion_path(self.root_path, directory)
                except (OSError, ValueError) as error:
                    os_error = error if isinstance(error, OSError) else OSError(str(error))
                    result = DeletionResult(
                        self.root_path,
                        False,
                        0,
                        0,
                        DeletionFailure(
                            self.root_path,
                            self.root_path,
                            "validate target",
                            os_error,
                        ),
                    )
                    results.append(DirectoryDeletionResult(directory, result))
                    self.call_from_thread(progress.target_finished, self.root_path)
                    continue

                future = executor.submit(
                    delete_tree,
                    target,
                    lambda path, count: self.call_from_thread(
                        progress.advance_entries,
                        path,
                        count,
                    ),
                )
                pending[future] = directory

            for future in as_completed(pending):
                directory = pending[future]
                try:
                    result = future.result()
                except Exception as error:
                    target = deletion_path(self.root_path, directory)
                    os_error = OSError(f"unexpected deletion failure: {error}")
                    result = DeletionResult(
                        target,
                        False,
                        0,
                        0,
                        DeletionFailure(
                            target,
                            target,
                            "delete directory",
                            os_error,
                        ),
                    )
                results.append(DirectoryDeletionResult(directory, result))
                self.call_from_thread(progress.target_finished, result.target)
        finally:
            executor.shutdown(wait=True)

        order = {directory.index: index for index, directory in enumerate(directories)}
        results.sort(key=lambda item: order[item.directory.index])
        self.call_from_thread(self._finish_deletion, results, progress)

    def _finish_deletion(
        self,
        results: list[DirectoryDeletionResult],
        progress: DeletionProgress,
    ) -> None:
        if self.screen is progress:
            self.pop_screen()

        deleted = [item.directory for item in results if item.result.deleted]
        self._remove_completed_directories(deleted)

        if deleted:
            count = len(deleted)
            entries = sum(item.result.entries_deleted for item in results)
            repairs = sum(item.result.permissions_repaired for item in results)
            repair_note = (
                f" Repaired permissions on {repairs} entries."
                if repairs
                else ""
            )
            self.notify(
                f"Deleted {count} director{'y' if count == 1 else 'ies'} "
                f"({entries:,} entries).{repair_note}"
            )

        if any(not item.result.deleted for item in results):
            self.push_screen(DeletionReport(results))

    def _remove_completed_directories(self, completed: list[Directory]) -> None:
        """Remove completed directory subtrees from marks and the stale scan view."""
        completed_indexes = {directory.index for directory in completed}
        for directory in self.data.directories:
            parent_index = directory.index
            seen = set()
            while parent_index not in seen and 0 <= parent_index < len(self.data.directories):
                seen.add(parent_index)
                if parent_index in completed_indexes:
                    self.marked.discard(directory.index)
                    break
                parent_index = self.data.directories[parent_index].parent_index

        tree = self.query_one(DirectoryTreeView)
        nodes = [tree.root]
        for node in nodes:
            nodes.extend(node.children)
        for node in nodes:
            if isinstance(node.data, Directory) and node.data.index in completed_indexes:
                node.remove()

    def _start_archive(
        self,
        directories: list[Directory],
        destination: Path,
    ) -> None:
        assert self.root_path is not None
        for directory in directories:
            try:
                source = deletion_path(self.root_path, directory)
            except (OSError, ValueError) as error:
                self.notify(str(error), severity="error")
                return
            if destination == source or destination.is_relative_to(source):
                self.notify(
                    f"Archive destination is inside marked source: {source}",
                    severity="error",
                    timeout=10,
                )
                return

        progress = ArchiveProgressScreen(directories, destination)
        self.push_screen(progress)
        self.call_after_refresh(
            self._launch_archive_worker,
            directories,
            destination,
            progress,
        )

    def _launch_archive_worker(
        self,
        directories: list[Directory],
        destination: Path,
        progress: ArchiveProgressScreen,
    ) -> None:
        self.run_worker(
            lambda: self._archive_in_background(directories, destination, progress),
            name="archive-marked-directories",
            group="archive",
            exclusive=True,
            thread=True,
        )

    def _archive_in_background(
        self,
        directories: list[Directory],
        destination: Path,
        progress: ArchiveProgressScreen,
    ) -> None:
        assert self.root_path is not None
        try:
            archive_root = create_archive_root(destination)
        except OSError as error:
            batch = ArchiveBatchResult(
                destination,
                None,
                [],
                ArchiveFailure("create archive root", destination, error),
            )
            self.call_from_thread(self._finish_archive, batch, progress)
            return

        self.call_from_thread(progress.set_archive_root, archive_root)
        results: list[DirectoryArchiveResult] = []
        pending = {}
        executor = ThreadPoolExecutor(
            max_workers=max(1, min(2, len(directories))),
            thread_name_prefix="wtfs-archive",
        )
        try:
            for directory in directories:
                source = deletion_path(self.root_path, directory)
                relative = Path(directory.path or directory.name)
                item = ArchiveItem(
                    source,
                    relative,
                    directory.total_size,
                    directory.total_files + directory.total_dirs,
                )
                future = executor.submit(
                    archive_item,
                    item,
                    archive_root,
                    lambda path, byte_count, entry_count: self.call_from_thread(
                        progress.advance,
                        path,
                        byte_count,
                        entry_count,
                    ),
                )
                pending[future] = directory

            for future in as_completed(pending):
                directory = pending[future]
                try:
                    result = future.result()
                except Exception as error:
                    source = deletion_path(self.root_path, directory)
                    item = ArchiveItem(
                        source,
                        Path(directory.path or directory.name),
                        directory.total_size,
                        directory.total_files + directory.total_dirs,
                    )
                    os_error = OSError(f"unexpected archive failure: {error}")
                    result = ArchiveResult(
                        item,
                        archive_root / item.relative_path,
                        False,
                        False,
                        0,
                        0,
                        "none",
                        ArchiveFailure("archive", source, os_error),
                    )
                results.append(DirectoryArchiveResult(directory, result))
                self.call_from_thread(progress.target_finished, result.item.source)
        finally:
            executor.shutdown(wait=True)

        order = {directory.index: index for index, directory in enumerate(directories)}
        results.sort(key=lambda item: order[item.directory.index])
        manifest_failure = None
        try:
            write_manifest(
                archive_root,
                self.root_path,
                [item.result for item in results],
            )
        except OSError as error:
            manifest_failure = ArchiveFailure(
                "write manifest",
                archive_root / "manifest.json",
                error,
            )

        batch = ArchiveBatchResult(
            destination,
            archive_root,
            results,
            manifest_failure,
        )
        self.call_from_thread(self._finish_archive, batch, progress)

    def _finish_archive(
        self,
        batch: ArchiveBatchResult,
        progress: ArchiveProgressScreen,
    ) -> None:
        if self.screen is progress:
            self.pop_screen()
        moved = [item.directory for item in batch.results if item.result.moved]
        self._remove_completed_directories(moved)
        self.push_screen(ArchiveReport(batch))


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
