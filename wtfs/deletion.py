"""Filesystem deletion with progress and conservative permission repair."""

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar


T = TypeVar("T")
ProgressCallback = Callable[[Path, int], None]


@dataclass(frozen=True)
class DeletionFailure:
    """A filesystem operation that prevented a tree from being deleted."""

    target: Path
    path: Path
    operation: str
    error: OSError

    @property
    def is_permission_error(self) -> bool:
        return isinstance(self.error, PermissionError) or self.error.errno in {
            errno.EACCES,
            errno.EPERM,
        }


@dataclass(frozen=True)
class DeletionResult:
    """Result of deleting one selected directory tree."""

    target: Path
    deleted: bool
    entries_deleted: int
    permissions_repaired: int
    failure: DeletionFailure | None = None


class _OperationFailed(Exception):
    def __init__(self, path: Path, operation: str, error: OSError):
        super().__init__(str(error))
        self.path = path
        self.operation = operation
        self.error = error


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _make_owner_accessible(path: Path) -> bool:
    """Add only owner access bits, without following symbolic links."""
    try:
        details = path.lstat()
    except OSError:
        return False
    if stat.S_ISLNK(details.st_mode):
        return False

    mode = stat.S_IMODE(details.st_mode) | stat.S_IRUSR | stat.S_IWUSR
    if stat.S_ISDIR(details.st_mode):
        mode |= stat.S_IXUSR
    try:
        os.chmod(path, mode, follow_symlinks=False)
    except OSError:
        return False
    return True


def _run_with_permission_retry(
    operation: str,
    path: Path,
    target: Path,
    function: Callable[[], T],
) -> tuple[T, int]:
    """Run an operation, repairing owner bits inside the target on permission failure."""
    try:
        return function(), 0
    except PermissionError:
        repaired = 0
        candidates = [path]
        if path != target and _is_within(path.parent, target):
            candidates.append(path.parent)
        for candidate in candidates:
            repaired += int(_make_owner_accessible(candidate))

        try:
            return function(), repaired
        except OSError as error:
            raise _OperationFailed(path, operation, error) from error
    except OSError as error:
        raise _OperationFailed(path, operation, error) from error


def delete_tree(
    target: Path,
    progress: ProgressCallback | None = None,
    progress_interval: int = 512,
) -> DeletionResult:
    """Delete a directory tree without following links, reporting incremental progress."""
    target = Path(target)
    entries_deleted = 0
    reported_entries = 0
    permissions_repaired = 0

    def report(force: bool = False) -> None:
        nonlocal reported_entries
        delta = entries_deleted - reported_entries
        if progress is not None and (force or delta >= progress_interval):
            progress(target, delta)
            reported_entries = entries_deleted

    try:
        try:
            target_details = target.lstat()
        except FileNotFoundError:
            return DeletionResult(target, True, 0, 0)
        except OSError as error:
            raise _OperationFailed(target, "inspect target", error) from error
        if stat.S_ISLNK(target_details.st_mode) or not stat.S_ISDIR(
            target_details.st_mode
        ):
            error = OSError(errno.EINVAL, "target is not a real directory", target)
            raise _OperationFailed(target, "validate", error)

        stack: list[tuple[Path, bool]] = [(target, False)]
        while stack:
            path, visited = stack.pop()
            if visited:
                _, repaired = _run_with_permission_retry(
                    "remove directory",
                    path,
                    target,
                    lambda path=path: os.rmdir(path),
                )
                permissions_repaired += repaired
                entries_deleted += 1
                report()
                continue

            try:
                path_details = path.lstat()
            except OSError as error:
                raise _OperationFailed(path, "inspect directory", error) from error
            if stat.S_ISLNK(path_details.st_mode) or not stat.S_ISDIR(
                path_details.st_mode
            ):
                error = OSError(
                    errno.ELOOP,
                    "directory changed into a symbolic link during deletion",
                    path,
                )
                raise _OperationFailed(path, "validate directory", error)

            entries, repaired = _run_with_permission_retry(
                "read directory",
                path,
                target,
                lambda path=path: list(os.scandir(path)),
            )
            permissions_repaired += repaired
            stack.append((path, True))

            for entry in entries:
                child = Path(entry.path)
                is_directory, repaired = _run_with_permission_retry(
                    "inspect entry",
                    child,
                    target,
                    lambda entry=entry: entry.is_dir(follow_symlinks=False),
                )
                permissions_repaired += repaired
                if is_directory:
                    stack.append((child, False))
                else:
                    _, repaired = _run_with_permission_retry(
                        "remove file",
                        child,
                        target,
                        lambda child=child: os.unlink(child),
                    )
                    permissions_repaired += repaired
                    entries_deleted += 1
                    report()

        report(force=True)
        return DeletionResult(
            target,
            True,
            entries_deleted,
            permissions_repaired,
        )
    except _OperationFailed as failure:
        report(force=True)
        return DeletionResult(
            target,
            False,
            entries_deleted,
            permissions_repaired,
            DeletionFailure(
                target,
                failure.path,
                failure.operation,
                failure.error,
            ),
        )
