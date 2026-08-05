"""Safe, progress-reporting directory archiving."""

import errno
import json
import os
import shutil
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from .deletion import delete_tree


ArchiveProgress = Callable[[Path, int, int], None]


@dataclass(frozen=True)
class ArchiveItem:
    source: Path
    relative_path: Path
    expected_bytes: int
    expected_entries: int


@dataclass(frozen=True)
class ArchiveFailure:
    phase: str
    path: Path
    error: OSError

    @property
    def is_permission_error(self) -> bool:
        return isinstance(self.error, PermissionError) or self.error.errno in {
            errno.EACCES,
            errno.EPERM,
        }


@dataclass(frozen=True)
class ArchiveResult:
    item: ArchiveItem
    destination: Path
    moved: bool
    copied: bool
    bytes_copied: int
    entries_copied: int
    method: str
    failure: ArchiveFailure | None = None


def mounted_volumes() -> list[Path]:
    """Return currently mounted macOS-style data volumes."""
    volumes_root = Path("/Volumes")
    try:
        volumes = [path for path in volumes_root.iterdir() if path.is_dir()]
    except OSError:
        return []
    return sorted(volumes, key=lambda path: path.name.casefold())


def create_archive_root(
    destination: Path,
    now: datetime | None = None,
) -> Path:
    """Create and return a unique timestamped archive root."""
    destination = Path(destination).expanduser().resolve(strict=True)
    if not destination.is_dir():
        raise NotADirectoryError(destination)

    timestamp = (now or datetime.now().astimezone()).strftime("%Y-%m-%d_%H-%M-%S%z")
    base_name = f"wtfs-archive-{timestamp}"
    for suffix in range(1000):
        name = base_name if suffix == 0 else f"{base_name}-{suffix + 1}"
        archive_root = destination / name
        try:
            archive_root.mkdir(mode=0o755)
        except FileExistsError:
            continue
        return archive_root
    raise FileExistsError(f"could not choose a unique archive name in {destination}")


def _add_owner_read_access(path: Path) -> bool:
    try:
        details = path.lstat()
    except OSError:
        return False
    if stat.S_ISLNK(details.st_mode):
        return False
    mode = stat.S_IMODE(details.st_mode) | stat.S_IRUSR
    if stat.S_ISDIR(details.st_mode):
        mode |= stat.S_IXUSR
    try:
        os.chmod(path, mode, follow_symlinks=False)
    except OSError:
        return False
    return True


def _copy_regular_file(
    source: str,
    destination: str,
    progress: ArchiveProgress | None,
) -> str:
    source_path = Path(source)
    destination_path = Path(destination)
    temporary = destination_path.with_name(
        f".{destination_path.name}.wtfs-partial-{os.getpid()}"
    )
    copied = 0

    def copy_contents() -> None:
        nonlocal copied
        with source_path.open("rb", buffering=0) as source_file:
            with temporary.open("xb", buffering=0) as destination_file:
                buffer = bytearray(8 * 1024 * 1024)
                view = memoryview(buffer)
                while count := source_file.readinto(buffer):
                    destination_file.write(view[:count])
                    copied += count
                    if progress is not None:
                        progress(source_path, count, 0)

    try:
        try:
            copy_contents()
        except PermissionError:
            if not _add_owner_read_access(source_path):
                raise
            copy_contents()
        os.replace(temporary, destination_path)
        shutil.copystat(source_path, destination_path, follow_symlinks=False)
        if progress is not None:
            progress(source_path, 0, 1)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return destination


def archive_item(
    item: ArchiveItem,
    archive_root: Path,
    progress: ArchiveProgress | None = None,
) -> ArchiveResult:
    """Move one directory into an archive, copying first across filesystems."""
    source = item.source
    destination = archive_root / item.relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() or destination.is_symlink():
        error = FileExistsError(errno.EEXIST, "archive destination exists", destination)
        return ArchiveResult(
            item,
            destination,
            False,
            False,
            0,
            0,
            "none",
            ArchiveFailure("prepare destination", destination, error),
        )

    try:
        os.rename(source, destination)
    except OSError as rename_error:
        if rename_error.errno != errno.EXDEV:
            return ArchiveResult(
                item,
                destination,
                False,
                False,
                0,
                0,
                "rename",
                ArchiveFailure("move", source, rename_error),
            )
    else:
        if progress is not None:
            progress(source, item.expected_bytes, item.expected_entries)
        return ArchiveResult(
            item,
            destination,
            True,
            False,
            item.expected_bytes,
            item.expected_entries,
            "rename",
        )

    copied_bytes = 0
    copied_entries = 0

    def record_progress(path: Path, byte_count: int, entry_count: int) -> None:
        nonlocal copied_bytes, copied_entries
        copied_bytes += byte_count
        copied_entries += entry_count
        if progress is not None:
            progress(path, byte_count, entry_count)

    try:
        shutil.copytree(
            source,
            destination,
            symlinks=True,
            copy_function=lambda src, dst: _copy_regular_file(
                src,
                dst,
                record_progress,
            ),
        )
    except (OSError, shutil.Error) as copy_error:
        if isinstance(copy_error, shutil.Error):
            recorded_errors = copy_error.args[0] if copy_error.args else []
            first_error = recorded_errors[0] if recorded_errors else None
            if first_error is not None:
                failed_source, _, reason = first_error
                if isinstance(reason, OSError):
                    error = reason
                elif "Permission denied" in str(reason):
                    error = PermissionError(errno.EACCES, str(reason), failed_source)
                else:
                    error = OSError(str(reason))
            else:
                error = OSError(str(copy_error))
        else:
            error = copy_error
        return ArchiveResult(
            item,
            destination,
            False,
            False,
            copied_bytes,
            copied_entries,
            "copy",
            ArchiveFailure("copy", source, error),
        )

    deletion = delete_tree(source)
    if not deletion.deleted:
        failure = deletion.failure
        error = failure.error if failure is not None else OSError("source cleanup failed")
        path = failure.path if failure is not None else source
        return ArchiveResult(
            item,
            destination,
            False,
            True,
            copied_bytes,
            copied_entries,
            "copy",
            ArchiveFailure("remove copied source", path, error),
        )

    return ArchiveResult(
        item,
        destination,
        True,
        True,
        copied_bytes,
        copied_entries,
        "copy",
    )


def write_manifest(
    archive_root: Path,
    source_root: Path,
    results: list[ArchiveResult],
) -> None:
    """Write a human-readable record of archive sources and outcomes."""
    manifest = {
        "created_at": datetime.now().astimezone().isoformat(),
        "source_root": str(source_root),
        "archive_root": str(archive_root),
        "items": [
            {
                "source": str(result.item.source),
                "relative_path": str(result.item.relative_path),
                "destination": str(result.destination),
                "moved": result.moved,
                "copied": result.copied,
                "bytes": result.bytes_copied,
                "entries": result.entries_copied,
                "method": result.method,
                "failure": (
                    {
                        "phase": result.failure.phase,
                        "path": str(result.failure.path),
                        "error": str(result.failure.error),
                    }
                    if result.failure is not None
                    else None
                ),
            }
            for result in results
        ],
    }
    temporary = archive_root / ".manifest.json.wtfs-partial"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n")
    os.replace(temporary, archive_root / "manifest.json")
