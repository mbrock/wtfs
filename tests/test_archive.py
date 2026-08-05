import errno
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from wtfs.archive import (
    ArchiveItem,
    archive_item,
    create_archive_root,
    write_manifest,
)
from wtfs.deletion import DeletionFailure, DeletionResult


class ArchiveRootTests(unittest.TestCase):
    def test_creates_unique_timestamped_roots(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            now = datetime(2026, 8, 5, 12, 34, 56, tzinfo=timezone.utc)

            first = create_archive_root(destination, now)
            second = create_archive_root(destination, now)

            self.assertEqual(first.name, "wtfs-archive-2026-08-05_12-34-56+0000")
            self.assertEqual(second.name, "wtfs-archive-2026-08-05_12-34-56+0000-2")


class ArchiveItemTests(unittest.TestCase):
    def test_same_filesystem_rename_preserves_relative_hierarchy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "contents").write_text("kept")
            archive_root = root / "archive"
            archive_root.mkdir()
            item = ArchiveItem(source, Path("Downloads/source"), 4, 2)
            progress = []

            result = archive_item(
                item,
                archive_root,
                lambda path, byte_count, entry_count: progress.append(
                    (path, byte_count, entry_count)
                ),
            )

            self.assertTrue(result.moved)
            self.assertEqual(result.method, "rename")
            self.assertFalse(source.exists())
            self.assertEqual(
                (archive_root / "Downloads/source/contents").read_text(),
                "kept",
            )
            self.assertEqual(sum(event[1] for event in progress), 4)

    def test_cross_filesystem_copy_preserves_symlinks_then_removes_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            (outside / "keep").write_text("outside")
            source = root / "source"
            source.mkdir()
            (source / "file").write_bytes(b"archived")
            (source / "link").symlink_to(outside, target_is_directory=True)
            archive_root = root / "archive"
            archive_root.mkdir()
            item = ArchiveItem(source, Path("source"), 8, 3)
            progress = []

            with patch(
                "wtfs.archive.os.rename",
                side_effect=OSError(errno.EXDEV, "Cross-device link"),
            ):
                result = archive_item(
                    item,
                    archive_root,
                    lambda path, byte_count, entry_count: progress.append(
                        (path, byte_count, entry_count)
                    ),
                )

            self.assertTrue(result.moved)
            self.assertTrue(result.copied)
            self.assertEqual(result.method, "copy")
            self.assertFalse(source.exists())
            self.assertEqual((archive_root / "source/file").read_bytes(), b"archived")
            self.assertTrue((archive_root / "source/link").is_symlink())
            self.assertTrue((outside / "keep").exists())
            self.assertEqual(sum(event[1] for event in progress), 8)

    def test_complete_copy_is_retained_when_source_cleanup_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "file").write_text("copy survives")
            archive_root = root / "archive"
            archive_root.mkdir()
            item = ArchiveItem(source, Path("source"), 13, 2)
            failure = DeletionFailure(
                source,
                source,
                "remove directory",
                PermissionError(errno.EACCES, "Permission denied", source),
            )
            failed_cleanup = DeletionResult(source, False, 0, 0, failure)

            with (
                patch(
                    "wtfs.archive.os.rename",
                    side_effect=OSError(errno.EXDEV, "Cross-device link"),
                ),
                patch("wtfs.archive.delete_tree", return_value=failed_cleanup),
            ):
                result = archive_item(item, archive_root)

            self.assertFalse(result.moved)
            self.assertTrue(result.copied)
            self.assertTrue(source.exists())
            self.assertEqual(
                (archive_root / "source/file").read_text(),
                "copy survives",
            )
            self.assertEqual(result.failure.phase, "remove copied source")

    def test_manifest_records_original_and_destination_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            archive_root = root / "archive"
            archive_root.mkdir()
            item = ArchiveItem(source, Path("source"), 0, 1)
            result = archive_item(item, archive_root)

            write_manifest(archive_root, root, [result])

            manifest = json.loads((archive_root / "manifest.json").read_text())
            self.assertEqual(manifest["source_root"], str(root))
            self.assertEqual(manifest["items"][0]["relative_path"], "source")
            self.assertTrue(manifest["items"][0]["moved"])


if __name__ == "__main__":
    unittest.main()
