import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wtfs.deletion import delete_tree


class DeleteTreeTests(unittest.TestCase):
    def test_deletes_tree_and_reports_entry_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target"
            (target / "nested").mkdir(parents=True)
            (target / "one").write_text("one")
            (target / "nested" / "two").write_text("two")
            progress = []

            result = delete_tree(
                target,
                lambda path, count: progress.append((path, count)),
                progress_interval=1,
            )

            self.assertTrue(result.deleted)
            self.assertEqual(result.entries_deleted, 4)
            self.assertEqual(sum(count for _, count in progress), 4)
            self.assertFalse(target.exists())

    def test_repairs_owner_access_on_read_only_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target"
            locked = target / "locked"
            locked.mkdir(parents=True)
            (locked / "contents").write_text("gone")
            os.chmod(locked, 0)

            result = delete_tree(target)

            self.assertTrue(result.deleted)
            self.assertGreaterEqual(result.permissions_repaired, 1)
            self.assertFalse(target.exists())

    def test_unlinks_symlink_without_deleting_its_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            (outside / "keep").write_text("still here")
            target = root / "target"
            target.mkdir()
            (target / "link").symlink_to(outside, target_is_directory=True)

            result = delete_tree(target)

            self.assertTrue(result.deleted)
            self.assertTrue((outside / "keep").exists())

    def test_returns_exact_permission_failure_after_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target"
            target.mkdir()

            with patch(
                "wtfs.deletion.os.scandir",
                side_effect=PermissionError(13, "Permission denied", target),
            ):
                result = delete_tree(target)

            self.assertFalse(result.deleted)
            self.assertIsNotNone(result.failure)
            self.assertTrue(result.failure.is_permission_error)
            self.assertEqual(result.failure.operation, "read directory")
            self.assertEqual(result.failure.path, target)


if __name__ == "__main__":
    unittest.main()
