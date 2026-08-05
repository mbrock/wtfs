import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from textual.widgets import Input

from wtfs.deletion import DeletionFailure, DeletionResult, delete_tree
from wtfs.dump import Directory, DirectoryTotals, WtfsDump
from wtfs.tui import (
    ArchiveDestination,
    ArchiveReport,
    ConfirmDeletion,
    DeletionReport,
    DeletionProgress,
    DirectoryTreeView,
    WtfsApp,
    deletion_path,
    outermost_directories,
)


def directory(index, parent_index, path):
    return Directory(
        index=index,
        parent_index=parent_index,
        name=Path(path).name,
        total_size=0,
        total_files=0,
        total_dirs=0,
        path=path,
    )


class DeletionPathTests(unittest.TestCase):
    def test_resolves_directory_beneath_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "one" / "two"
            target.mkdir(parents=True)

            self.assertEqual(
                deletion_path(root, directory(2, 1, "./one/two")),
                target.resolve(),
            )

    def test_rejects_scan_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "scan root"):
                deletion_path(Path(temporary), directory(0, 0, "."))

    def test_rejects_path_outside_scan_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "escapes"):
                deletion_path(Path(temporary), directory(1, 0, "../outside"))

    def test_rejects_symbolic_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "real").mkdir()
            (root / "link").symlink_to(root / "real", target_is_directory=True)

            with self.assertRaisesRegex(ValueError, "symbolic link"):
                deletion_path(root, directory(1, 0, "./link"))


class OutermostDirectoriesTests(unittest.TestCase):
    def test_removes_descendants_of_marked_directories(self):
        directories = [
            directory(0, 0, "."),
            directory(1, 0, "./one"),
            directory(2, 1, "./one/two"),
            directory(3, 0, "./three"),
        ]

        self.assertEqual(
            outermost_directories(
                [directories[2], directories[3], directories[1]],
                directories,
            ),
            [directories[1], directories[3]],
        )


class DeletionFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_marks_reviews_and_deletes_selected_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "discard-me"
            target.mkdir()
            (target / "contents").write_text("gone")

            data = WtfsDump()
            data.totals = DirectoryTotals(2, 1, 20 * 1024 * 1024)
            data.directories = [
                directory(0, 0, "."),
                directory(1, 0, "./discard-me"),
            ]
            data.directories[1].total_size = 20 * 1024 * 1024

            app = WtfsApp(data, root)
            async with app.run_test() as pilot:
                await pilot.pause()
                tree = app.query_one(DirectoryTreeView)
                tree.move_cursor(tree.root.children[0].children[0])

                app.action_mark()
                self.assertEqual(app.marked, {1})

                app.action_delete_marked()
                await pilot.pause()
                self.assertIsInstance(app.screen, ConfirmDeletion)

                await pilot.press("y")
                await app.workers.wait_for_complete()
                await pilot.pause()
                self.assertFalse(target.exists())
                self.assertEqual(app.marked, set())

    async def test_shows_progress_while_deletion_is_running(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "discard-me"
            target.mkdir()

            data = WtfsDump()
            data.totals = DirectoryTotals(2, 0, 20 * 1024 * 1024)
            data.directories = [
                directory(0, 0, "."),
                directory(1, 0, "./discard-me"),
            ]
            data.directories[1].total_size = 20 * 1024 * 1024

            started = threading.Event()
            release = threading.Event()

            def delayed_delete(path, progress):
                started.set()
                release.wait(timeout=2)
                return delete_tree(path, progress)

            app = WtfsApp(data, root)
            with patch("wtfs.tui.delete_tree", side_effect=delayed_delete):
                async with app.run_test() as pilot:
                    await pilot.pause()
                    tree = app.query_one(DirectoryTreeView)
                    tree.move_cursor(tree.root.children[0].children[0])
                    app.action_mark()
                    app.action_delete_marked()
                    await pilot.pause()
                    await pilot.press("y")
                    await pilot.pause()

                    self.assertTrue(started.wait(timeout=1))
                    self.assertIsInstance(app.screen, DeletionProgress)

                    release.set()
                    await app.workers.wait_for_complete()
                    await pilot.pause()
                    self.assertFalse(target.exists())

    async def test_permission_failure_stays_marked_and_opens_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "keep-me"
            target.mkdir()

            data = WtfsDump()
            data.totals = DirectoryTotals(2, 0, 20 * 1024 * 1024)
            data.directories = [
                directory(0, 0, "."),
                directory(1, 0, "./keep-me"),
            ]
            data.directories[1].total_size = 20 * 1024 * 1024
            failure = DeletionFailure(
                target,
                target / "locked",
                "remove file",
                PermissionError(13, "Permission denied", target / "locked"),
            )
            failed_result = DeletionResult(target, False, 0, 1, failure)

            app = WtfsApp(data, root)
            with patch("wtfs.tui.delete_tree", return_value=failed_result):
                async with app.run_test() as pilot:
                    await pilot.pause()
                    tree = app.query_one(DirectoryTreeView)
                    tree.move_cursor(tree.root.children[0].children[0])
                    app.action_mark()
                    app.action_delete_marked()
                    await pilot.pause()
                    await pilot.press("y")
                    await app.workers.wait_for_complete()
                    await pilot.pause()

                    self.assertIsInstance(app.screen, DeletionReport)
                    self.assertEqual(app.marked, {1})
                    self.assertTrue(target.exists())
                    report = DeletionReport._failure_text(failure)
                    self.assertIn("Permission denied", report)
                    self.assertIn("Full Disk Access", report)


class ArchiveFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_prompts_for_destination_and_archives_mirrored_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "scanned"
            target = root / "Downloads" / "later"
            target.mkdir(parents=True)
            (target / "contents").write_text("saved")
            destination = Path(temporary) / "destination"
            destination.mkdir()

            data = WtfsDump()
            data.totals = DirectoryTotals(3, 1, 20 * 1024 * 1024)
            data.directories = [
                directory(0, 0, "."),
                directory(1, 0, "./Downloads"),
                directory(2, 1, "./Downloads/later"),
            ]
            data.directories[1].total_size = 20 * 1024 * 1024
            data.directories[2].total_size = 20 * 1024 * 1024

            app = WtfsApp(data, root)
            with patch("wtfs.tui.mounted_volumes", return_value=[]):
                async with app.run_test() as pilot:
                    await pilot.pause()
                    tree = app.query_one(DirectoryTreeView)
                    downloads = tree.root.children[0].children[0]
                    downloads.expand()
                    await pilot.pause()
                    tree.move_cursor(downloads.children[0])
                    app.action_mark()
                    app.action_archive_marked()
                    await pilot.pause()

                    self.assertIsInstance(app.screen, ArchiveDestination)
                    app.screen.query_one("#archive-path", Input).value = str(destination)
                    await pilot.click("#archive")
                    await pilot.pause()
                    await app.workers.wait_for_complete()
                    await pilot.pause()

                    self.assertIsInstance(app.screen, ArchiveReport)
                    archives = list(destination.glob("wtfs-archive-*"))
                    self.assertEqual(len(archives), 1)
                    archived = archives[0] / "Downloads" / "later"
                    self.assertEqual((archived / "contents").read_text(), "saved")
                    self.assertTrue((archives[0] / "manifest.json").exists())
                    self.assertFalse(target.exists())
                    self.assertEqual(app.marked, set())


if __name__ == "__main__":
    unittest.main()
