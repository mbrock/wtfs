import tempfile
import unittest
from pathlib import Path

from wtfs.dump import Directory, DirectoryTotals, WtfsDump
from wtfs.tui import (
    ConfirmDeletion,
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
                await pilot.pause()
                self.assertFalse(target.exists())
                self.assertEqual(app.marked, set())


if __name__ == "__main__":
    unittest.main()
