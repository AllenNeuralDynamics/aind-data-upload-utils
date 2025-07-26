"""Tests for create_symlinks_job module.."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_upload_utils.create_sym_links_job import (
    CreateSymLinksJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
EXAMPLE_SOURCE_DIR = RESOURCES_DIR / "example_chronic_ephys"


class TestCreateSymLinksJob(unittest.TestCase):
    """Tests for CreateSymLinksJob class."""

    def test_extract_list_of_files_no_chunk(self):
        """Tests _extract_list_of_files with no chunk parameter"""

        settings = JobSettings(
            input_directory=str(EXAMPLE_SOURCE_DIR),
            output_directory=str(RESOURCES_DIR),
        )
        job = CreateSymLinksJob(job_settings=settings)
        list_of_files = job._extract_list_of_files()
        expected_list_of_files = [str(EXAMPLE_SOURCE_DIR)]
        self.assertEqual(expected_list_of_files, list_of_files)

    def test_extract_list_of_files_with_chunk(self):
        """Tests _extract_list_of_files with with chunk parameter"""

        settings = JobSettings(
            input_directory=str(EXAMPLE_SOURCE_DIR),
            output_directory=str(RESOURCES_DIR),
            chunk="2025-01-31T19-00-00",
        )
        job = CreateSymLinksJob(job_settings=settings)
        list_of_files = job._extract_list_of_files()
        camera_path = EXAMPLE_SOURCE_DIR / "behavior-videos" / "TopCamera"
        expected_list_of_files = [
            str(camera_path / "TopCamera_2025-01-31T19-00-00.csv"),
            str(camera_path / "TopCamera_2025-01-31T19-00-00.mp4"),
        ]
        self.assertCountEqual(expected_list_of_files, list_of_files)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.makedirs")
    @patch("os.symlink")
    def test_create_sym_link_path_exists(
        self,
        mock_sym_link: MagicMock,
        mock_mkdirs: MagicMock,
        mock_path_isdir: MagicMock,
        mock_path_exists: MagicMock,
    ):
        """Tests _create_sym_link when destination already exists"""
        mock_path_exists.return_value = True
        with self.assertLogs(level="DEBUG") as captured:
            CreateSymLinksJob._create_sym_link("src", "dst", dry_run=True)
        mock_path_isdir.assert_not_called()
        mock_mkdirs.assert_not_called()
        mock_sym_link.assert_not_called()
        expected_logs = [
            "WARNING:root:Destination dst exists! Will skip linking."
        ]
        self.assertEqual(expected_logs, captured.output)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.makedirs")
    @patch("os.symlink")
    def test_create_sym_link_directory(
        self,
        mock_sym_link: MagicMock,
        mock_mkdirs: MagicMock,
        mock_path_isdir: MagicMock,
        mock_path_exists: MagicMock,
    ):
        """Tests _create_sym_link when source is directory"""
        mock_path_exists.return_value = False
        mock_path_isdir.return_value = True
        with self.assertLogs(level="DEBUG") as captured:
            CreateSymLinksJob._create_sym_link("src", "dst", dry_run=False)
        expected_logs = ["DEBUG:root:Sym linking src to dst"]
        mock_mkdirs.assert_not_called()
        mock_sym_link.assert_called_once_with(
            src="src", dst="dst", target_is_directory=True
        )
        self.assertEqual(expected_logs, captured.output)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.makedirs")
    @patch("os.symlink")
    def test_create_sym_link_directory_dry_run(
        self,
        mock_sym_link: MagicMock,
        mock_mkdirs: MagicMock,
        mock_path_isdir: MagicMock,
        mock_path_exists: MagicMock,
    ):
        """Tests _create_sym_link when source is directory dry_run"""
        mock_path_exists.return_value = False
        mock_path_isdir.return_value = True
        with self.assertLogs(level="DEBUG") as captured:
            CreateSymLinksJob._create_sym_link("src", "dst", dry_run=True)
        expected_logs = [
            (
                "INFO:root:(dryrun):"
                " os.symlink(src=src, dst=dst, target_is_directory=True)"
            )
        ]
        mock_mkdirs.assert_not_called()
        mock_sym_link.assert_not_called()
        self.assertEqual(expected_logs, captured.output)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.makedirs")
    @patch("os.symlink")
    def test_create_sym_link_file(
        self,
        mock_sym_link: MagicMock,
        mock_mkdirs: MagicMock,
        mock_path_isdir: MagicMock,
        mock_path_exists: MagicMock,
    ):
        """Tests _create_sym_link when source is a file"""
        mock_path_exists.return_value = False
        mock_path_isdir.return_value = False
        with self.assertLogs(level="DEBUG") as captured:
            CreateSymLinksJob._create_sym_link(
                "src/file.txt", "dst/file.txt", dry_run=False
            )
        expected_logs = ["DEBUG:root:Sym linking src/file.txt to dst/file.txt"]
        mock_mkdirs.assert_called_once_with("dst", exist_ok=True)
        mock_sym_link.assert_called_once_with(
            src="src/file.txt", dst="dst/file.txt", target_is_directory=False
        )
        self.assertEqual(expected_logs, captured.output)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.makedirs")
    @patch("os.symlink")
    def test_create_sym_link_file_dry_run(
        self,
        mock_sym_link: MagicMock,
        mock_mkdirs: MagicMock,
        mock_path_isdir: MagicMock,
        mock_path_exists: MagicMock,
    ):
        """Tests _create_sym_link when source is a file dry_run"""
        mock_path_exists.return_value = False
        mock_path_isdir.return_value = False
        src = str(Path("src") / "file.txt")
        dst = str(Path("dst") / "file.txt")
        with self.assertLogs(level="DEBUG") as captured:
            CreateSymLinksJob._create_sym_link(src, dst, dry_run=True)
        expected_logs = [
            (
                f"INFO:root:(dryrun): os.makedirs(os.path.dirname({dst}),"
                f" exist_ok=True)"
            ),
            (
                f"INFO:root:(dryrun): os.symlink(src={src}, dst={dst},"
                f" target_is_directory=False)"
            ),
        ]
        mock_mkdirs.assert_not_called()
        mock_sym_link.assert_not_called()
        self.assertEqual(expected_logs, captured.output)

    @patch(
        "aind_data_upload_utils.create_sym_links_job.CreateSymLinksJob"
        "._create_sym_link"
    )
    def test_run_job(self, mock_create_sym_link: MagicMock):
        """Tests run_job method without chunk"""
        src = str(EXAMPLE_SOURCE_DIR / "behavior-videos")
        dst = str(RESOURCES_DIR / "behavior-videos")
        settings = JobSettings(
            input_directory=src, output_directory=dst, dry_run=True
        )
        job = CreateSymLinksJob(job_settings=settings)
        with self.assertLogs(level="DEBUG") as captured:
            job.run_job()
        expected_logs = [
            (
                f"DEBUG:root:Running job with settings input_directory='{src}'"
                f" output_directory='{dst}' "
                f"chunk=None "
                f"dry_run=True"
            ),
            "DEBUG:root:Extracting list of files",
            "DEBUG:root:Finished job.",
        ]
        mock_create_sym_link.assert_called_once_with(
            src=src, dst=dst, dry_run=True
        )
        self.assertEqual(expected_logs, captured.output)

    @patch(
        "aind_data_upload_utils.create_sym_links_job.CreateSymLinksJob"
        "._create_sym_link"
    )
    def test_run_job_with_chunk(self, mock_create_sym_link: MagicMock):
        """Tests run_job method with chunk"""
        src = str(EXAMPLE_SOURCE_DIR / "behavior-videos")
        dst = str(RESOURCES_DIR / "behavior-videos")
        settings = JobSettings(
            input_directory=src,
            output_directory=dst,
            chunk="2025-01-31T19-00-00",
            dry_run=True,
        )
        job = CreateSymLinksJob(job_settings=settings)
        with self.assertLogs(level="DEBUG") as captured:
            job.run_job()
        expected_logs = [
            (
                f"DEBUG:root:Running job with settings input_directory='{src}'"
                f" output_directory='{dst}' "
                f"chunk='2025-01-31T19-00-00' "
                f"dry_run=True"
            ),
            "DEBUG:root:Extracting list of files",
            "DEBUG:root:Finished job.",
        ]

        mock_create_sym_link.assert_has_calls(
            [
                call(
                    src=str(
                        Path(src)
                        / "TopCamera"
                        / "TopCamera_2025-01-31T19-00-00.csv"
                    ),
                    dst=str(
                        Path(dst)
                        / "TopCamera"
                        / "TopCamera_2025-01-31T19-00-00.csv"
                    ),
                    dry_run=True,
                ),
                call(
                    src=str(
                        Path(src)
                        / "TopCamera"
                        / "TopCamera_2025-01-31T19-00-00.mp4"
                    ),
                    dst=str(
                        Path(dst)
                        / "TopCamera"
                        / "TopCamera_2025-01-31T19-00-00.mp4"
                    ),
                    dry_run=True,
                ),
            ],
            any_order=True,
        )
        self.assertEqual(expected_logs, captured.output)


if __name__ == "__main__":
    unittest.main()
