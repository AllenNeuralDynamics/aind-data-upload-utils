"""Test module for classes and methods in delete_staging_folder_job"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_upload_utils.delete_staging_folder_job import (
    DeleteStagingFolderJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
SMART_SPIM_DIR = (
    RESOURCES_DIR
    / "example_smartspim_data_set"
    / "SmartSPIM_695464_2023-10-18_20-30-30"
)


class TestJobSettings(unittest.TestCase):
    """
    Tests for JobSettings class
    """

    def test_class_constructor(self):
        """Tests that job settings can be constructed from serialized json."""
        job_settings = JobSettings(staging_directory=SMART_SPIM_DIR)
        deserialized_settings = job_settings.model_validate_json(
            job_settings.model_dump_json()
        )
        self.assertEqual(job_settings, deserialized_settings)

    def test_regex_pattern(self):
        """Tests regex pattern matches correctly"""

        job_settings = JobSettings(staging_directory=SMART_SPIM_DIR)

        good_match_1 = "/allen/aind/stage/svc_aind_airflow/prod/abc_123"
        good_match_2 = "/allen/aind/stage/svc_aind_airflow/dev/abc 123/def456"
        bad_match_1 = "/ allen/aind/stage/svc_aind_airflow/prod/"
        bad_match_2 = "/"
        bad_match_3 = "/something/else/here"

        self.assertRegex(good_match_1, job_settings.pattern_to_match)
        self.assertRegex(good_match_2, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_1, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_2, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_3, job_settings.pattern_to_match)


class TestDeleteStagingFolderJob(unittest.TestCase):
    """Tests DeleteStagingFolderJob"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up basic job"""
        job_settings = JobSettings(
            staging_directory=SMART_SPIM_DIR, num_of_dir_levels=1
        )
        cls.example_job = DeleteStagingFolderJob(job_settings=job_settings)

    def setUp(self):
        """Patch rmtree in every test"""
        self.patch_rmtree = patch("shutil.rmtree")
        self.mock_rmtree = self.patch_rmtree.start()

    def tearDown(self):
        """Stop patch"""
        self.patch_rmtree.stop()

    def test_get_list_of_sub_directories(self):
        """Tests _get_list_of_sub_directories"""
        folder = self.example_job.job_settings.staging_directory
        list_of_dirs = self.example_job._get_list_of_sub_directories(
            folder=folder
        )
        expected_list = [
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_488_Em_525",
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600",
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680",
            f"{SMART_SPIM_DIR.as_posix()}/derivatives/Ex_488_Em_525_MIP",
            f"{SMART_SPIM_DIR.as_posix()}/derivatives/Ex_561_Em_600_MIP",
        ]
        self.assertCountEqual(expected_list, list_of_dirs)

    @patch("os.path.exists")
    def test_remove_directory_success(self, mock_exists: MagicMock):
        """Tests _remove_directory when valid path is passed."""
        mock_exists.return_value = True
        with self.assertLogs(level="INFO") as captured:
            self.example_job._remove_directory(
                "/allen/aind/stage/svc_aind_airflow/dev/abc"
            )
        self.assertEqual(1, len(captured.output))
        self.mock_rmtree.assert_called_once_with(
            "/allen/aind/stage/svc_aind_airflow/dev/abc"
        )

    @patch("os.path.exists")
    def test_remove_directory_not_exists(self, mock_exists: MagicMock):
        """Tests _remove_directory when directory does not exist."""
        mock_exists.return_value = False
        with self.assertLogs(level="INFO") as captured:
            self.example_job._remove_directory(
                "/allen/aind/stage/svc_aind_airflow/dev/abc"
            )
        expected_output = [
            (
                "WARNING:root:/allen/aind/stage/svc_aind_airflow/dev/abc"
                " does not exist!"
            )
        ]
        self.mock_rmtree.assert_not_called()
        self.assertEqual(expected_output, captured.output)

    def test_remove_directory_error(self):
        """Tests _remove_directory when invalid path is passed."""

        with self.assertRaises(Exception) as e:
            self.example_job._remove_directory(
                "/allen/aind/stage/svc_aind_airflow/dev"
            )
        expected_error_message = (
            "Directory /allen/aind/stage/svc_aind_airflow/dev is not under "
            "parent folder! Will not remove automatically!"
        )
        self.assertEqual(expected_error_message, e.exception.args[0])
        self.mock_rmtree.assert_not_called()

    def test_remove_directory_norm_error(self):
        """Tests _remove_directory when path is not normalized."""

        with self.assertRaises(Exception) as e:
            self.example_job._remove_directory(
                "/allen/aind/stage/svc_aind_airflow/dev/../abc"
            )
        self.assertIn("needs to be absolute and normalized!", str(e.exception))
        self.mock_rmtree.assert_not_called()

    @patch("os.path.exists")
    @patch("logging.info")
    def test_remove_directory_dry_run(
        self,
        mock_log_info: MagicMock,
        mock_exists: MagicMock,
    ):
        """Tests _remove_directory when dry_run is set to True."""
        mock_exists.return_value = True
        job_settings = JobSettings(
            staging_directory=SMART_SPIM_DIR, num_of_dir_levels=1, dry_run=True
        )
        job = DeleteStagingFolderJob(job_settings=job_settings)
        job._remove_directory("/allen/aind/stage/svc_aind_airflow/dev/abc")
        mock_log_info.assert_called_once_with(
            "(DRYRUN): "
            "shutil.rmtree('/allen/aind/stage/svc_aind_airflow/dev/abc')"
        )
        self.mock_rmtree.assert_not_called()

    @patch("os.path.exists")
    @patch("logging.debug")
    def test_dask_task_to_process_directory_list(
        self,
        mock_log_debug: MagicMock,
        mock_exists: MagicMock,
    ):
        """Tests _dask_task_to_process_directory_list."""
        mock_exists.return_value = True

        dir_list = [
            "/allen/aind/stage/svc_aind_airflow/dev/abc/def",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/ghi",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/jkl",
        ]
        with self.assertLogs(level="INFO") as captured:
            self.example_job._dask_task_to_process_directory_list(
                directories=dir_list
            )
        self.mock_rmtree.assert_has_calls(
            [
                call("/allen/aind/stage/svc_aind_airflow/dev/abc/def"),
                call("/allen/aind/stage/svc_aind_airflow/dev/abc/ghi"),
                call("/allen/aind/stage/svc_aind_airflow/dev/abc/jkl"),
            ]
        )
        mock_log_debug.assert_has_calls(
            [
                call(
                    "Removing list: ["
                    "'/allen/aind/stage/svc_aind_airflow/dev/abc/def', "
                    "'/allen/aind/stage/svc_aind_airflow/dev/abc/ghi', "
                    "'/allen/aind/stage/svc_aind_airflow/dev/abc/jkl']"
                ),
                call(
                    "Removing /allen/aind/stage/svc_aind_airflow/dev/abc/def. "
                    "On 1 of 3"
                ),
                call(
                    "Removing /allen/aind/stage/svc_aind_airflow/dev/abc/ghi. "
                    "On 2 of 3"
                ),
                call(
                    "Removing /allen/aind/stage/svc_aind_airflow/dev/abc/jkl. "
                    "On 3 of 3"
                ),
            ]
        )
        self.assertEqual(3, len(captured.output))

    @patch("logging.debug")
    def test_dask_task_to_process_directory_list_error(
        self, mock_log_debug: MagicMock
    ):
        """Tests _dask_task_to_process_directory_list when invalid path."""
        dir_list = [
            "/foo/abc/def",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/ghi",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/jkl",
        ]
        with self.assertRaises(Exception) as e:
            self.example_job._dask_task_to_process_directory_list(
                directories=dir_list
            )
        expected_error_message = (
            "Directory /foo/abc/def is not under "
            "parent folder! Will not remove automatically!"
        )
        self.assertEqual(expected_error_message, e.exception.args[0])
        mock_log_debug.assert_has_calls(
            [
                call(
                    "Removing list: ["
                    "'/foo/abc/def', "
                    "'/allen/aind/stage/svc_aind_airflow/dev/abc/ghi', "
                    "'/allen/aind/stage/svc_aind_airflow/dev/abc/jkl']"
                ),
                call("Removing /foo/abc/def. On 1 of 3"),
            ]
        )

    @patch("logging.debug")
    @patch("dask.bag.map_partitions")
    def test_remove_subdirectories(
        self,
        mock_map_partitions: MagicMock,
        mock_log_debug: MagicMock,
    ):
        """Tests _remove_subdirectories"""
        dir_list = [
            "/allen/aind/stage/svc_aind_airflow/dev/abc/def",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/ghi",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/jkl",
        ]
        self.example_job._remove_subdirectories(sub_directories=dir_list)
        mock_map_partitions.assert_called()
        mock_log_debug.assert_not_called()

    @patch(
        "aind_data_upload_utils.delete_staging_folder_job."
        "DeleteStagingFolderJob._remove_subdirectories"
    )
    @patch(
        "aind_data_upload_utils.delete_staging_folder_job."
        "DeleteStagingFolderJob._remove_directory"
    )
    @patch("logging.debug")
    def test_run_job(
        self,
        mock_log_debug: MagicMock,
        mock_remove_directory: MagicMock,
        mock_remove_subdirectories: MagicMock,
    ):
        """Tests run_job method"""
        mock_remove_subdirectories.return_value = None
        mock_remove_directory.return_value = None
        self.example_job.run_job()
        mock_remove_subdirectories.assert_called()
        mock_remove_directory.assert_called()
        mock_log_debug.assert_called()


if __name__ == "__main__":
    unittest.main()
