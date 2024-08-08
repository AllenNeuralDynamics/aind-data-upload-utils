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
        bad_match_1 = "/ allen/aind/stage/svc_aind_airflow/prod"
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

    # Patch shutil.rmtree in every unit test
    @patch("shutil.rmtree")
    def test_get_list_of_sub_directories(self, mock_rm_tree: MagicMock):
        """Tests _get_list_of_sub_directories"""
        list_of_dirs = self.example_job._get_list_of_sub_directories()
        expected_list = [
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_488_Em_525",
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600",
            f"{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680",
            f"{SMART_SPIM_DIR.as_posix()}/derivatives/Ex_488_Em_525_MIP",
            f"{SMART_SPIM_DIR.as_posix()}/derivatives/Ex_561_Em_600_MIP",
        ]
        self.assertCountEqual(expected_list, list_of_dirs)

        mock_rm_tree.assert_not_called()

    @patch("shutil.rmtree")
    def test_remove_directory_success(self, mock_rm_tree: MagicMock):
        """Tests _remove_directory when valid path is passed."""
        self.example_job._remove_directory(
            "/allen/aind/stage/svc_aind_airflow/dev/abc"
        )
        mock_rm_tree.assert_called_once_with(
            "/allen/aind/stage/svc_aind_airflow/dev/abc"
        )

    @patch("shutil.rmtree")
    def test_remove_directory_error(self, mock_rm_tree: MagicMock):
        """Tests _remove_directory when invalid path is passed."""

        with self.assertRaises(Exception) as e:
            self.example_job._remove_directory(
                "/allen/aind/stage/svc_aind_airflow/dev"
            )
        expected_error_message = (
            "Directory /allen/aind/stage/svc_aind_airflow/dev is not under "
            "staging folder! Will not remove automatically!"
        )
        self.assertEqual(expected_error_message, e.exception.args[0])
        mock_rm_tree.assert_not_called()

    @patch("shutil.rmtree")
    @patch("logging.debug")
    def test_dask_task_to_process_directory_list(
        self, mock_log_debug: MagicMock, mock_rm_tree: MagicMock
    ):
        """Tests _dask_task_to_process_directory_list."""
        dir_list = [
            "/allen/aind/stage/svc_aind_airflow/dev/abc/def",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/ghi",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/jkl",
        ]
        self.example_job._dask_task_to_process_directory_list(
            directories=dir_list
        )
        mock_rm_tree.assert_has_calls(
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

    @patch("shutil.rmtree")
    @patch("logging.debug")
    def test_dask_task_to_process_directory_list_error(
        self, mock_log_debug: MagicMock, mock_rm_tree: MagicMock
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
            "staging folder! Will not remove automatically!"
        )
        self.assertEqual(expected_error_message, e.exception.args[0])
        mock_rm_tree.assert_not_called()
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

    @patch("shutil.rmtree")
    @patch("logging.debug")
    @patch("dask.bag.map_partitions")
    def test_remove_subdirectories(
        self,
        mock_map_partitions: MagicMock,
        mock_log_debug: MagicMock,
        mock_rm_tree: MagicMock,
    ):
        """Tests _remove_subdirectories"""
        dir_list = [
            "/allen/aind/stage/svc_aind_airflow/dev/abc/def",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/ghi",
            "/allen/aind/stage/svc_aind_airflow/dev/abc/jkl",
        ]
        self.example_job._remove_subdirectories(sub_directories=dir_list)
        mock_map_partitions.assert_called()
        # Shouldn't be called because map_partitions is being mocked
        mock_rm_tree.assert_not_called()
        mock_log_debug.assert_not_called()

    @patch("shutil.rmtree")
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
        mock_rm_tree: MagicMock,
    ):
        """Tests run_job method"""
        mock_remove_subdirectories.return_value = None
        mock_remove_directory.return_value = None
        self.example_job.run_job()
        mock_remove_subdirectories.assert_called()
        mock_remove_directory.assert_called()
        # _remove_directory is mocked, so rmtree shouldn't be called
        mock_rm_tree.assert_not_called()
        mock_log_debug.assert_called()


if __name__ == "__main__":
    unittest.main()
