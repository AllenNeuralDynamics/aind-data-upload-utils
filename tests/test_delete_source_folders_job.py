"""Test module for classes and methods in delete_staging_folder_job"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_upload_utils.delete_source_folders_job import (
    DeleteSourceFoldersJob,
    DirectoriesToDeleteConfigs,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
SMART_SPIM_DIR = (
    RESOURCES_DIR
    / "example_smartspim_data_set"
    / "SmartSPIM_695464_2023-10-18_20-30-30"
)

EPHYS_DIR = RESOURCES_DIR / "example_ephys_data_set"


class TestJobSettings(unittest.TestCase):
    """
    Tests for JobSettings class
    """

    def test_class_constructor(self):
        """Tests that job settings can be constructed from serialized json."""
        job_settings = JobSettings(
            directories=DirectoriesToDeleteConfigs(
                modality_sources={
                    "ecephys": str(EPHYS_DIR),
                    "SmartSPIM": str(SMART_SPIM_DIR),
                },
                metadata_dir=str(RESOURCES_DIR),
            )
        )
        deserialized_settings = job_settings.model_validate_json(
            job_settings.model_dump_json()
        )
        self.assertEqual(job_settings, deserialized_settings)

    def test_regex_pattern(self):
        """Tests regex pattern matches correctly"""

        job_settings = JobSettings(
            directories=DirectoriesToDeleteConfigs(
                modality_sources={"SmartSPIM": str(SMART_SPIM_DIR)}
            )
        )

        good_match_1 = "/allen/aind/stage/svc_aind_airflow/prod/abc_123"
        good_match_2 = "/allen/aind/stage/svc_aind_airflow/dev/abc 123/def456"
        good_match_3 = (
            "/allen/aind/scratch/dynamic_foraging_rig_transfer/behavior"
        )
        bad_match_1 = "/ allen/aind/stage/svc_aind_airflow/prod/"
        bad_match_2 = "/"
        bad_match_3 = "/something/else/here"
        bad_match_4 = "/allen/aind/scratch/dynamic_foraging_rig_transfer/"

        self.assertRegex(good_match_1, job_settings.pattern_to_match)
        self.assertRegex(good_match_2, job_settings.pattern_to_match)
        self.assertRegex(good_match_3, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_1, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_2, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_3, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_4, job_settings.pattern_to_match)


class TestDeleteSourceFoldersJob(unittest.TestCase):
    """Tests DeleteSourceFoldersJob"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up basic job"""
        job_settings = JobSettings(
            directories=DirectoriesToDeleteConfigs(
                modality_sources={
                    "ecephys": str(EPHYS_DIR),
                    "SmartSPIM": str(SMART_SPIM_DIR),
                },
                metadata_dir=str(RESOURCES_DIR),
            ),
            num_of_dir_levels=1,
        )
        cls.example_job = DeleteSourceFoldersJob(job_settings=job_settings)

    # Patch shutil.rmtree in every unit test
    @patch("shutil.rmtree")
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_subdirectories"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_directory"
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
        print(mock_remove_directory.mock_calls)
        mock_remove_directory.assert_has_calls(
            [
                call(str(EPHYS_DIR)),
                call(str(SMART_SPIM_DIR)),
                call(str(RESOURCES_DIR)),
            ]
        )
        # _remove_directory is mocked, so rmtree shouldn't be called
        mock_rm_tree.assert_not_called()
        mock_log_debug.assert_called()


if __name__ == "__main__":
    unittest.main()
