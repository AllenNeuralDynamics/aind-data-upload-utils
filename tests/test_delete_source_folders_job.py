"""Test module for classes and methods in delete_staging_folder_job"""

import os
import unittest
from copy import deepcopy
from datetime import datetime, timezone
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
                derivatives_dir=str(RESOURCES_DIR / "example_derivatives_dir"),
            ),
            s3_location="s3://example/abc_123",
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
            ),
            s3_location="s3://example/abc_123",
        )

        good_match_1 = "/allen/aind/stage/svc_aind_airflow/prod/abc_123"
        good_match_2 = "/allen/aind/stage/svc_aind_airflow/dev/abc 123/def456"
        good_match_3 = (
            "/allen/aind/scratch/dynamic_foraging_rig_transfer/behavior"
        )
        good_match_4 = (
            "//allen/aind/scratch/dynamic_foraging_rig_transfer/behavior"
        )
        bad_match_1 = "/ allen/aind/stage/svc_aind_airflow/prod/"
        bad_match_2 = "/"
        bad_match_3 = "/something/else/here"
        bad_match_4 = "/allen/aind/scratch/dynamic_foraging_rig_transfer/"
        bad_match_5 = (
            "///allen/aind/scratch/dynamic_foraging_rig_transfer/behavior"
        )

        self.assertRegex(good_match_1, job_settings.pattern_to_match)
        self.assertRegex(good_match_2, job_settings.pattern_to_match)
        self.assertRegex(good_match_3, job_settings.pattern_to_match)
        self.assertRegex(good_match_4, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_1, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_2, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_3, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_4, job_settings.pattern_to_match)
        self.assertNotRegex(bad_match_5, job_settings.pattern_to_match)


class TestDeleteSourceFoldersJob(unittest.TestCase):
    """Tests DeleteSourceFoldersJob"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up basic job"""
        s3_check_job_settings = JobSettings(
            directories=DirectoriesToDeleteConfigs(
                modality_sources={
                    "ecephys": str(EPHYS_DIR),
                },
                metadata_dir=str(RESOURCES_DIR),
                derivatives_dir=str(RESOURCES_DIR / "example_derivatives_dir"),
            ),
            num_of_dir_levels=1,
            s3_location="s3://example/abc_123",
        )
        job_settings = JobSettings(
            directories=DirectoriesToDeleteConfigs(
                modality_sources={
                    "ecephys": str(EPHYS_DIR),
                    "SmartSPIM": str(SMART_SPIM_DIR),
                },
                metadata_dir=str(RESOURCES_DIR),
                derivatives_dir=str(RESOURCES_DIR / "example_derivatives_dir"),
            ),
            num_of_dir_levels=1,
            s3_location="s3://example/abc_123",
        )
        example_s3_response = {
            "ResponseMetadata": dict(),
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": "abc_123/data_description.json",
                    "LastModified": datetime(2025, 6, 6, 23, 3, 53).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "abc_123/metadata.nd.json",
                    "LastModified": datetime(2025, 6, 7, 7, 1, 32).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "abc_123/processing.json",
                    "LastModified": datetime(2025, 6, 6, 23, 4, 26).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "abc_123/subject.json",
                    "LastModified": datetime(2025, 6, 6, 23, 4, 26).replace(
                        tzinfo=timezone.utc
                    ),
                },
            ],
            "Name": "example",
            "Prefix": "abc_123/",
            "Delimiter": "/",
            "MaxKeys": 50,
            "CommonPrefixes": [
                {"Prefix": "abc_123/ecephys/"},
                {"Prefix": "abc_123/derivatives/"},
                {"Prefix": "abc_123/original_metadata/"},
            ],
            "EncodingType": "url",
            "KeyCount": 6,
        }
        cls.example_job = DeleteSourceFoldersJob(job_settings=job_settings)
        cls.s3_check_job = DeleteSourceFoldersJob(
            job_settings=s3_check_job_settings
        )
        cls.example_s3_response = example_s3_response

    def setUp(self):
        """Patch rmtree in every test"""
        self.patch_rmtree = patch("shutil.rmtree")
        self.mock_rm_tree = self.patch_rmtree.start()
        self.patch_remove = patch("os.remove")
        self.mock_remove = self.patch_remove.start()
        self.patch_rmdir = patch("os.rmdir")
        self.mock_rmdir = self.patch_rmdir.start()

    def tearDown(self):
        """Stop patch"""
        self.patch_rmtree.stop()
        self.patch_remove.stop()
        self.patch_rmdir.stop()

    @patch("boto3.client")
    @patch("os.listdir")
    def test_s3_check(
        self,
        mock_list_dir: MagicMock,
        mock_boto_client: MagicMock,
    ):
        """Tests s3 check"""
        mock_boto_client.return_value.list_objects_v2.return_value = (
            self.example_s3_response
        )
        mock_list_dir.return_value = ["data_description.json"]
        with self.assertLogs(level="INFO") as captured:
            self.s3_check_job._s3_check()
        self.assertIn(
            "INFO:root:Checking s3://example/abc_123.", captured.output
        )
        self.assertEqual(4, len(captured.output))

    @patch("boto3.client")
    @patch("os.listdir")
    def test_s3_check_too_many_s3_object_failure(
        self,
        mock_list_dir: MagicMock,
        mock_boto_client: MagicMock,
    ):
        """Tests s3 check when there are too many s3 objects"""
        s3_response = deepcopy(self.example_s3_response)
        s3_response["IsTruncated"] = True
        mock_boto_client.return_value.list_objects_v2.return_value = (
            s3_response
        )
        with self.assertRaises(Exception) as e:
            with self.assertLogs(level="INFO") as captured:
                self.s3_check_job._s3_check()
        self.assertEqual(
            "Unexpected number of objects in s3://example/abc_123!",
            str(e.exception),
        )
        mock_list_dir.assert_not_called()
        self.assertIn(
            "INFO:root:Checking s3://example/abc_123.", captured.output
        )
        self.assertEqual(1, len(captured.output))

    @patch("boto3.client")
    @patch("os.listdir")
    def test_s3_check_local_file_mismatch(
        self,
        mock_list_dir: MagicMock,
        mock_boto_client: MagicMock,
    ):
        """Tests s3 check when local files are not in s3"""
        mock_boto_client.return_value.list_objects_v2.return_value = (
            self.example_s3_response
        )
        mock_list_dir.return_value = ["acquisition.json"]
        with self.assertRaises(Exception) as e:
            with self.assertLogs(level="INFO") as captured:
                self.s3_check_job._s3_check()
        self.assertIn("not found in S3!", str(e.exception))
        self.assertIn(
            "INFO:root:Checking s3://example/abc_123.", captured.output
        )
        self.assertEqual(3, len(captured.output))

    @patch("boto3.client")
    @patch("os.listdir")
    def test_s3_check_local_folder_mismatch(
        self,
        mock_list_dir: MagicMock,
        mock_boto_client: MagicMock,
    ):
        """Tests s3 check when there are modality folders not in s3"""
        mock_list_dir.return_value = ["data_description.json"]
        s3_response = deepcopy(self.example_s3_response)
        s3_response["CommonPrefixes"] = [
            {"Prefix": "abc_123/original_metadata/"}
        ]
        mock_boto_client.return_value.list_objects_v2.return_value = (
            s3_response
        )
        with self.assertRaises(Exception) as e:
            with self.assertLogs(level="INFO") as captured:
                self.s3_check_job._s3_check()
        self.assertIn("not found in S3!", str(e.exception))
        self.assertIn(
            "INFO:root:Checking s3://example/abc_123.", captured.output
        )
        self.assertEqual(3, len(captured.output))

    @patch("boto3.client")
    @patch("os.listdir")
    def test_s3_check_modalities_to_delete_filter(
        self,
        mock_list_dir: MagicMock,
        mock_boto_client: MagicMock,
    ):
        """Tests s3 check when modalities_to_delete filter is set"""
        updated_settings = self.s3_check_job.job_settings.model_copy(
            deep=True, update={"modalities_to_delete": ["ecephys"]}
        )
        job = DeleteSourceFoldersJob(job_settings=updated_settings)
        mock_boto_client.return_value.list_objects_v2.return_value = (
            self.example_s3_response
        )
        mock_list_dir.return_value = ["data_description.json"]
        with self.assertLogs(level="INFO") as captured:
            job._s3_check()
        self.assertIn(
            "INFO:root:Modality filter set to only delete ['ecephys'].",
            captured.output,
        )
        self.assertEqual(5, len(captured.output))

    @patch("os.scandir")
    def test_remove_metadata_directory(self, mock_scandir: MagicMock):
        """Tests remove_metadata_directory method when empty."""

        mock_scandir.return_value.__enter__.return_value = []
        metadata_files = {"subject.json", "data_description.json"}
        self.example_job._remove_metadata_directory(
            metadata_files_in_both_places=metadata_files
        )
        self.assertEqual(2, len(self.mock_remove.mock_calls))
        self.mock_rmdir.assert_called_once()

    @patch("os.scandir")
    def test_remove_metadata_directory_not_empty(
        self, mock_scandir: MagicMock
    ):
        """Tests remove_metadata_directory method when not emptied."""

        mock_scandir.return_value.__enter__.return_value = ["extra_folder"]
        metadata_files = {"subject.json", "data_description.json"}
        with self.assertLogs(level="WARNING"):
            self.example_job._remove_metadata_directory(
                metadata_files_in_both_places=metadata_files
            )
        self.assertEqual(2, len(self.mock_remove.mock_calls))
        self.mock_rmdir.assert_not_called()

    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_metadata_directory"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_subdirectories"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_directory"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._s3_check"
    )
    @patch("logging.debug")
    def test_run_job(
        self,
        mock_log_debug: MagicMock,
        mock_s3_check: MagicMock,
        mock_remove_directory: MagicMock,
        mock_remove_subdirectories: MagicMock,
        mock_remove_metadata_directory: MagicMock,
    ):
        """Tests run_job method"""
        mock_s3_check.return_value = {"data_description.json"}
        mock_remove_subdirectories.return_value = None
        mock_remove_directory.return_value = None
        self.example_job.run_job()
        mock_remove_subdirectories.assert_called()
        mock_remove_directory.assert_has_calls(
            [
                call(str(EPHYS_DIR)),
                call(str(SMART_SPIM_DIR)),
                call(str(RESOURCES_DIR / "example_derivatives_dir")),
            ]
        )
        mock_log_debug.assert_called()
        mock_remove_metadata_directory.assert_called_once_with(
            metadata_files_in_both_places={"data_description.json"}
        )

    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_metadata_directory"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_subdirectories"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._remove_directory"
    )
    @patch(
        "aind_data_upload_utils.delete_source_folders_job."
        "DeleteSourceFoldersJob._s3_check"
    )
    @patch("logging.debug")
    def test_run_job_with_modality_filter(
        self,
        mock_log_debug: MagicMock,
        mock_s3_check: MagicMock,
        mock_remove_directory: MagicMock,
        mock_remove_subdirectories: MagicMock,
        mock_remove_metadata_directory: MagicMock,
    ):
        """Tests run_job method with modality filter"""
        mock_s3_check.return_value = {"data_description.json"}
        mock_remove_subdirectories.return_value = None
        mock_remove_directory.return_value = None
        updated_settings = self.example_job.job_settings.model_copy(
            deep=True, update={"modalities_to_delete": ["ecephys"]}
        )
        job = DeleteSourceFoldersJob(job_settings=updated_settings)
        job.run_job()
        mock_remove_subdirectories.assert_called()
        mock_remove_directory.assert_has_calls(
            [
                call(str(EPHYS_DIR)),
            ]
        )
        mock_log_debug.assert_called()
        mock_remove_metadata_directory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
