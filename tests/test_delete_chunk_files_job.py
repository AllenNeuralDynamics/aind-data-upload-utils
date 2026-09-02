"""Test module for classes and methods in delete_chunk_files_job"""

import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_upload_utils.delete_chunk_files_job import (
    DeleteChunkFilesJob,
    JobSettings,
)

CLASS_NAME = (
    "aind_data_upload_utils.delete_chunk_files_job.DeleteChunkFilesJob"
)
RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
CHRONIC_EPHYS_DIR = RESOURCES_DIR / "example_chronic_ephys"


class TestJobSettings(unittest.TestCase):
    """
    Tests for JobSettings class
    """

    def test_class_constructor(self):
        """Tests that job settings can be constructed from serialized json."""
        job_settings = JobSettings(
            dry_run=True,
            s3_bucket="example",
            s3_prefix="123456_2025-01-31_19-00-00",
            modality_sources={
                "behavior-videos": str(CHRONIC_EPHYS_DIR / "behavior-videos")
            },
            chunk="2025-01-31T19-00-00",
            local_chunk_not_in_s3=None,
        )
        deserialized_settings = job_settings.model_validate_json(
            job_settings.model_dump_json()
        )
        self.assertEqual(job_settings, deserialized_settings)


class TestDeleteChunkFilesJob(unittest.TestCase):
    """Tests DeleteChunkFilesJob"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up basic job"""
        job_settings = JobSettings(
            dry_run=True,
            s3_bucket="example",
            s3_prefix="123456_2025-01-31_19-00-00",
            modality_sources={
                "behavior-videos": str(CHRONIC_EPHYS_DIR / "behavior-videos")
            },
            chunk="2025-01-31T19-00-00",
            local_chunk_not_in_s3="AmplifierData",
        )
        actual_run_job_settings = job_settings.model_copy(
            deep=True, update={"dry_run": False}
        )
        example_s3_response = {
            "ResponseMetadata": dict(),
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": (
                        "123456_2025-01-31_19-00-00/behavior-videos/"
                        "TopCamera/TopCamera_2025-01-31T19-00-00.csv"
                    ),
                    "LastModified": datetime(2025, 6, 6, 23, 3, 53).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": (
                        "123456_2025-01-31_19-00-00/behavior-videos/"
                        "TopCamera/TopCamera_2025-01-31T19-00-00.mp4"
                    ),
                    "LastModified": datetime(2025, 6, 7, 7, 1, 32).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "123456_2025-01-31_19-00-00/processing.json",
                    "LastModified": datetime(2025, 6, 6, 23, 4, 26).replace(
                        tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "123456_2025-01-31_19-00-00/subject.json",
                    "LastModified": datetime(2025, 6, 6, 23, 4, 26).replace(
                        tzinfo=timezone.utc
                    ),
                },
            ],
            "Name": "example",
            "Prefix": "123456_2025-01-31_19-00-00/",
            "Delimiter": "/",
            "MaxKeys": 50,
            "CommonPrefixes": [
                {"Prefix": "123456_2025-01-31_19-00-00/behavior-videos/"},
                {"Prefix": "123456_2025-01-31_19-00-00/original_metadata/"},
            ],
            "EncodingType": "url",
            "KeyCount": 4,
        }
        cls.example_job = DeleteChunkFilesJob(job_settings=job_settings)
        cls.actual_run_job = DeleteChunkFilesJob(
            job_settings=actual_run_job_settings
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

    def test_extract_list_of_local_files(self):
        """Tests _extract_list_of_local_files"""
        with self.assertLogs(level="DEBUG") as captured:
            local_files = self.example_job._extract_list_of_local_files()
        expected_local_files = [
            str(
                CHRONIC_EPHYS_DIR
                / "behavior-videos"
                / "TopCamera"
                / "TopCamera_2025-01-31T19-00-00.mp4"
            ),
            str(
                CHRONIC_EPHYS_DIR
                / "behavior-videos"
                / "TopCamera"
                / "TopCamera_2025-01-31T19-00-00.csv"
            ),
        ]
        self.assertEqual(
            ["DEBUG:root:Extracting list of files"], captured.output
        )
        self.assertEqual(expected_local_files, local_files)

    @patch("boto3.client")
    def test_get_list_of_s3_files(
        self,
        mock_boto_client: MagicMock,
    ):
        """Tests _get_list_of_s3_files"""
        mock_s3_client = MagicMock()
        mock_paginator = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [self.example_s3_response]
        with self.assertLogs(level="DEBUG") as captured:
            s3_files = self.example_job._get_list_of_s3_files()

        self.assertEqual(
            ["DEBUG:root:Extracting list of files from S3."], captured.output
        )
        self.assertEqual(2, len(s3_files))

    def test_get_list_of_files_to_delete(self):
        """Tests _get_list_of_files_to_delete"""
        files_to_delete = self.example_job._get_list_of_files_to_delete(
            local_files=[
                "abc/behavior-videos/x_1.mp4",
            ],
            s3_files=["s3://abc/behavior-videos/x_1.mp4"],
        )
        self.assertEqual(["abc/behavior-videos/x_1.mp4"], files_to_delete)

    def test_get_list_of_files_to_delete_nonunique_local(self):
        """Tests _get_list_of_files_to_delete when non-unique local"""
        with self.assertRaises(ValueError):
            _ = self.example_job._get_list_of_files_to_delete(
                local_files=[
                    "abc/behavior-videos/y/x_1.mp4",
                    "abc/behavior-videos/z/x_1.mp4",
                ],
                s3_files=["s3://abc/behavior-videos/y/x_1.mp4"],
            )

    def test_get_list_of_files_to_delete_nonunique_s3(self):
        """Tests _get_list_of_files_to_delete when non-unique s3"""
        with self.assertRaises(ValueError):
            _ = self.example_job._get_list_of_files_to_delete(
                local_files=["abc/behavior-videos/x_1.mp4"],
                s3_files=[
                    "s3://abc/behavior-videos/y/x_1.mp4",
                    "s3://abc/behavior-videos/z/x_1.mp4",
                ],
            )

    def test_get_list_of_files_to_delete_with_exclude(self):
        """Tests _get_list_of_files_to_delete when ignored s3 set"""
        files_to_delete = self.example_job._get_list_of_files_to_delete(
            local_files=[
                "abc/behavior-videos/x_1.mp4",
                "abc/ephys/AmplifierData.bin",
            ],
            s3_files=["s3://abc/behavior-videos/x_1.mp4"],
        )
        self.assertEqual(
            ["abc/behavior-videos/x_1.mp4", "abc/ephys/AmplifierData.bin"],
            files_to_delete,
        )

    @patch("os.path.isfile")
    def test_remove_list_of_files_dryrun(self, mock_is_file: MagicMock):
        """Tests _remove_list_of_files with dry_run True."""
        mock_is_file.return_value = True
        with self.assertLogs(level="INFO") as captured:
            self.example_job._remove_list_of_files(
                files_to_delete=["abc/behavior-videos/x_1.mp4"]
            )
        self.assertEqual(1, len(captured.output))
        self.assertIn("INFO:root:(DRYRUN): os.remove", captured.output[0])
        self.mock_remove.assert_not_called()

    @patch("os.path.isfile")
    def test_remove_list_of_files(self, mock_is_file: MagicMock):
        """Tests _remove_list_of_files with dry_run False."""
        new_job_settings = self.example_job.job_settings.model_copy(
            deep=True, update={"dry_run": False}
        )
        new_job = DeleteChunkFilesJob(job_settings=new_job_settings)
        mock_is_file.return_value = True
        with self.assertLogs(level="INFO") as captured:
            new_job._remove_list_of_files(
                files_to_delete=["abc/behavior-videos/x_1.mp4"]
            )
        self.assertEqual(1, len(captured.output))
        self.assertIn("INFO:root:Removing", captured.output[0])
        self.mock_remove.assert_called()

    @patch("os.path.isfile")
    def test_remove_list_of_files_warning(self, mock_is_file: MagicMock):
        """Tests _remove_list_of_files with warning."""
        new_job_settings = self.example_job.job_settings.model_copy(
            deep=True, update={"dry_run": False}
        )
        new_job = DeleteChunkFilesJob(job_settings=new_job_settings)
        mock_is_file.return_value = False
        with self.assertLogs(level="WARNING") as captured:
            new_job._remove_list_of_files(
                files_to_delete=["abc/behavior-videos/x_1.mp4"]
            )
        self.assertEqual(1, len(captured.output))
        self.assertIn("directory already removed", captured.output[0])
        self.mock_remove.assert_not_called()

    @patch(f"{CLASS_NAME}._extract_list_of_local_files")
    @patch(f"{CLASS_NAME}._get_list_of_s3_files")
    @patch(f"{CLASS_NAME}._remove_list_of_files")
    def test_run_job(
        self,
        mock_remove_list_of_files: MagicMock,
        mock_get_list_of_s3_files: MagicMock,
        mock_extract_list_of_local_files: MagicMock,
    ):
        """Test run_job."""

        mock_extract_list_of_local_files.return_value = [
            "abc/behavior-videos/x_1.mp4",
        ]
        mock_get_list_of_s3_files.return_value = [
            "s3://abc/behavior-videos/x_1.mp4"
        ]
        with self.assertLogs(level="DEBUG") as captured:
            self.example_job.run_job()
        print(mock_remove_list_of_files.mock_calls)
        print(captured.output)
        mock_remove_list_of_files.assert_called_once_with(
            ["abc/behavior-videos/x_1.mp4"]
        )
        self.assertEqual(1, len(captured.output))


if __name__ == "__main__":
    unittest.main()
