"""Module for testing the CheckMetadataJob class."""

import unittest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
from aind_data_upload_utils.check_metadata_job import (
    CheckMetadataJob,
    JobSettings,
)
import os

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestCheckMetadataJob(unittest.TestCase):
    """Test class for CheckMetadataJob."""

    def setUp(self):
        """Set up the test environment."""
        self.metadata_dir = RESOURCES_DIR / "metadata_dir"
        self.job_settings = JobSettings(
            metadata_dir=self.metadata_dir, dry_run=False
        )
        self.job = CheckMetadataJob(job_settings=self.job_settings)

    def test_validate_json_valid(self):
        """Test that valid JSON files pass validation."""
        valid_file = self.metadata_dir / "subject.json"
        result = self.job._validate_json(valid_file)
        self.assertTrue(result)

    def test_validate_json_invalid(self):
        """Test that invalid JSON files fail validation."""
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".json"
        ) as temp_file:
            temp_file.write(b"{invalid_json}")
            temp_file_path = Path(temp_file.name)

        try:
            result = self.job._validate_json(temp_file_path)
            self.assertFalse(result)
        finally:
            temp_file_path.unlink()

    def test_validate_json_missing(self):
        """Test that missing files fail validation."""
        missing_file = self.metadata_dir / "missing.json"
        result = self.job._validate_json(missing_file)
        self.assertFalse(result)

    def test_check_required_files_valid(self):
        """Test that required files check succeeds."""
        self.assertIsNone(self.job._check_required_files())

    def test_check_required_files_missing(self):
        """Test that missing required files raise an error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_metadata_dir = Path(temp_dir)
            temp_job_settings = JobSettings(
                metadata_dir=temp_metadata_dir, dry_run=False
            )
            temp_job = CheckMetadataJob(job_settings=temp_job_settings)

            with self.assertRaises(FileNotFoundError):
                temp_job._check_required_files()

    def test_check_optional_files(self):
        """Test that missing optional files do not raise an error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_metadata_dir = Path(temp_dir)
            temp_job_settings = JobSettings(
                metadata_dir=temp_metadata_dir, dry_run=False
            )
            temp_job = CheckMetadataJob(job_settings=temp_job_settings)

            self.assertIsNone(temp_job._check_optional_files())

    def test_check_either_or_files_valid(self):
        """
        Test that at least one file in each either-or pair
        passes validation.
        """
        with self.assertRaises(FileNotFoundError):
            self.job._check_either_or_files()

    def test_check_either_or_files_missing(self):
        """Test that missing either-or files raises an error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_metadata_dir = Path(temp_dir)
            temp_job_settings = JobSettings(
                metadata_dir=temp_metadata_dir, dry_run=False
            )
            temp_job = CheckMetadataJob(job_settings=temp_job_settings)

            with self.assertRaises(FileNotFoundError):
                temp_job._check_either_or_files()

    @patch("logging.info")
    def test_run_job_success(self, mock_logging_info):
        """
        Test the main job runner when all required files,
        including either-or files, exist.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_metadata_dir = Path(temp_dir)
            # Populate the temporary metadata directory
            for file_name in self.job.REQUIRED_FILES:
                shutil.copy(
                    self.metadata_dir / file_name,
                    temp_metadata_dir / file_name,
                )
            for file_pair in self.job.EITHER_OR_FILES:
                (temp_metadata_dir / file_pair[0]).write_text(
                    "{}"
                )  # Create a valid JSON file
            for optional_file in self.job.OPTIONAL_FILES:
                (temp_metadata_dir / optional_file).write_text(
                    "{}"
                )  # Create a valid JSON file

            temp_job_settings = JobSettings(
                metadata_dir=temp_metadata_dir, dry_run=False
            )
            temp_job = CheckMetadataJob(job_settings=temp_job_settings)

            # Should not raise any exceptions
            temp_job.run_job()
            mock_logging_info.assert_any_call(
                "Starting metadata validation job."
            )
            self.assertTrue(
                any(
                    "Metadata validation completed in" in call[0][0]
                    for call in mock_logging_info.call_args_list
                ),
                "Expected log message not found.",
            )

    @patch("logging.info")
    def test_run_job_failure(self, mock_logging_info):
        """Test the main job runner."""
        # metadata_dir does not have either/or files
        with self.assertRaises(FileNotFoundError) as e:
            self.job.run_job()
            expected_message = (
                "None of the files in ('instrument.json', 'rig.json')"
                " exist or are valid."
            )
            self.assertEqual(str(e.exception), expected_message)
        mock_logging_info.assert_called_with(
            "Starting metadata validation job."
        )


if __name__ == "__main__":
    unittest.main()
