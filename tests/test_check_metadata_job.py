"""Module for testing the CheckMetadataJob class."""

import unittest
from pathlib import Path
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

    def test_run_job_success(self):
        """
        Tests run_job succeeds when all required, optional,
        and either-or files are valid using default settings.
        """
        default_job_settings = JobSettings(metadata_dir=self.metadata_dir)
        job = CheckMetadataJob(job_settings=default_job_settings)
        job.run_job()

    def test_missing_required_files(self):
        """Tests that job fails when required files are missing."""
        job_settings = JobSettings(
            metadata_dir=self.metadata_dir,
            required_files=["missing.json"],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(FileNotFoundError) as context:
            job.run_job()
        self.assertIn("Missing required files", str(context.exception))

    def test_invalid_optional_file(self):
        """Tests that job fails when an optional file is invalid."""
        job_settings = JobSettings(
            metadata_dir=self.metadata_dir,
            optional_files=["optional.json"],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(ValueError) as context:
            job.run_job()
        self.assertIn(
            "Expecting value: line 1 column 1 (char 0)", str(context.exception)
        )

    def test_missing_either_or_files(self):
        """
        Tests that job fails when neither file in an either-or pair
        exists.
        """
        job_settings = JobSettings(
            metadata_dir=self.metadata_dir,
            either_or_files=[("missing1.json", "missing2.json")],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(FileNotFoundError) as context:
            job.run_job()
        self.assertIn("Neither of the files in", str(context.exception))


if __name__ == "__main__":
    unittest.main()
