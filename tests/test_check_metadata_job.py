"""Module for testing the CheckMetadataJob class."""

import os
import unittest
from pathlib import Path

from aind_data_upload_utils.check_metadata_job import (
    CheckMetadataJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
METADATA_DIR = RESOURCES_DIR / "metadata_dir"


class TestCheckMetadataJob(unittest.TestCase):
    """Test class for CheckMetadataJob."""

    def test_job_settings_property(self):
        """Tests all_files property."""
        default_job_settings = JobSettings(metadata_dir=METADATA_DIR)
        all_files = default_job_settings.all_files
        expected_files = {
            "instrument.json",
            "rig.json",
            "procedures.json",
            "quality_control.json",
            "session.json",
            "subject.json",
            "data_description.json",
            "acquisition.json",
            "processing.json",
        }
        self.assertEqual(expected_files, all_files)

    def test_run_job_success(self):
        """
        Tests run_job succeeds when all required, optional, and either-or
        files are valid using default settings.
        """
        default_job_settings = JobSettings(metadata_dir=METADATA_DIR)
        job = CheckMetadataJob(job_settings=default_job_settings)
        job.run_job()

    def test_run_job_success_str(self):
        """
        Tests run_job succeeds when metadata_dir input as string.
        """
        default_job_settings = JobSettings(metadata_dir=str(METADATA_DIR))
        job = CheckMetadataJob(job_settings=default_job_settings)
        job.run_job()

    def test_missing_required_files(self):
        """Tests that job fails when required files are missing."""
        job_settings = JobSettings(
            metadata_dir=METADATA_DIR,
            required_files=["missing.json"],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(FileNotFoundError) as context:
            job.run_job()
        self.assertIn("Missing required files", str(context.exception))

    def test_invalid_optional_file(self):
        """Tests that job fails when an optional file is invalid."""
        job_settings = JobSettings(
            metadata_dir=METADATA_DIR,
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
        Tests that job fails when neither file in an either-or pair exists.
        """
        job_settings = JobSettings(
            metadata_dir=METADATA_DIR,
            either_or_files=[("missing1.json", "missing2.json")],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(FileNotFoundError) as context:
            job.run_job()
        self.assertIn("Neither of the files in", str(context.exception))

    def test_both_either_or_files(self):
        """
        Tests that job fails when neither file in both either-or pair exists.
        """
        job_settings = JobSettings(
            metadata_dir=METADATA_DIR,
            either_or_files=[("optional.json", "rig.json")],
        )
        job = CheckMetadataJob(job_settings=job_settings)
        with self.assertRaises(ValueError) as context:
            job.run_job()
        self.assertIn("Only one of", str(context.exception))


if __name__ == "__main__":
    unittest.main()
