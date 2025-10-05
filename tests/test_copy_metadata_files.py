"""Tests copy_metadata_files module"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_upload_utils.copy_metadata_files import (
    CopyMetadataJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
METADATA_DIR = RESOURCES_DIR / "metadata_dir"


class TestCopyMetadataJob(unittest.TestCase):
    """Test class for CopyMetadataJob."""

    def test_job_settings_property(self):
        """Tests all_files property."""
        default_job_settings = JobSettings(
            metadata_dir=METADATA_DIR, output_directory="tests"
        )
        possible_files = default_job_settings.possible_files
        expected_possible_files = {
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
        self.assertEqual(expected_possible_files, possible_files)

    @patch("shutil.copy")
    def test_run_job(self, mock_copy: MagicMock):
        """Tests run_job method."""
        default_job_settings = JobSettings(
            metadata_dir=METADATA_DIR, output_directory="tests"
        )
        job = CopyMetadataJob(job_settings=default_job_settings)
        with self.assertLogs(level="DEBUG") as captured:
            job.run_job()
        expected_copy_calls = [
            call(METADATA_DIR / "session.json", Path("tests/session.json")),
            call(
                METADATA_DIR / "procedures.json",
                Path("tests") / "procedures.json",
            ),
            call(
                METADATA_DIR / "subject.json", Path("tests") / "subject.json"
            ),
            call(
                METADATA_DIR / "data_description.json",
                Path("tests") / "data_description.json",
            ),
            call(METADATA_DIR / "rig.json", Path("tests") / "rig.json"),
            call(
                METADATA_DIR / "processing.json",
                Path("tests") / "processing.json",
            ),
        ]
        self.assertEqual(1, len(captured.output))
        self.assertIn("metadata_files", captured.output[0])
        mock_copy.assert_has_calls(expected_copy_calls, any_order=True)


if __name__ == "__main__":
    unittest.main()
