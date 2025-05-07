"""
Module to check that certain metadata files exist and are in valid format.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from time import time

from pydantic import Field
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for CheckMetadataJob"""

    metadata_dir: Path = Field(
        ..., description="Directory containing metadata JSON files."
    )
    dry_run: bool = Field(
        default=False,
        description="Log validation results without raising errors.",
    )


class CheckMetadataJob:
    """Job to validate the existence and format of metadata JSON files."""

    REQUIRED_FILES = [
        "data_description.json",
        "subject.json",
        "procedures.json",
    ]
    OPTIONAL_FILES = [
        "processing.json",
        "quality_control.json",
    ]
    EITHER_OR_FILES = [
        ("instrument.json", "rig.json"),
        ("acquisition.json", "session.json"),
    ]

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for CheckMetadataJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    @staticmethod
    def _validate_json(file_path: Path) -> bool:
        """
        Validates that a file exists and is valid JSON.

        Parameters
        ----------
        file_path : Path

        Returns
        -------
        bool
            True if the file is valid JSON, False otherwise.
        """
        try:
            with open(file_path, "r") as f:
                json.load(f)
            logging.debug(f"Validated JSON file: {file_path}")
            return True
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logging.error(f"Validation failed for {file_path}: {e}")
            return False

    def _check_required_files(self) -> None:
        """Checks that all required files exist and are valid JSON."""
        for file_name in self.REQUIRED_FILES:
            file_path = self.job_settings.metadata_dir / file_name
            if (
                not self._validate_json(file_path)
                and not self.job_settings.dry_run
            ):
                raise FileNotFoundError(
                    f"Required file {file_name} is missing or invalid."
                )

    def _check_optional_files(self) -> None:
        """Checks that optional files, if they exist, are valid JSON."""
        for file_name in self.OPTIONAL_FILES:
            file_path = self.job_settings.metadata_dir / file_name
            if file_path.exists():
                if (
                    not self._validate_json(file_path)
                    and not self.job_settings.dry_run
                ):
                    raise ValueError(f"Optional file {file_name} is invalid.")

    def _check_either_or_files(self) -> None:
        """
        Checks that at least one file in each either-or pair exists
        and is valid JSON.
        """
        for file_pair in self.EITHER_OR_FILES:
            valid = False
            for file_name in file_pair:
                file_path = self.job_settings.metadata_dir / file_name
                if self._validate_json(file_path):
                    valid = True
                    break
            if not valid and not self.job_settings.dry_run:
                raise FileNotFoundError(
                    f"None of the files in {file_pair} exist or are valid."
                )

    def run_job(self):
        """Main job runner. Validates metadata files."""
        job_start_time = time()
        logging.info("Starting metadata validation job.")
        self._check_required_files()
        self._check_optional_files()
        self._check_either_or_files()
        job_end_time = time()
        execution_time = job_end_time - job_start_time
        logging.info(
            f"Metadata validation completed in {execution_time:.2f} seconds."
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    main_job = CheckMetadataJob(job_settings=main_job_settings)
    main_job.run_job()
