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
from typing import Union, List

from pydantic import Field
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for CheckMetadataJob"""

    metadata_dir: Union[Path, str] = Field(
        ..., description="Directory containing metadata JSON files."
    )
    required_files: List[str] = Field(
        default=[
            "data_description.json",
            "subject.json",
            "procedures.json",
        ],
        description="List of required metadata files.",
    )
    optional_files: List[str] = Field(
        default=[
            "processing.json",
            "quality_control.json",
        ],
        description="List of optional metadata files.",
    )
    either_or_files: List[tuple] = Field(
        default=[
            ("instrument.json", "rig.json"),
            ("acquisition.json", "session.json"),
        ],
        description="List of either/or metadata file pairs.",
    )


class CheckMetadataJob:
    """Job to validate the existence and format of metadata JSON files."""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for CheckMetadataJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    @staticmethod
    def _validate_json(file_path: Path) -> None:
        """
        Validates that a file exists and is valid JSON.

        Parameters
        ----------
        file_path : Path

         Raises
        ------
        FileNotFoundError
            If the file does not exist.
        json.JSONDecodeError
            If the file is not valid JSON.
        """
        with open(file_path, "r") as f:
            json.load(f)
            logging.debug(f"Validated JSON file: {file_path}")

    def run_job(self):
        """Main job runner. Validates metadata files."""
        job_start_time = time()
        logging.info("Starting metadata validation job.")
        available_files = {
            file.name for file in self.job_settings.metadata_dir.glob("*.json")
        }

        # Check required files
        missing_required = (
            set(self.job_settings.required_files) - available_files
        )
        if missing_required:
            raise FileNotFoundError(
                f"Missing required files: {missing_required}"
            )
        for file_name in self.job_settings.required_files:
            self._validate_json(self.job_settings.metadata_dir / file_name)

        # Check optional files
        for file_name in (
            set(self.job_settings.optional_files) & available_files
        ):
            self._validate_json(self.job_settings.metadata_dir / file_name)

        # Check either-or files
        for file_pair in self.job_settings.either_or_files:
            if not any(
                (self.job_settings.metadata_dir / file_name).exists()
                for file_name in file_pair
            ):
                raise FileNotFoundError(
                    f"Neither of the files in {file_pair} exist."
                )
            for file_name in file_pair:
                file_path = self.job_settings.metadata_dir / file_name
                if file_path.exists():
                    self._validate_json(file_path)
                    break

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
