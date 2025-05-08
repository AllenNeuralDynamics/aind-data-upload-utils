"""
Module to check that certain metadata files exist and are in valid format.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Set, Tuple, Union

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
    required_files: Set[str] = Field(
        default={
            "data_description.json",
            "subject.json",
            "procedures.json",
        },
        description="List of required metadata files.",
    )
    optional_files: Set[str] = Field(
        default={
            "processing.json",
            "quality_control.json",
        },
        description="List of optional metadata files.",
    )
    either_or_files: List[Tuple[str, str]] = Field(
        default={
            ("instrument.json", "rig.json"),
            ("acquisition.json", "session.json"),
        },
        description="List of either/or metadata file pairs.",
    )

    @property
    def all_files(self) -> Set[str]:
        """Return all possible metadata files."""
        return self.required_files.union(self.optional_files).union(
            set([x for xs in self.either_or_files for x in xs])
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
    def _check_either_or_pair(
        pair: Tuple[str, str], metadata_files: Set[str]
    ) -> None:
        """Verify that only of the files in a pair is in the metadata dir."""

        if not pair[0] in metadata_files and not pair[1] in metadata_files:
            raise FileNotFoundError(f"Neither of the files in {pair} exist!")
        if pair[0] in metadata_files and pair[1] in metadata_files:
            raise ValueError(f"Only one of {pair} can be present!")
        return None

    def run_job(self):
        """Main job runner. Validates metadata files."""

        json_files = {
            file.name
            for file in Path(self.job_settings.metadata_dir).glob("*.json")
        }

        metadata_files = self.job_settings.all_files.intersection(json_files)
        missing_required = (
            set(self.job_settings.required_files) - metadata_files
        )
        if missing_required:
            raise FileNotFoundError(
                f"Missing required files: {missing_required}"
            )

        for file_pair in self.job_settings.either_or_files:
            self._check_either_or_pair(
                pair=file_pair, metadata_files=metadata_files
            )

        # Validate json
        for file_name in metadata_files:
            path = Path(self.job_settings.metadata_dir) / file_name
            with open(path, "r") as f:
                json.load(f)


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
