"""
Small job to copy metadata files from one folder to another.
"""

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Set, Union

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
    output_directory: Union[Path, str] = Field(
        ..., description="Where to copy the files to."
    )
    possible_files: Set[str] = Field(
        default={
            "data_description.json",
            "subject.json",
            "procedures.json",
            "processing.json",
            "quality_control.json",
            "instrument.json",
            "rig.json",
            "acquisition.json",
            "session.json",
        },
        description="Set of possible files to copy over.",
    )


class CopyMetadataJob:
    """Job to copy metadata files from one folder to another."""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for CopyMetadataJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def run_job(self):
        """Main job runner."""

        all_json_files = {
            file.name
            for file in Path(self.job_settings.metadata_dir).glob("*.json")
        }
        metadata_files = self.job_settings.possible_files.intersection(
            all_json_files
        )
        logging.debug(f"metadata_files: {metadata_files}")
        for file_name in metadata_files:
            src_path = Path(self.job_settings.metadata_dir) / file_name
            dst_path = Path(self.job_settings.output_directory) / file_name
            shutil.copy(src_path, dst_path)


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
    main_job = CopyMetadataJob(job_settings=main_job_settings)
    main_job.run_job()
