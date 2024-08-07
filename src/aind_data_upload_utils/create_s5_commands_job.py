"""
Module to handle creating s5 commands. Scans the first few levels of a staging
folder and creates a list of commands for s5cmd to run in parallel. Will save
the commands to a text file. s5cmd can then be called via singularity like:
  >> singularity exec docker://peakcom/s5cmd:v2.2.2 /s5cmd --log error run
       s5_commands.txt
"""

import argparse
import logging
import os
import sys
from glob import glob
from pathlib import Path
from time import time
from typing import List, Optional, Union

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for CreateS5CommandsJob"""

    s3_location: str = Field(
        ..., description="S3 bucket and prefix to upload staging directory to"
    )
    staging_directory: Path = Field(
        ..., description="Location of the staging directory"
    )
    num_of_dir_levels_to_partition: int = Field(
        default=4,
        description=(
            "Will scan the directory tree of the staging folder up to this "
            "number of levels. All of the files and directories this many "
            "levels deep will be used to generate s5cmd upload commands to "
            "run in parallel."
        ),
    )
    s5_commands_file: Optional[Path] = Field(
        validate_default=True,
        description=(
            "Location to save the s5cmd text file to. As default, will save "
            "to the staging folder."
        )
    )

    @model_validator(mode="after")
    def set_default_commands_file(self):
        if self.s5_commands_file is None:
            self.s5_commands_file = self.staging_directory / "s5_commands.txt"
        return self


class CreateS5CommandsJob:
    """Job to scan staging folder directory tree and output list of files
    and directories to upload to S3 using s5cmd"""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for CreateS5CommandsJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _map_file_path_to_s3_location(self, file_path: str) -> str:
        """
        Maps a local file path to a s3 object path.
        Parameters
        ----------
        file_path : str
          Example, '/allen/aind/stage/svc_aind_airflow/prod/abc_123'

        Returns
        -------
        str
          Example, 's3://some_bucket/ecephys_12345_2020-10-10_10-10-10/abc_123'

        """

        return file_path.replace(
            self.job_settings.staging_directory.as_posix().rstrip("/"),
            self.job_settings.s3_location.rstrip("/"),
            1,
        )

    def _create_file_cp_command(self, file_path: str) -> str:
        return (
            f'cp "{file_path}" '
            f'"{self._map_file_path_to_s3_location(file_path)}"'
        )

    def _create_directory_cp_command(self, directory_path: str) -> str:

        local_dir = f"{directory_path.rstrip('/')}/*"
        s3_directory = (
            f"{self._map_file_path_to_s3_location(directory_path).rstrip('/')}"
            f"/"
        )
        return f'cp "{local_dir}" "{s3_directory}"'

    def _get_list_of_upload_commands(self) -> List[str]:
        """
        Extracts a list of directories from self.job_settings.upload_configs
        to scan for broken symlinks. The list will be passed into dask to
        parallelize the scan. Will also scan files in top levels and raise an
        error if broken symlinks are found when compiling the list of dirs.
        Returns
        -------
        List[Union[Path, str]]

        """

        base_path = str(self.job_settings.staging_directory).rstrip("/")
        s5_commands = []
        for _ in range(0, self.job_settings.num_of_dir_levels_to_partition):
            base_path = base_path + "/*"
            for sub_path in glob(base_path):
                if os.path.isfile(Path(sub_path).resolve()):
                    s5_commands.append(self._create_file_cp_command(sub_path))
        base_path + "/*"
        for sub_path in glob(base_path):
            if os.path.isfile(Path(sub_path).resolve()):
                s5_commands.append(self._create_file_cp_command(sub_path))
            elif os.path.isdir(Path(sub_path).resolve()):
                s5_commands.append(self._create_directory_cp_command(sub_path))
            else:
                raise NotADirectoryError(
                    f"Possible broken file path: {sub_path}"
                )
        return s5_commands

    def _save_s5_commands_to_file(self, s5_commands: List[str]) -> None:
        with open(self.job_settings.s5_commands_file, "w") as f:
            for line in s5_commands:
                f.write(f"{line}\n")

    def run_job(self):
        job_start_time = time()
        list_of_upload_commands = self._get_list_of_upload_commands()
        self._save_s5_commands_to_file(list_of_upload_commands)
        job_end_time = time()
        execution_time = job_end_time - job_start_time
        logging.debug(f"Task took {execution_time} seconds")


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
    main_job = CreateS5CommandsJob(job_settings=main_job_settings)
    main_job.run_job()
