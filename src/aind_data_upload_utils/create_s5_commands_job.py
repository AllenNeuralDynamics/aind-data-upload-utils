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
from typing import List, Optional

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
        default=None,
        validate_default=True,
        description=(
            "Location to save the s5cmd text file to. As default, will save "
            "to the staging folder."
        ),
    )

    @model_validator(mode="after")
    def set_default_commands_file(self):
        """If s5_commands_file is None, will default to saving the file to
        the staging directory."""
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
          Example,
          '/stage/ecephys_12345...-10/abc_123'

        Returns
        -------
        str
          Example, 's3://some_bucket/ecephys_12345...-10-10/abc_123'

        """

        return file_path.replace(
            self.job_settings.staging_directory.as_posix().rstrip("/"),
            self.job_settings.s3_location.rstrip("/"),
            1,
        )

    def _create_file_cp_command(self, file_path: str) -> str:
        """
        Maps a file path to command for s5cmd
        Parameters
        ----------
        file_path : str
          Example,
          '/stage/ecephys_12345...-10/ophys/hello.txt'

        Returns
        -------
        str
          Example,
          'cp "/stage/ecephys_12345...-10/ophys/hello.txt"
          "s3://some_bucket/ecephys_12345...-10/ophys/hello.txt"'

        """
        return (
            f'cp "{file_path}" '
            f'"{self._map_file_path_to_s3_location(file_path)}"'
        )

    def _create_directory_cp_command(self, directory_path: str) -> str:
        """
        Maps a dir path to command for s5cmd
        Parameters
        ----------
        directory_path : str
          Example,
          '/stage/ecephys_12345...-10/ophys/sub_dir'

        Returns
        -------
        str
          Example,
          'cp "/stage/ecephys_12345...-10/ophys/sub_dir/*"
          "s3://some_bucket/ecephys_12345...-10/ophys/sub_dir/"'

        """
        local_dir = f"{directory_path.rstrip('/')}/*"
        s3_directory = (
            f"{self._map_file_path_to_s3_location(directory_path).rstrip('/')}"
            f"/"
        )
        return f'cp "{local_dir}" "{s3_directory}"'

    def _get_list_of_upload_commands(self) -> List[str]:
        """
        Scans directory tree of the staging folder to generate a list of files
        and subdirectories to upload via s5cmd.
        Returns
        -------
        List[str]
          A list of s5cmd that can be run.

        """

        base_path = self.job_settings.staging_directory.as_posix().rstrip("/")
        s5_commands = []
        for _ in range(0, self.job_settings.num_of_dir_levels_to_partition):
            base_path = base_path + "/*"
            for sub_path in glob(base_path):
                if os.path.isfile(Path(sub_path).resolve()):
                    s5_commands.append(
                        self._create_file_cp_command(Path(sub_path).as_posix())
                    )
        base_path = base_path + "/*"
        for sub_path in glob(base_path):
            if os.path.isfile(Path(sub_path).resolve()):
                s5_commands.append(
                    self._create_file_cp_command(Path(sub_path).as_posix())
                )
            elif os.path.isdir(Path(sub_path).resolve()):
                s5_commands.append(
                    self._create_directory_cp_command(
                        Path(sub_path).as_posix()
                    )
                )
            else:
                raise NotADirectoryError(
                    f"Possible broken file path: {sub_path}"
                )
        return s5_commands

    def _save_s5_commands_to_file(self, s5_commands: List[str]) -> None:
        """
        Writes list of s5 commands to location defined in configs.
        Parameters
        ----------
        s5_commands : List[str]

        Returns
        -------
        None

        """
        with open(self.job_settings.s5_commands_file, "w") as f:
            for line in s5_commands:
                f.write(f"{line}\n")

    def run_job(self) -> None:
        """
        Runs job.
        - Scans staging directory to generate list of s5 commands
        - Saves commands to text file that can be used by s5cmd.
        Returns
        -------
        None

        """
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
