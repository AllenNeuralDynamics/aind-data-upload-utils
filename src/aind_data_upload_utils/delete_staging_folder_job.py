"""
Module to handle deleting staging folder using dask
"""

import argparse
import logging
import os
import re
import shutil
import sys
from glob import glob
from pathlib import Path
from re import Pattern
from time import time
from typing import ClassVar, List

from dask import bag as dask_bag
from pydantic import Field
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for UploadJob"""

    staging_directory: Path = Field(
        ..., description="staging folder to delete"
    )
    num_of_dir_levels: int = Field(
        default=4,
        description="Number of subdirectory levels to remove",
    )
    n_partitions: int = Field(
        default=20, description="Number of dask tasks to run in parallel"
    )
    dry_run: bool = Field(
        default=False,
        description="Log commands without actually deleting anything",
    )

    # In addition to managing permissions, the parent directory
    # pattern is also hard-coded for extra security. We don't want
    # requests to remove anything outside this directory.
    pattern_to_match: ClassVar[Pattern] = re.compile(
        r"^/allen/aind/stage/svc_aind_airflow/(?:prod|dev)/.*"
    )


class DeleteStagingFolderJob:
    """Job to scan basic upload job configs source directories for broken
    symlinks"""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for UploadJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _get_list_of_sub_directories(self) -> List[str]:
        """
        Extracts a list from self.job_settings.staging_directory.
        Will travers self.job_settings.num_of_dir_levels deep.
        Returns
        -------
        List[str]
          List of paths rendered as posix strings

        """

        base_path = self.job_settings.staging_directory.as_posix().rstrip("/")
        sub_directories_to_remove = []
        for _ in range(0, self.job_settings.num_of_dir_levels + 1):
            base_path = base_path + "/*"
        for sub_path in glob(base_path):
            if os.path.isdir(Path(sub_path).resolve()):
                sub_directories_to_remove.append(
                    Path(sub_path).as_posix().rstrip("/")
                )
        return sub_directories_to_remove

    def _remove_directory(self, directory: str) -> None:
        """
        Removes a directory using shutil.rmtree
        Parameters
        ----------
        directory : str

        Returns
        -------
        None
          Raises an error if directory does not match regex pattern.

        """
        # Verify directory to remove is under staging directory
        if not re.match(self.job_settings.pattern_to_match, directory):
            raise Exception(
                f"Directory {directory} is not under staging folder! "
                f"Will not remove automatically!"
            )
        elif self.job_settings.dry_run:
            logging.info(f"Removing: {directory}")
        else:
            shutil.rmtree(directory)

    def _dask_task_to_process_directory_list(
        self, directories: List[str]
    ) -> None:
        """
        Removes each directory in list
        Parameters
        ----------
        directories : List[str]

        Returns
        -------
        None
          Will raise an error if a request is made to remove directory
          outside of staging folder.

        """
        logging.debug(f"Removing list: {directories}")
        total_to_scan = len(directories)
        for dir_counter, directory in enumerate(directories, start=1):
            logging.debug(
                f"Removing {directory}. On {dir_counter} of {total_to_scan}"
            )
            # Verify directory to remove is under staging directory!
            self._remove_directory(directory)

    def _remove_subdirectories(self, sub_directories: List[str]) -> None:
        """
        Uses dask to partition list of directory paths to remove and removes
        the partitioned lists in parallel.
        Returns
        -------
        None
          Will raise an error if a request is made to remove a directory
          outside the staging folder.
        """
        # We'll use dask to partition the sub_directories.
        directory_bag = dask_bag.from_sequence(
            sub_directories, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_directory_list, directory_bag
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main job runner. Walks num_of_dir_levels deep and removes all
        subdirectories in that level. Then removes top directory."""
        job_start_time = time()
        # Remove batches of subdirectories in parallel
        list_of_sub_dirs = self._get_list_of_sub_directories()
        self._remove_subdirectories(list_of_sub_dirs)
        # Remove top-level staging folder
        self._remove_directory(
            self.job_settings.staging_directory.as_posix().rstrip("/")
        )
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
    main_job = DeleteStagingFolderJob(job_settings=main_job_settings)
    main_job.run_job()
