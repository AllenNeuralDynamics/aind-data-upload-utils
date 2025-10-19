"""
Module to handle deleting staging folder using dask
"""

import argparse
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from time import time
from typing import ClassVar, List

from dask import bag as dask_bag
from pydantic import Field
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for DeleteStagingFolderJob"""

    staging_directory: Path = Field(..., description="Folder(s) to delete.")
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
    pattern_to_match: ClassVar[re.Pattern] = re.compile(
        r"^/allen/aind/stage/svc_aind_airflow/(?:prod|dev)/.+"
    )


class DeleteStagingFolderJob:
    """Job to delete a staging folder. Uses dask to prune subdirectories."""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for DeleteStagingFolderJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _get_list_of_sub_directories(self, folder: Path) -> List[str]:
        """
        Extracts a list folder. Will traverse
        self.job_settings.num_of_dir_levels deep.

        Parameters
        ----------
        folder : Path

        Returns
        -------
        List[str]

        """

        sub_directories_to_remove = []
        max_depth = self.job_settings.num_of_dir_levels

        def do_scan(start_dir: Path, output: list, depth=0):
            """Recursively iterate through directories up to max_depth.
            Modification of:
            https://stackoverflow.com/a/42720847
            """
            for f in start_dir.iterdir():
                if f.is_dir() and not f.is_symlink() and depth < max_depth:
                    do_scan(f, output, depth + 1)
                elif depth == max_depth and f.is_dir() and not f.is_symlink():
                    output.append(f)

        do_scan(folder, sub_directories_to_remove)
        return [d.as_posix() for d in sub_directories_to_remove]

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
        # Verify directory to remove is under parent directory
        norm_path = os.path.normpath(directory)
        if norm_path != directory or not os.path.isabs(directory):
            raise Exception(
                f"{directory} needs to be absolute and normalized!"
            )
        if not re.match(self.job_settings.pattern_to_match, directory):
            raise Exception(
                f"Directory {directory} is not under parent folder! "
                f"Will not remove automatically!"
            )
        elif not os.path.exists(directory):
            logging.warning(f"{directory} does not exist!")
        elif self.job_settings.dry_run:
            logging.info(f"(DRYRUN): shutil.rmtree({directory})")
        else:
            logging.info(f"Removing {directory}.")
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
        folder = self.job_settings.staging_directory
        list_of_sub_dirs = self._get_list_of_sub_directories(folder=folder)
        self._remove_subdirectories(list_of_sub_dirs)
        # Remove top-level staging folder
        self._remove_directory(folder.as_posix().rstrip("/"))
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
