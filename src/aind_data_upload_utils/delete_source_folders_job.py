"""
Module to handle removing source folders.
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from time import time
from typing import ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# Set log level from env var
from aind_data_upload_utils.delete_staging_folder_job import (
    DeleteStagingFolderJob,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class DirectoriesToDeleteConfigs(BaseModel):
    """Basic model needed from BasicUploadConfigs"""

    modality_sources: Dict[str, str] = Field(
        default=dict(),
        description="Looks like {'ecephys':'folder', 'behavior': 'folder2'}",
    )
    metadata_dir: Optional[str] = Field(default=None)


class JobSettings(BaseSettings):
    """Job settings for DeleteStagingFolderJob"""

    directories: DirectoriesToDeleteConfigs
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
        r"^/allen/aind/stage/svc_aind_airflow/(?:prod|dev)/.+|"
        r"^/allen/aind/scratch/.+/.+"
    )


class DeleteSourceFoldersJob(DeleteStagingFolderJob):
    """Job to remove source folders."""

    # noinspection PyMissingConstructor
    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for DeleteStagingFolderJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _get_list_of_modality_directories(self) -> List[Union[Path, str]]:
        """
        Extracts a list of modality sources.
        Returns
        -------
        List[Union[Path, str]]

        """
        dirs_to_del_configs = self.job_settings.directories
        directories_to_delete = []
        # First, get modality directories
        for (
            modality_abbr,
            modality_source,
        ) in dirs_to_del_configs.modality_sources.items():
            source_dir = modality_source
            directories_to_delete.append(Path(source_dir))
        return directories_to_delete

    def run_job(self):
        """Main job runner. Walks num_of_dir_levels deep and removes all
        subdirectories in that level. Then removes top directory."""
        job_start_time = time()
        folders_to_remove = self._get_list_of_modality_directories()
        for folder in folders_to_remove:
            # Remove batches of subdirectories in parallel
            list_of_sub_dirs = self._get_list_of_sub_directories(folder=folder)
            self._remove_subdirectories(list_of_sub_dirs)
            # Remove top-level folder
            self._remove_directory(folder.as_posix().rstrip("/"))
        metadata_dir = self.job_settings.directories.metadata_dir
        if metadata_dir is not None:
            self._remove_directory(metadata_dir)
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
    main_job = DeleteSourceFoldersJob(job_settings=main_job_settings)
    main_job.run_job()
