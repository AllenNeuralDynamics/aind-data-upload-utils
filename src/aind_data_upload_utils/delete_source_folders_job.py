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
from urllib.parse import urlparse

import boto3
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# Set log level from env var
from aind_data_upload_utils.delete_staging_folder_job import (
    DeleteStagingFolderJob,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)


class DirectoriesToDeleteConfigs(BaseModel):
    """Basic model that can be easily passed via the transfer service."""

    modality_sources: Dict[str, str] = Field(
        default=dict(),
        description="Looks like {'ecephys':'folder', 'behavior': 'folder2'}",
    )
    metadata_dir: Optional[str] = Field(default=None)
    derivatives_dir: Optional[str] = Field(default=None)


class JobSettings(BaseSettings):
    """Job settings for DeleteSourceFoldersJob"""

    directories: DirectoriesToDeleteConfigs
    num_of_dir_levels: int = Field(
        default=4,
        description="Number of subdirectory levels to remove",
    )
    n_partitions: int = Field(
        default=20, description="Number of dask tasks to run in parallel"
    )
    dry_run: bool = Field(
        default=True,
        description="Log commands without actually deleting anything",
    )
    s3_location: str = Field(
        description="Will verify the s3_location exists first."
    )
    modalities_to_delete: Optional[List[str]] = Field(
        default=None,
        description=(
            "If not None, then will only delete the modality folders or the"
            " derivatives folder if in this list."
        ),
    )
    # In addition to managing permissions, the parent directory
    # pattern is also hard-coded for extra security. We don't want
    # requests to remove anything outside this directory.
    pattern_to_match: ClassVar[re.Pattern] = re.compile(
        r"^/{1,2}allen/aind/(?:stage|scratch)/.+/.+"
    )


class DeleteSourceFoldersJob(DeleteStagingFolderJob):
    """Job to remove source folders."""

    # noinspection PyMissingConstructor
    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for DeleteSourceFoldersJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _s3_check(self) -> None:
        """Check that s3 obj exists with expected subdirectories and files."""
        logging.info(f"Checking {self.job_settings.s3_location}.")
        s3_location = self.job_settings.s3_location
        parsed_url = urlparse(s3_location, allow_fragments=False)
        bucket = parsed_url.netloc
        prefix = f"{parsed_url.path.lstrip('/')}/"
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter="/"
        )
        if response["IsTruncated"]:
            raise Exception(f"Unexpected number of objects in {s3_location}!")
        keys = [row["Key"] for row in response["Contents"]]
        common_prefixes = [row["Prefix"] for row in response["CommonPrefixes"]]
        s3_files = [k.replace(prefix, "") for k in keys]
        s3_folders = [
            p.replace(prefix, "").strip("/") for p in common_prefixes
        ]
        logging.info(f"Files in S3: {s3_files}.")
        logging.info(f"Folders in S3: {s3_folders}.")
        local_md_files = []
        local_md_dir = self.job_settings.directories.metadata_dir
        if local_md_dir is not None:
            md_files = os.listdir(local_md_dir)
            local_md_files = [f for f in md_files if f.endswith(".json")]
        files_in_both_places = set(s3_files).intersection(local_md_files)
        files_locally_not_in_s3 = set(local_md_files).difference(
            files_in_both_places
        )
        if files_locally_not_in_s3 != set():
            raise Exception(
                f"There are files in {local_md_dir} not found in S3! "
                f"{files_locally_not_in_s3}"
            )
        local_srcs = self.job_settings.directories.modality_sources
        local_dirs = set(local_srcs.keys())
        if self.job_settings.directories.derivatives_dir is not None:
            local_dirs.add("derivatives")
        dirs_in_both_places = set(s3_folders).intersection(local_dirs)
        dirs_locally_not_in_s3 = set(local_dirs).difference(
            dirs_in_both_places
        )
        if dirs_locally_not_in_s3 != set():
            raise Exception(
                f"There are directories in {self.job_settings.directories} "
                f"not found in S3! {dirs_locally_not_in_s3}"
            )
        logging.info(f"Finished checking {self.job_settings.s3_location}.")

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
            if (
                self.job_settings.modalities_to_delete is None
                or modality_abbr in self.job_settings.modalities_to_delete
            ):
                source_dir = modality_source
                directories_to_delete.append(Path(source_dir))
        return directories_to_delete

    def run_job(self):
        """Main job runner. Walks num_of_dir_levels deep and removes all
        subdirectories in that level. Then removes top directory."""
        job_start_time = time()
        self._s3_check()
        folders_to_remove = self._get_list_of_modality_directories()
        for folder in folders_to_remove:
            # Remove batches of subdirectories in parallel
            list_of_sub_dirs = self._get_list_of_sub_directories(folder=folder)
            self._remove_subdirectories(list_of_sub_dirs)
            # Remove top-level folder
            self._remove_directory(folder.as_posix().rstrip("/"))
        derivatives_dir = self.job_settings.directories.derivatives_dir
        if derivatives_dir is not None and (
            self.job_settings.modalities_to_delete is None
            or "derivatives" in self.job_settings.modalities_to_delete
        ):
            self._remove_directory(derivatives_dir)
        # Remove metadata_dir last since that might be in top level
        metadata_dir = self.job_settings.directories.metadata_dir
        if (
            metadata_dir is not None
            and self.job_settings.modalities_to_delete is None
        ):
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
