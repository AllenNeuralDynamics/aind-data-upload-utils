"""
Module to handle deleting chunked files from source
"""

import argparse
import logging
import os
import re
import sys
from glob import glob
from pathlib import Path
from time import time
from typing import ClassVar, Dict, List, Optional

import boto3
from pydantic import Field
from pydantic_settings import BaseSettings


class JobSettings(BaseSettings):
    """Job settings for DeleteSourceFoldersJob"""

    dry_run: bool = Field(
        default=True,
        description="Log commands without actually deleting anything",
    )
    s3_bucket: str = Field(description="Bucket where data was moved to.")
    s3_prefix: str = Field(description="S3 prefix where data was moved to.")
    modality_sources: Dict[str, str] = Field(
        default=dict(),
        description="Looks like {'ecephys':'folder', 'behavior': 'folder2'}",
    )
    chunk: str = Field(...)
    local_chunk_not_in_s3: Optional[str] = Field(default=None)
    # In addition to managing permissions, the parent directory
    # pattern is also hard-coded for extra security. We don't want
    # requests to remove anything outside this directory.
    pattern_to_match: ClassVar[re.Pattern] = re.compile(
        r"^/{1,2}allen/aind/(?:stage|scratch)/.+/.+"
    )


class DeleteChunkFilesJob:
    """Job to files that were uploaded to S3."""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for DeleteFoldersJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _extract_list_of_local_files(self) -> List[str]:
        """Extract a list of local files that match a chunk pattern."""

        logging.debug("Extracting list of files")
        chunk = self.job_settings.chunk
        modality_parent_dirs = self.job_settings.modality_sources.values()
        paths_to_process = []
        for modality_dir in modality_parent_dirs:
            all_paths = glob(
                os.path.join(modality_dir, "**", "*"),
                recursive=True,
            )
            sub_paths_to_process = [
                path
                for path in all_paths
                if (
                    (os.path.isfile(path) or os.path.islink(path))
                    and re.search(chunk, path)
                )
            ]
            paths_to_process.extend(sub_paths_to_process)
        return paths_to_process

    def _get_list_of_s3_files(self) -> List[str]:
        """Return a list of chunks from S3."""
        logging.debug("Extracting list of files from S3.")
        bucket_name = self.job_settings.s3_bucket
        s3_folder = self.job_settings.s3_prefix
        chunk = self.job_settings.chunk
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        file_keys = []
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_folder)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_keys.append(obj["Key"])
        filtered_keys = [k for k in file_keys if re.search(chunk, k)]
        return filtered_keys

    def _get_list_of_files_to_delete(
        self, local_files: List[str], s3_files: List[str]
    ) -> List[str]:
        """Compare local files to s3 files and get list of files to delete."""
        local_file_names = [Path(r).name for r in local_files]
        s3_file_names = [r.split("/")[-1] for r in s3_files]
        if len(local_file_names) != len(set(local_file_names)):
            raise ValueError("Local file names must be unique!")
        if len(s3_file_names) != len(set(s3_file_names)):
            raise ValueError("S3 file names must be unique!")
        files_to_delete = []
        for local_file in local_files:
            filename = Path(local_file).name
            if filename in s3_file_names or re.search(
                self.job_settings.local_chunk_not_in_s3, filename
            ):
                files_to_delete.append(local_file)
        return files_to_delete

    def _remove_list_of_files(self, files_to_delete: List[str]) -> None:
        """Remove a list of files."""
        for local_file in files_to_delete:
            if not self.job_settings.dry_run and os.path.isfile(local_file):
                logging.info(f"Removing {local_file}")
                os.remove(local_file)
            elif not self.job_settings.dry_run and not os.path.isfile(
                local_file
            ):
                logging.warning(
                    f"{local_file} not found! It may have been in a parent "
                    f"directory already removed."
                )
            else:
                logging.info(f"(DRYRUN): os.remove('{local_file}')")

    def run_job(self):
        """Main job runner. Walks num_of_dir_levels deep and removes all
        subdirectories in that level. Then removes top directory."""
        job_start_time = time()
        local_files = self._extract_list_of_local_files()
        s3_files = self._get_list_of_s3_files()
        files_to_remove = self._get_list_of_files_to_delete(
            local_files=local_files, s3_files=s3_files
        )
        self._remove_list_of_files(files_to_remove)
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
    main_job = DeleteChunkFilesJob(job_settings=main_job_settings)
    main_job.run_job()
