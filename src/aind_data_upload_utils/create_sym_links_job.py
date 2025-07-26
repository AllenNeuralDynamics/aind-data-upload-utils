"""Job to symlink a directory or files that match a specific pattern."""

import logging
import os
import sys
from glob import glob
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(
    BaseSettings, cli_parse_args=True, cli_ignore_unknown_args=True
):
    """Job settings for CreateSymLinksJob"""

    input_source: str = Field(...)
    output_directory: str = Field(...)
    chunk: Optional[str] = Field(default=None)
    dry_run: bool = Field(
        default=False,
        description="Log commands without actually deleting anything",
    )


class CreateSymLinksJob:
    """Job to create sym links from a source folder to a destination folder."""

    def __init__(self, job_settings: JobSettings):
        """Class constructor. Overrides parent constructor."""
        self.job_settings = job_settings

    @staticmethod
    def _create_sym_link(src: str, dst: str, dry_run: bool):
        """Create a sym link"""
        if os.path.exists(dst):
            logging.warning(f"Destination {dst} exists! Will skip linking.")
        elif os.path.isdir(src) and not dry_run:
            logging.debug(f"Sym linking {src} to {dst}")
            os.symlink(src=src, dst=dst, target_is_directory=True)
        elif os.path.isdir(src) and dry_run:
            logging.info(
                f"(dryrun): os.symlink(src={src}, dst={dst},"
                f" target_is_directory=True)"
            )
        elif not os.path.isdir(src) and not dry_run:
            logging.debug(f"Sym linking {src} to {dst}")
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.symlink(src=src, dst=dst, target_is_directory=False)
        else:
            logging.info(
                f"(dryrun): os.makedirs(os.path.dirname({dst}), exist_ok=True)"
            )
            logging.info(
                f"(dryrun): os.symlink(src={src}, dst={dst},"
                f" target_is_directory=False)"
            )

    def _extract_list_of_files(self) -> List[str]:
        """Extract a list of files to symlink."""

        logging.debug("Extracting list of files")
        chunk = self.job_settings.chunk
        directory_path = self.job_settings.input_source
        if chunk is None:
            paths_to_process = [self.job_settings.input_source]
        else:
            all_paths = glob(
                os.path.join(directory_path, "**", f"*{chunk}*"),
                recursive=True,
            )
            paths_to_process = [
                path
                for path in all_paths
                if os.path.isfile(path) or os.path.islink(path)
            ]
        return paths_to_process

    def run_job(self):
        """Run the job."""
        logging.debug(f"Running job with settings {self.job_settings}")
        path_list = self._extract_list_of_files()
        for f in path_list:
            if os.path.isdir(f):
                dst = self.job_settings.output_directory
            else:
                rel_path = os.path.relpath(
                    f, start=self.job_settings.input_source
                )
                dst = os.path.join(
                    self.job_settings.output_directory, rel_path
                )
            self._create_sym_link(
                src=f, dst=dst, dry_run=self.job_settings.dry_run
            )
        logging.debug("Finished job.")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if len(sys_args) == 2 and sys_args[0] == "--job-settings":
        main_settings = JobSettings.model_validate_json(sys_args[1])
    else:
        main_settings = JobSettings()
    job = CreateSymLinksJob(job_settings=main_settings)
    job.run_job()
