"""
Job to parse CSV data and send webhook notifications.
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Dict, List, Set, Union

import boto3
import requests
from pydantic import Field
from pydantic_settings import BaseSettings

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for WebhookNotificationJob"""

    csv_file: Union[Path, str] = Field(
        ..., description="Path to the CSV file to parse (local/S3)."
    )
    exclude_list_file: Union[Path, str] = Field(
        ...,
        description=(
            "Path to the plain text file containing excluded "
            "usernames or capsule URLs (one per line, local/S3)."
        ),
    )
    webhook_url: str = Field(
        ..., description="Webhook URL to send notifications to."
    )


class WebhookNotificationJob:
    """Job to parse CSV data and send webhook notifications."""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for WebhookNotificationJob.

        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    def _is_s3_uri(self, path: Union[Path, str]) -> bool:
        """
        Check if the given path is an S3 URI.

        Parameters
        ----------
        path: Union[Path, str]
            Path to check.

        Returns
        -------
        bool
            True if path is an S3 URI, False otherwise.
        """
        return str(path).startswith("s3://")

    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """
        Parse S3 URI into bucket and key.

        Parameters
        ----------
        s3_uri: str
            S3 URI in format s3://bucket/key.

        Returns
        -------
        tuple[str, str]
            Tuple of (bucket, key).
        """
        path_part = s3_uri[5:]
        bucket, key = path_part.split("/", 1)
        return bucket, key

    def read_exclude_list(self) -> Set[str]:
        """
        Reads the exclude list file and returns a set of items to exclude.

        Returns
        -------
        Set[str]
            Set of usernames or capsule URLs to exclude.
        """
        exclude_items = set()
        exclude_file_path = self.job_settings.exclude_list_file

        if self._is_s3_uri(exclude_file_path):
            bucket, key = self._parse_s3_uri(str(exclude_file_path))
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            exclude_content = response["Body"].read().decode("utf-8").strip()
            s3_client.close()
            logging.debug(f"Read exclude list from S3: s3://{bucket}/{key}")
        else:
            exclude_file_path = Path(exclude_file_path)
            with open(exclude_file_path, "r", encoding="utf-8") as f:
                exclude_content = f.read().strip()
            logging.debug(
                f"Read exclude list from local file: {exclude_file_path}"
            )

        if exclude_content:
            exclude_items = {
                item.strip()
                for item in exclude_content.split("\n")
                if item.strip()
            }

        logging.debug(f"Exclude items: {exclude_items}")
        return exclude_items

    def read_csv_file(self) -> List[Dict[str, str]]:
        """
        Reads the CSV file and returns all rows as a list of dictionaries.

        Returns
        -------
        List[Dict[str, str]]
            List of dictionaries representing CSV rows.
        """
        csv_file_path = self.job_settings.csv_file

        if self._is_s3_uri(csv_file_path):
            bucket, key = self._parse_s3_uri(str(csv_file_path))
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = response["Body"].read().decode("utf-8")
            s3_client.close()
            logging.debug(f"Read CSV from S3: s3://{bucket}/{key}")

            csv_data = []
            csv_reader = csv.DictReader(StringIO(csv_content))
            for row in csv_reader:
                csv_data.append(dict(row))
        else:
            csv_data = []
            csv_file_path = Path(csv_file_path)
            with open(csv_file_path, "r", encoding="utf-8") as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    csv_data.append(dict(row))
            logging.debug(f"Read CSV from local file: {csv_file_path}")

        logging.debug(f"Read {len(csv_data)} rows from CSV file")
        return csv_data

    def filter_csv_data(
        self, csv_data: List[Dict[str, str]], exclude_items: Set[str]
    ) -> List[Dict[str, str]]:
        """
        Filters CSV data by excluding specified usernames or capsule URLs.

        Parameters
        ----------
        csv_data: List[Dict[str, str]]
            List of dictionaries representing CSV rows.
        exclude_items: Set[str]
            Set of usernames or capsule URLs to exclude.

        Returns
        -------
        List[Dict[str, str]]
            Filtered list of dictionaries.
        """
        filtered_data = []

        for row_index, row in enumerate(csv_data):
            user_email = row["user_email"]
            capsule_url = row["capsule_url"]

            if user_email in exclude_items or capsule_url in exclude_items:
                logging.info(
                    f"Excluding row {row_index + 1}: {user_email} - "
                    f"{capsule_url}"
                )
                continue

            filtered_data.append(row)

        logging.debug(f"Filtered data: {len(filtered_data)} rows remaining")
        return filtered_data

    def group_by_user(
        self, filtered_data: List[Dict[str, str]]
    ) -> Dict[str, List[Dict[str, str]]]:
        """
        Groups filtered CSV data by user email.

        Parameters
        ----------
        filtered_data: List[Dict[str, str]]
            Filtered list of dictionaries representing CSV rows.

        Returns
        -------
        Dict[str, List[Dict[str, str]]]
            Dictionary with user emails as keys and lists of capsule data.
        """
        user_data = defaultdict(list)

        for row in filtered_data:
            user_email = row["user_email"]
            capsule_data = {"capsule_url": row["capsule_url"]}
            user_data[user_email].append(capsule_data)

        logging.debug(f"Grouped data for {len(user_data)} users")
        return dict(user_data)

    def send_webhook_notifications(
        self, user_data: Dict[str, List[Dict[str, str]]]
    ) -> None:
        """
        Sends POST requests to the webhook endpoint.

        Parameters
        ----------
        user_data: Dict[str, List[Dict[str, str]]]
            Dictionary with user emails as keys and lists of capsule data.
        """
        webhook_url = self.job_settings.webhook_url

        for user_email, capsules in user_data.items():
            table_rows = ""
            for capsule in capsules:
                capsule_url = capsule["capsule_url"]
                table_rows += f"{capsule_url}<br>"

            html_table = f"<body>{table_rows}</body>"
            payload = {"user_email": user_email, "capsule_urls": html_table}

            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    verify=False,
                    timeout=30,
                )
                response.raise_for_status()
                logging.info(
                    f"Successfully sent notification for {user_email}"
                    )
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Failed to send notification for {user_email}: {e}"
                )
                raise

    def run_job(self) -> None:
        """Main job runner."""
        logging.info("Starting webhook notification job")

        exclude_items = self.read_exclude_list()
        csv_data = self.read_csv_file()
        filtered_data = self.filter_csv_data(csv_data, exclude_items)
        user_data = self.group_by_user(filtered_data)
        self.send_webhook_notifications(user_data)
        logging.info("Webhook notification job completed")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=False,
        type=str,
        help=(
            "Instead of init args the job settings can optionally be passed "
            "as a json string in the command line."
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    main_job = WebhookNotificationJob(job_settings=main_job_settings)
    main_job.run_job()
