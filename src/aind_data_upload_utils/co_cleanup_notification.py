"""
Job to parse CSV data and send webhook notifications.
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Union

import requests
from pydantic import Field
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for WebhookNotificationJob"""

    csv_file: Union[Path, str] = Field(
        ..., description="Path to the CSV file to parse."
    )
    exclude_list_file: Union[Path, str] = Field(
        ..., description="Path to the plain text file containing excluded row numbers."
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

    def parse_csv(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Parses the CSV file and groups capsule URLs by user email.
        
        Returns
        -------
        Dict[str, List[Dict[str, str]]]
            Dictionary with user emails as keys and lists of capsule data as values.
        """
        # Read exclude list
        exclude_rows = set()
        exclude_file_path = Path(self.job_settings.exclude_list_file)
        if exclude_file_path.exists():
            with open(exclude_file_path, 'r', encoding='utf-8') as f:
                exclude_content = f.read().strip()
                if exclude_content:
                    exclude_rows = {
                        int(row.strip()) - 1 for row in exclude_content.split(',')
                        if row.strip().isdigit()
                    }
        
        logging.debug(f"Exclude rows: {exclude_rows}")
        
        # Parse CSV file
        user_data = defaultdict(list)
        csv_file_path = Path(self.job_settings.csv_file)
        
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row_index, row in enumerate(csv_reader):
                if row_index in exclude_rows:
                    user_email = row.get('user_email', 'N/A')
                    logging.info(f"Excluding row {row_index + 1}: {user_email}")
                    continue
                
                user_email = row["user_email"]
                capsule_data = {
                    "capsule_url": row["capsule_url"]
                }
                user_data[user_email].append(capsule_data)
        
        logging.debug(f"Parsed data for {len(user_data)} users")
        return dict(user_data)

    def send_webhook_notifications(self, user_data: Dict[str, List[Dict[str, str]]]) -> None:
        """
        Sends POST requests to the webhook endpoint.
        
        Parameters
        ----------
        user_data: Dict[str, List[Dict[str, str]]]
            Dictionary with user emails as keys and lists of capsule data as values.
        """
        webhook_url = self.job_settings.webhook_url
        
        for user_email, capsules in user_data.items():
            table_rows = ""
            for capsule in capsules:
                capsule_url = capsule["capsule_url"]
                table_rows += f"{capsule_url}<br>"
            
            html_table = f"<body>{table_rows}</body>"
            payload = {
                "user_email": user_email,
                "capsule_urls": html_table
            }
            
            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    verify=False,
                    timeout=30
                )
                response.raise_for_status()
                logging.info(f"Successfully sent notification for {user_email}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to send notification for {user_email}: {e}")

    def run_job(self) -> None:
        """Main job runner."""
        logging.info("Starting webhook notification job")
        
        # Parse CSV data
        user_data = self.parse_csv()
        
        # Send notifications
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
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    main_job = WebhookNotificationJob(job_settings=main_job_settings)
    main_job.run_job()
