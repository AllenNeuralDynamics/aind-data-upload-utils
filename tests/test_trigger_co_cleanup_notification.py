"""Tests trigger_co_cleanup_notification module"""
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

from aind_data_upload_utils.trigger_co_cleanup_notification import (
    JobSettings,
    WebhookNotificationJob,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
CSV_FILE = RESOURCES_DIR / "example_capsules.csv"
EXCLUDE_FILE = RESOURCES_DIR / "exclude_list.txt"


class TestWebhookNotificationJob(unittest.TestCase):
    """Test class for WebhookNotificationJob."""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up job settings for all tests."""
        cls.job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test",
        )
        cls.example_job = WebhookNotificationJob(job_settings=cls.job_settings)

    def test_job_settings_properties(self):
        """Tests JobSettings properties."""
        self.assertEqual(self.job_settings.csv_file, CSV_FILE)
        self.assertEqual(self.job_settings.exclude_list_file, EXCLUDE_FILE)
        self.assertEqual(
            self.job_settings.webhook_url, "https://webhook.site/test"
        )

    def test_s3_uri_methods(self):
        """Tests S3 URI detection and parsing methods."""
        self.assertTrue(self.example_job._is_s3_uri("s3://bucket/key"))
        self.assertTrue(
            self.example_job._is_s3_uri("s3://bucket/folder/file.csv")
        )
        self.assertFalse(
            self.example_job._is_s3_uri("/local/path/file.csv")
        )
        self.assertFalse(self.example_job._is_s3_uri("file.csv"))
        # Test _parse_s3_uri method
        bucket, key = self.example_job._parse_s3_uri("s3://my-bucket/file.csv")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "file.csv")
        bucket, key = self.example_job._parse_s3_uri(
            "s3://aind-devops-dev/co_capsule_cleanup/list.csv"
        )
        self.assertEqual(bucket, "aind-devops-dev")
        self.assertEqual(key, "co_capsule_cleanup/list.csv")

    def test_read_exclude_list_local_file(self):
        """Tests read_exclude_list method with local file."""
        with self.assertLogs(level="DEBUG") as captured:
            exclude_items = self.example_job.read_exclude_list()
        self.assertIsInstance(exclude_items, set)
        self.assertIn("user2@example.com", exclude_items)
        debug_logs = [log for log in captured.output if "Exclude items" in log]
        self.assertEqual(len(debug_logs), 1)

    @patch("boto3.client")
    def test_read_exclude_list_s3_file(self, mock_boto3_client):
        """Tests read_exclude_list method with S3 file."""
        mock_s3_client = MagicMock()
        mock_response = {"Body": MagicMock()}
        mock_response["Body"].read.return_value = (
            b"user2@example.com\nuser3@example.com"
        )
        mock_s3_client.get_object.return_value = mock_response
        mock_boto3_client.return_value = mock_s3_client

        s3_job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file="s3://test-bucket/exclude.txt",
            webhook_url="https://webhook.site/test"
        )
        s3_job = WebhookNotificationJob(job_settings=s3_job_settings)

        exclude_items = s3_job.read_exclude_list()
        self.assertIn("user2@example.com", exclude_items)
        self.assertIn("user3@example.com", exclude_items)
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="exclude.txt"
        )
        mock_s3_client.close.assert_called_once()

    def test_read_csv_file_local(self):
        """Tests read_csv_file method with local file."""
        with self.assertLogs(level="DEBUG") as captured:
            csv_data = self.example_job.read_csv_file()
        self.assertIsInstance(csv_data, list)
        self.assertEqual(len(csv_data), 4)
        for row in csv_data:
            self.assertIn("user_email", row)
            self.assertIn("capsule_url", row)
        debug_logs = [
            log for log in captured.output
            if "Read" in log and "rows" in log
        ]
        self.assertEqual(len(debug_logs), 1)

    @patch("boto3.client")
    def test_read_csv_file_s3(self, mock_boto3_client):
        """Tests read_csv_file method with S3 file."""
        csv_content = (
            "user_email,capsule_url\n"
            "user1@example.com,https://codeocean.com/capsule/12345\n"
            "user2@example.com,https://codeocean.com/capsule/23456"
        )
        mock_s3_client = MagicMock()
        mock_response = {"Body": MagicMock()}
        mock_response["Body"].read.return_value = csv_content.encode("utf-8")
        mock_s3_client.get_object.return_value = mock_response
        mock_boto3_client.return_value = mock_s3_client

        s3_job_settings = JobSettings(
            csv_file="s3://test-bucket/data.csv",
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        s3_job = WebhookNotificationJob(job_settings=s3_job_settings)

        csv_data = s3_job.read_csv_file()
        self.assertEqual(len(csv_data), 2)
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="data.csv"
        )
        mock_s3_client.close.assert_called_once()

    def test_filter_csv_data(self):
        """Tests filter_csv_data method."""
        csv_data = self.example_job.read_csv_file()
        exclude_items = {"user2@example.com"}
        with self.assertLogs(level="INFO") as captured:
            filtered_data = self.example_job.filter_csv_data(
                csv_data, exclude_items
            )
        self.assertEqual(len(filtered_data), 3)
        filtered_users = [row["user_email"] for row in filtered_data]
        self.assertNotIn("user2@example.com", filtered_users)
        info_logs = [log for log in captured.output if "Excluding row" in log]
        self.assertEqual(len(info_logs), 1)

    def test_group_by_user(self):
        """Tests group_by_user method."""
        filtered_data = [
            {"user_email": "user1@example.com", "capsule_url": "url1"},
            {"user_email": "user1@example.com", "capsule_url": "url2"},
            {"user_email": "user3@example.com", "capsule_url": "url3"},
        ]
        with self.assertLogs(level="DEBUG") as captured:
            user_data = self.example_job.group_by_user(filtered_data)
        self.assertIn("user1@example.com", user_data)
        self.assertIn("user3@example.com", user_data)
        self.assertEqual(len(user_data["user1@example.com"]), 2)
        self.assertEqual(len(user_data["user3@example.com"]), 1)
        debug_logs = [
            log for log in captured.output if "Grouped data" in log
        ]
        self.assertEqual(len(debug_logs), 1)

    def test_exclude_list_integration(self):
        """Tests exclusion by both user email and capsule URL."""
        exclude_items = self.example_job.read_exclude_list()
        csv_data = self.example_job.read_csv_file()
        filtered_data = self.example_job.filter_csv_data(
            csv_data, exclude_items
        )
        user_data = self.example_job.group_by_user(filtered_data)

        self.assertNotIn("user2@example.com", user_data)
        self.assertIn("user1@example.com", user_data)
        self.assertIn("user3@example.com", user_data)

        self.assertEqual(len(user_data["user1@example.com"]), 1)
        self.assertEqual(
            user_data["user1@example.com"][0]["capsule_url"],
            "https://codeocean.com/capsule/34567",
        )

        self.assertEqual(len(user_data["user3@example.com"]), 1)
        self.assertEqual(
            user_data["user3@example.com"][0]["capsule_url"],
            "https://codeocean.com/capsule/45678",
        )

    @patch("requests.post")
    def test_send_webhook_notifications_success(self, mock_post: MagicMock):
        """Tests successful webhook notifications."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        test_data = {
            "user1@example.com": [{"capsule_url": "https://example.com/1"}],
            "user2@example.com": [{"capsule_url": "https://example.com/2"}]
        }
        with self.assertLogs(level="INFO") as captured:
            self.example_job.send_webhook_notifications(test_data)
        self.assertEqual(mock_post.call_count, 2)
        success_logs = [
            log for log in captured.output if "Successfully" in log
        ]
        self.assertEqual(len(success_logs), 2)

    @patch("requests.post")
    def test_send_webhook_notifications_failure(self, mock_post: MagicMock):
        """Tests webhook notification failures."""
        mock_post.side_effect = requests.exceptions.RequestException("Error")

        test_data = {
            "user1@example.com": [{"capsule_url": "https://example.com/1"}]
        }
        with self.assertLogs(level="ERROR") as captured:
            with self.assertRaises(requests.exceptions.RequestException):
                self.example_job.send_webhook_notifications(test_data)

        error_logs = [
            log for log in captured.output if "Failed to send" in log
        ]
        self.assertEqual(len(error_logs), 1)

    @patch("requests.post")
    def test_run_job_integration(self, mock_post: MagicMock):
        """Tests the complete run_job workflow."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with self.assertLogs(level="INFO") as captured:
            self.example_job.run_job()
        start_log = any("Starting webhook" in log for log in captured.output)
        end_log = any("completed" in log for log in captured.output)
        self.assertTrue(start_log)
        self.assertTrue(end_log)
        self.assertEqual(mock_post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
