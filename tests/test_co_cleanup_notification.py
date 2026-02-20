"""Tests co_cleanup_notification module"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_upload_utils.co_cleanup_notification import (
    WebhookNotificationJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
CSV_FILE = RESOURCES_DIR / "example_capsules.csv"
EXCLUDE_FILE = RESOURCES_DIR / "exclude_list.txt"


class TestWebhookNotificationJob(unittest.TestCase):
    """Test class for WebhookNotificationJob."""

    def test_job_settings_properties(self):
        """Tests JobSettings properties."""
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        self.assertEqual(job_settings.csv_file, CSV_FILE)
        self.assertEqual(job_settings.exclude_list_file, EXCLUDE_FILE)
        self.assertEqual(job_settings.webhook_url, "https://webhook.site/test")

    def test_parse_csv_with_excludes(self):
        """Tests parse_csv method with exclude list."""
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        job = WebhookNotificationJob(job_settings=job_settings)
        
        with self.assertLogs(level="DEBUG") as captured:
            result = job.parse_csv()
        
        # Test the structure of the returned data
        self.assertIsInstance(result, dict)
        
        # With user2@example.com excluded, we should have user1 and user3
        expected_users = {"user1@example.com", "user3@example.com"}
        self.assertEqual(set(result.keys()), expected_users)
        
        # user1@example.com should have 2 capsules (rows 1 and 3)
        self.assertEqual(len(result["user1@example.com"]), 2)
        
        # user3@example.com should have 1 capsule (row 4)
        self.assertEqual(len(result["user3@example.com"]), 1)
        
        # Check capsule data structure
        for user_email, capsules in result.items():
            self.assertIsInstance(capsules, list)
            self.assertGreater(len(capsules), 0)  # Each user should have at least one capsule
            for capsule in capsules:
                self.assertIn("capsule_url", capsule)
                self.assertIsInstance(capsule["capsule_url"], str)
                self.assertTrue(capsule["capsule_url"].startswith("http"))  # Should be a valid URL

        # Verify that debug log contains metadata about files processed
        debug_logs = [log for log in captured.output if "Exclude items" in log]
        self.assertEqual(len(debug_logs), 1)

    def test_parse_csv_exclude_by_capsule_url(self):
        """Tests parse_csv method excluding by capsule URL."""
        # Create a temporary exclude file with a capsule URL
        exclude_capsule_file = RESOURCES_DIR / "exclude_capsule.txt"
        with open(exclude_capsule_file, 'w') as f:
            f.write("https://codeocean.com/capsule/12345")
        
        try:
            job_settings = JobSettings(
                csv_file=CSV_FILE,
                exclude_list_file=exclude_capsule_file,
                webhook_url="https://webhook.site/test"
            )
            job = WebhookNotificationJob(job_settings=job_settings)
            
            result = job.parse_csv()
            
            # Should exclude the first row with capsule 12345, leaving user1 with 1 capsule and user2, user3
            self.assertIn("user1@example.com", result)
            self.assertIn("user2@example.com", result)
            self.assertIn("user3@example.com", result)
            
            # user1 should have only 1 capsule left (the second one)
            self.assertEqual(len(result["user1@example.com"]), 1)
            self.assertEqual(result["user1@example.com"][0]["capsule_url"], "https://codeocean.com/capsule/34567")
        
        finally:
            # Clean up
            if exclude_capsule_file.exists():
                exclude_capsule_file.unlink()

    def test_parse_csv_without_excludes(self):
        """Tests parse_csv method when exclude file doesn't exist."""
        non_existent_exclude = RESOURCES_DIR / "non_existent.txt"
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=non_existent_exclude,
            webhook_url="https://webhook.site/test"
        )
        job = WebhookNotificationJob(job_settings=job_settings)
        
        result = job.parse_csv()
        
        # Should include all users when no exclude file exists
        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_success(self, mock_post: MagicMock):
        """Tests successful webhook notifications."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        job = WebhookNotificationJob(job_settings=job_settings)
        
        test_data = {
            "user1@example.com": [
                {"capsule_url": "https://example.com/capsule1"},
                {"capsule_url": "https://example.com/capsule2"}
            ],
            "user2@example.com": [
                {"capsule_url": "https://example.com/capsule3"}
            ]
        }
        
        with self.assertLogs(level="INFO") as captured:
            job.send_webhook_notifications(test_data)
        
        # Check that requests.post was called for each user
        self.assertEqual(mock_post.call_count, 2)
        
        # Check log messages for successful notifications
        success_logs = [log for log in captured.output if "Successfully sent notification" in log]
        self.assertEqual(len(success_logs), 2)

    @patch('requests.post')
    def test_send_webhook_notifications_failure(self, mock_post: MagicMock):
        """Tests webhook notification failures."""
        # Mock failed response with the correct exception type
        import requests
        mock_post.side_effect = requests.exceptions.RequestException("Network error")
        
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        job = WebhookNotificationJob(job_settings=job_settings)
        
        test_data = {
            "user1@example.com": [
                {"capsule_url": "https://example.com/capsule1"}
            ]
        }
        
        with self.assertLogs(level="ERROR") as captured:
            job.send_webhook_notifications(test_data)
        
        # Check that error was logged
        error_logs = [log for log in captured.output if "Failed to send notification" in log]
        self.assertEqual(len(error_logs), 1)

    @patch('requests.post')
    def test_run_job_integration(self, mock_post: MagicMock):
        """Tests the complete run_job workflow."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        job_settings = JobSettings(
            csv_file=CSV_FILE,
            exclude_list_file=EXCLUDE_FILE,
            webhook_url="https://webhook.site/test"
        )
        job = WebhookNotificationJob(job_settings=job_settings)
        
        with self.assertLogs(level="INFO") as captured:
            job.run_job()
        
        # Check that the job completed successfully
        start_log = any("Starting webhook notification job" in log for log in captured.output)
        end_log = any("Webhook notification job completed" in log for log in captured.output)
        self.assertTrue(start_log)
        self.assertTrue(end_log)
        
        # Verify webhook calls were made
        self.assertGreater(mock_post.call_count, 0)


if __name__ == "__main__":
    unittest.main()
