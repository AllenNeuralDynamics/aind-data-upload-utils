"""Test module for classes and methods in check_directories_job"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_upload_utils.create_s5_commands_job import (
    CreateS5CommandsJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
SMART_SPIM_DIR = (
    RESOURCES_DIR
    / "example_smartspim_data_set"
    / "SmartSPIM_695464_2023-10-18_20-30-30"
)


class TestJobSettings(unittest.TestCase):
    """
    Tests for JobSettings class
    """

    def test_class_constructor(self):
        """Tests that job settings can be constructed from serialized json."""
        job_settings = JobSettings(
            s3_location="s3://some_bucket/some_prefix",
            staging_directory="stage",
        )
        deserialized_settings = job_settings.model_validate_json(
            job_settings.model_dump_json()
        )
        self.assertEqual(job_settings, deserialized_settings)
        self.assertEqual(
            Path("stage") / "s5_commands.txt", job_settings.s5_commands_file
        )


class TestCheckDirectoriesJob(unittest.TestCase):
    """Tests CheckDirectoriesJob class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up class with example settings"""
        cls.example_job = CreateS5CommandsJob(
            job_settings=JobSettings(
                s3_location="s3://some_bucket/some_prefix",
                staging_directory=SMART_SPIM_DIR,
                num_of_dir_levels_to_partition=2,
            )
        )
        cls.expected_commands = [
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/instrument.json" '
                f'"s3://some_bucket/some_prefix/instrument.json"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/nohup.out" '
                f'"s3://some_bucket/some_prefix/SmartSPIM/nohup.out"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/ASI_logging.txt"'
                f' "s3://some_bucket/some_prefix/derivatives/ASI_logging.txt"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'DarkMaster_cropped.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/DarkMaster_cropped.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'FinalReport.txt" "s3://some_bucket/some_prefix/'
                f'derivatives/FinalReport.txt"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_488_Ch0_0.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_488_Ch0_0.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_488_Ch0_1.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_488_Ch0_1.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_561_Ch1_0.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_561_Ch1_0.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_561_Ch1_1.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_561_Ch1_1.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_639_Ch2_0.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_639_Ch2_0.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Flat_639_Ch2_1.tif" "s3://some_bucket/some_prefix/'
                f'derivatives/Flat_639_Ch2_1.tif"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'TileSettings.ini" "s3://some_bucket/some_prefix/'
                f'derivatives/TileSettings.ini"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/metadata.json" '
                f'"s3://some_bucket/some_prefix/derivatives/metadata.json"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/metadata.txt" '
                f'"s3://some_bucket/some_prefix/derivatives/metadata.txt"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'processing_manifest.json" "s3://some_bucket/some_prefix/'
                f'derivatives/processing_manifest.json"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/'
                f'Ex_488_Em_525/471320/*" "s3://some_bucket/some_prefix/'
                f'SmartSPIM/Ex_488_Em_525/471320/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/'
                f'Ex_488_Em_525/503720/*" "s3://some_bucket/some_prefix/'
                f'SmartSPIM/Ex_488_Em_525/503720/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/'
                f'Ex_488_Em_525/536120/*" "s3://some_bucket/some_prefix/'
                f'SmartSPIM/Ex_488_Em_525/536120/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_488_Em_525/'
                f'568520/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_488_Em_525/568520/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_488_Em_525/'
                f'some_file_here.txt" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_488_Em_525/some_file_here.txt"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600/'
                f'471320/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_561_Em_600/471320/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600/'
                f'503720/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_561_Em_600/503720/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600/'
                f'536120/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_561_Em_600/536120/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_561_Em_600/'
                f'568520/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_561_Em_600/568520/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680/'
                f'471320/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_639_Em_680/471320/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680/'
                f'503720/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_639_Em_680/503720/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680/'
                f'536120/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_639_Em_680/536120/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/SmartSPIM/Ex_639_Em_680/'
                f'568520/*" "s3://some_bucket/some_prefix/SmartSPIM/'
                f'Ex_639_Em_680/568520/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_488_Em_525_MIP/471320/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_488_Em_525_MIP/471320/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_488_Em_525_MIP/503720/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_488_Em_525_MIP/503720/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_488_Em_525_MIP/536120/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_488_Em_525_MIP/536120/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_488_Em_525_MIP/568520/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_488_Em_525_MIP/568520/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_561_Em_600_MIP/471320/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_561_Em_600_MIP/471320/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_561_Em_600_MIP/503720/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_561_Em_600_MIP/503720/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_561_Em_600_MIP/536120/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_561_Em_600_MIP/536120/"'
            ),
            (
                f'cp "{SMART_SPIM_DIR.as_posix()}/derivatives/'
                f'Ex_561_Em_600_MIP/568520/*" "s3://some_bucket/some_prefix/'
                f'derivatives/Ex_561_Em_600_MIP/568520/"'
            ),
        ]

    def test_map_file_path_to_s3_location(self):
        """Tests _map_file_path_to_s3_location"""
        file_path_1 = (
            self.example_job.job_settings.staging_directory / "hello.txt"
        )
        file_path_2 = (
            self.example_job.job_settings.staging_directory
            / "abc"
            / "hello.txt"
        )
        dir_path_1 = (
            self.example_job.job_settings.staging_directory / "abc" / "def"
        )
        s3_location_1 = self.example_job._map_file_path_to_s3_location(
            file_path_1.as_posix()
        )
        s3_location_2 = self.example_job._map_file_path_to_s3_location(
            file_path_2.as_posix()
        )
        s3_location_3 = self.example_job._map_file_path_to_s3_location(
            dir_path_1.as_posix()
        )
        self.assertEqual(
            "s3://some_bucket/some_prefix/hello.txt", s3_location_1
        )
        self.assertEqual(
            "s3://some_bucket/some_prefix/abc/hello.txt", s3_location_2
        )
        self.assertEqual("s3://some_bucket/some_prefix/abc/def", s3_location_3)

    def test_create_file_cp_command(self):
        """Tests _create_file_cp_command"""
        file_path_1 = (
            self.example_job.job_settings.staging_directory / "hello.txt"
        )
        file_path_2 = (
            self.example_job.job_settings.staging_directory
            / "abc"
            / "hello.txt"
        )
        command_1 = self.example_job._create_file_cp_command(
            file_path_1.as_posix()
        )
        command_2 = self.example_job._create_file_cp_command(
            file_path_2.as_posix()
        )
        expected_command_1 = (
            f'cp "{file_path_1.as_posix()}" '
            f'"s3://some_bucket/some_prefix/hello.txt"'
        )
        expected_command_2 = (
            f'cp "{file_path_2.as_posix()}" '
            f'"s3://some_bucket/some_prefix/abc/hello.txt"'
        )
        self.assertEqual(expected_command_1, command_1)
        self.assertEqual(expected_command_2, command_2)

    def test_create_directory_cp_command(self):
        """Tests _create_directory_cp_command"""
        dir_path_1 = self.example_job.job_settings.staging_directory / "abc"
        dir_path_2 = (
            self.example_job.job_settings.staging_directory / "abc" / "def"
        )
        command_1 = self.example_job._create_directory_cp_command(
            dir_path_1.as_posix()
        )
        command_2 = self.example_job._create_directory_cp_command(
            dir_path_2.as_posix()
        )

        expected_command_1 = (
            f'cp "{dir_path_1.as_posix()}/*" '
            f'"s3://some_bucket/some_prefix/abc/"'
        )
        expected_command_2 = (
            f'cp "{dir_path_2.as_posix()}/*" '
            f'"s3://some_bucket/some_prefix/abc/def/"'
        )
        self.assertEqual(expected_command_1, command_1)
        self.assertEqual(expected_command_2, command_2)

    def test_get_list_of_upload_commands(self):
        """Tests _get_list_of_upload_commands"""
        list_of_commands = self.example_job._get_list_of_upload_commands()
        self.assertCountEqual(self.expected_commands, list_of_commands)

    @patch("os.path.isfile")
    @patch("os.path.isdir")
    def test_get_list_of_upload_commands_error(
        self, mock_is_dir: MagicMock, mock_is_file: MagicMock
    ):
        """Tests _get_list_of_upload_commands edge case where path is
        corrupt"""
        mock_is_file.return_value = False
        mock_is_dir.return_value = False
        with self.assertRaises(NotADirectoryError):
            self.example_job._get_list_of_upload_commands()

    @patch("builtins.open", new_callable=mock_open())
    def test_save_s5_commands_to_file(self, mock_write: MagicMock):
        """Tests _save_s5_commands_to_file"""

        self.example_job._save_s5_commands_to_file(self.expected_commands)
        self.assertEqual(39, len(mock_write.mock_calls))

    @patch("logging.debug")
    @patch("builtins.open", new_callable=mock_open())
    def test_run_job(self, mock_write: MagicMock, mock_log_debug: MagicMock):
        """Tests run_job"""

        self.example_job.run_job()
        self.assertEqual(39, len(mock_write.mock_calls))
        mock_log_debug.assert_called()


if __name__ == "__main__":
    unittest.main()
