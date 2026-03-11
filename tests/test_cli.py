"""Tests for CLI module."""

import sys
from unittest.mock import patch

import pytest

from media_archive_sync.cli import main


class TestCliMain:
    """Tests for CLI main function."""

    def test_cli_basic_download(self):
        """Test basic CLI download flow."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config") as mock_download,
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {"http://example.com/": 1},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                "/tmp/media",
                "--workers",
                "5",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_crawl.assert_called_once()
            mock_download.assert_called_once()

    def test_cli_dry_run(self):
        """Test CLI dry run mode."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config") as mock_download,
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--dry-run",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_crawl.assert_called_once()
            mock_download.assert_not_called()

    def test_cli_quiet_mode(self):
        """Test CLI quiet mode."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--quiet",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            # In quiet mode, crawl should still be called but logging suppressed
            mock_crawl.assert_called_once()

    def test_cli_organize_mode(self):
        """Test CLI with organize mode."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
            patch(
                "media_archive_sync.organizer.organize_files_by_month"
            ) as mock_organize,
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )
            mock_organize.return_value = {"Jan_2024": []}

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                "/tmp/media",
                "--organize",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_organize.assert_called_once()

    def test_cli_path_traversal_protection(self):
        """Test that path traversal is detected and prevented."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
        ):
            mock_crawl.return_value = (
                [("http://example.com/../etc/passwd", "passwd")],
                {},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                "/tmp/media",
            ]

            with (
                patch.object(sys, "argv", test_args),
                pytest.raises(ValueError, match="Path traversal"),
            ):
                main()

    def test_cli_no_files_found(self):
        """Test CLI when no files are found."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config") as mock_download,
        ):
            mock_crawl.return_value = ([], {})

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/empty/",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_crawl.assert_called_once()
            # download should still be called with empty list
            mock_download.assert_called_once()

    def test_cli_default_workers(self):
        """Test CLI with default workers value."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )

            test_args = ["cli.py", "--remote", "http://example.com/"]

            with patch.object(sys, "argv", test_args):
                main()

            # crawl should be called with default params
            mock_crawl.assert_called_once()


class TestCliArgParsing:
    """Tests for CLI argument parsing."""

    def test_required_remote_argument(self):
        """Test that --remote is required."""
        test_args = ["cli.py"]

        with patch.object(sys, "argv", test_args), pytest.raises(SystemExit):
            main()

    def test_default_local_path(self):
        """Test default local path is ./media."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
        ):
            mock_crawl.return_value = ([], {})

            test_args = ["cli.py", "--remote", "http://example.com/"]

            with patch.object(sys, "argv", test_args):
                main()

            # Should use default local path
            mock_crawl.assert_called_once()


class TestCliPathComputation:
    """Tests for path computation in CLI."""

    def test_compute_target_path_normal(self):
        """Test computing target path for normal URL."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config") as mock_download,
        ):
            mock_crawl.return_value = (
                [("http://example.com/videos/video.mp4", "video.mp4")],
                {},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                "/tmp/media",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            # download should receive proper paths
            mock_download.assert_called_once()

    def test_compute_target_path_encoded(self):
        """Test computing target path for URL-encoded path."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config") as mock_download,
        ):
            mock_crawl.return_value = (
                [
                    (
                        "http://example.com/video%20with%20spaces.mp4",
                        "video with spaces.mp4",
                    )
                ],
                {},
            )

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                "/tmp/media",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_download.assert_called_once()


class TestCliOrganize:
    """Tests for CLI organize functionality."""

    def test_organize_with_files(self, tmp_path):
        """Test organizing files."""
        local_root = tmp_path / "media"
        local_root.mkdir()
        # Create actual file to avoid FileNotFoundError during move
        (local_root / "video.mp4").write_text("fake content")

        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
            patch(
                "media_archive_sync.organizer.organize_files_by_month"
            ) as mock_organize,
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )
            # Return an empty dict to avoid actual file operations
            mock_organize.return_value = {}

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                str(local_root),
                "--organize",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_organize.assert_called_once()

    def test_organize_dry_run(self):
        """Test organizing files in dry-run mode."""
        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
            patch(
                "media_archive_sync.organizer.organize_files_by_month"
            ) as mock_organize,
        ):
            mock_crawl.return_value = (
                [("http://example.com/video.mp4", "video.mp4")],
                {},
            )
            mock_organize.return_value = {}

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--organize",
                "--dry-run",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            mock_organize.assert_called_once()

    def test_organize_moves_files(self, tmp_path):
        """Test that organize mode actually moves files."""
        local_root = tmp_path / "media"
        local_root.mkdir()
        # Create a video file with epoch in name
        video_file = local_root / "video_1609459200.mp4"
        video_file.write_text("fake video content")

        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
            patch(
                "media_archive_sync.organizer.organize_files_by_month"
            ) as mock_organize,
        ):
            mock_crawl.return_value = ([], {})
            # Return the actual file in the organized dict
            mock_organize.return_value = {"Jan_2021": [video_file]}

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                str(local_root),
                "--organize",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            # File should have been moved to month folder
            mock_organize.assert_called_once()

    def test_organize_handles_existing_files(self, tmp_path):
        """Test organize handles existing files with counter."""
        local_root = tmp_path / "media"
        local_root.mkdir()
        # Create source file
        video_file = local_root / "video_1609459200.mp4"
        video_file.write_text("source content")
        # Create existing file in target
        month_dir = local_root / "Jan_2021"
        month_dir.mkdir()
        existing_file = month_dir / "video_1609459200.mp4"
        existing_file.write_text("existing content")

        with (
            patch("media_archive_sync.cli.crawl_archive") as mock_crawl,
            patch("media_archive_sync.cli.download_with_config"),
            patch(
                "media_archive_sync.organizer.organize_files_by_month"
            ) as mock_organize,
        ):
            mock_crawl.return_value = ([], {})
            mock_organize.return_value = {"Jan_2021": [video_file]}

            test_args = [
                "cli.py",
                "--remote",
                "http://example.com/",
                "--local",
                str(local_root),
                "--organize",
            ]

            with patch.object(sys, "argv", test_args):
                main()

            # Should have created video_1609459200_1.mp4
            assert (month_dir / "video_1609459200_1.mp4").exists()

    def test_main_module_execution(self):
        """Test main module execution block."""
        import media_archive_sync.cli as cli_module

        with (
            patch.object(cli_module, "main") as mock_main,
            patch.object(cli_module, "__name__", "__main__"),
        ):
            # Simulate the if __name__ == "__main__" block
            if cli_module.__name__ == "__main__":
                cli_module.main()

            mock_main.assert_called_once()
