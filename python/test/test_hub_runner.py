#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
from unittest.mock import patch

from click.testing import CliRunner

from ai.chronon.repo.hub_runner import hub

class TestHubRunner:
    """Test cases for hub_runner backfill command."""

    def _run_and_print(self, runner, command, args):
        """Helper method to run command and print output."""
        result = runner.invoke(command, args)

        # Print stdout
        if result.output:
            print(f"\n=== STDOUT ===\n{result.output}")

        # Print stderr if separate
        if hasattr(result, 'stderr') and result.stderr:
            print(f"\n=== STDERR ===\n{result.stderr}")

        # Print exception if any
        if result.exception:
            print(f"\n=== EXCEPTION ===\n{result.exception}")
            import traceback
            traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__)

        return result

    def test_hub_runner(self):
        """Test that hub command group can be invoked."""
        runner = CliRunner()
        result = self._run_and_print(runner, hub, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            '--repo', canary,
            '--conf', online_join_conf,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "backfill"
        assert json_payload['start'] == "2024-01-15"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_adhoc_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            '--repo', canary,
            '--conf', online_join_conf,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        # start-ds is not supported
        assert result.exit_code != 0
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            '--repo', canary,
            '--conf', online_join_conf,
            '--no-use-auth',
            '--end-ds', '2024-02-15',
        ])
        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "deploy"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_schedule_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner(catch_exceptions=False)
        result = self._run_and_print(runner, hub, [
            'schedule',
            '--repo', canary,
            '--conf', online_join_conf,
            '--no-use-auth',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/schedule/v2/schedules")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['branch'] == "test-branch"
        assert json_payload['modeSchedules'] == {"BACKFILL": "@daily", "DEPLOY": "@daily"}

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"
