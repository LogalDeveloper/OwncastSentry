# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for bot command handlers."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
import time_machine
from aioresponses import aioresponses

from owncastsentry.commands import CommandHandler
from owncastsentry.models import StreamState
from owncastsentry.utils import OWNCAST_STATUS_PATH, UNKNOWN_STATUS_THRESHOLD
from tests.conftest import VALID_STATUS_RESPONSE


def _make_command_handler() -> CommandHandler:
    """Build a CommandHandler with dummy dependencies for pure logic tests."""
    return CommandHandler(
        owncast_client=MagicMock(),
        stream_repo=MagicMock(),
        subscription_repo=MagicMock(),
        logger=logging.getLogger("test"),
    )


class TestFormatDuration:
    """Elapsed time calculation from ISO timestamps."""

    _NOW = datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        ("seconds_ago", "expected"),
        [
            pytest.param(1, "1 second", id="singular-second"),
            pytest.param(30, "30 seconds", id="plural-seconds"),
            pytest.param(60, "1 minute", id="singular-minute"),
            pytest.param(120, "2 minutes", id="plural-minutes"),
            pytest.param(3600, "1 hour", id="singular-hour"),
            pytest.param(7200, "2 hours", id="plural-hours"),
            pytest.param(86400, "1 day", id="singular-day"),
            pytest.param(172800, "2 days", id="plural-days"),
        ],
    )
    @time_machine.travel(_NOW)
    def test_formats_duration(self, seconds_ago: int, expected: str) -> None:
        """Format a timestamp into a human-readable duration."""
        handler = _make_command_handler()
        timestamp = (self._NOW - timedelta(seconds=seconds_ago)).isoformat()
        result = handler._format_duration(timestamp)
        assert result == expected

    def test_invalid_timestamp(self) -> None:
        """Return 'unknown duration' for unparsable timestamps."""
        handler = _make_command_handler()
        assert handler._format_duration("not-a-timestamp") == "unknown duration"


class TestSubscribeCommand:
    """Subscribe command end-to-end via maubot."""

    async def test_subscribe_valid_stream(self, maubot_test_bot, maubot_plugin) -> None:
        """Subscribe to a valid Owncast stream."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "Subscription added! This room will receive notifications when "
            "stream.logal.dev goes live."
        )

    async def test_subscribe_invalid_stream(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """Reject subscription to an invalid Owncast instance."""
        status_url = f"https://invalid.com{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(status_url, status=404)
            await maubot_test_bot.send("!subscribe invalid.com")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "The URL you supplied does not appear to "
            "be a valid Owncast instance. You may have "
            "specified an invalid domain, or the "
            "instance is offline."
        )

    async def test_subscribe_already_subscribed(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """Reject duplicate subscription in the same room."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Second subscribe; stream already exists so validation is skipped
        await maubot_test_bot.send("!subscribe stream.logal.dev")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "This room is already subscribed to notifications for stream.logal.dev."
        )

    async def test_subscribe_existing_stream_new_room(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """Skip instance validation when subscribing from a new room."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Subscribe from a different room; skips validation and should not
        # query the remote instance. An empty aioresponses context will raise
        # ConnectionError if any HTTP request is attempted.
        with aioresponses():
            await maubot_test_bot.send(
                "!subscribe stream.logal.dev", room_id="!otherroom:example.com"
            )

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "Subscription added! This room will receive notifications when "
            "stream.logal.dev goes live."
        )


class TestUnsubscribeCommand:
    """Unsubscribe command end-to-end via maubot."""

    async def test_unsubscribe_existing(self, maubot_test_bot, maubot_plugin) -> None:
        """Unsubscribe from a subscribed stream."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        await maubot_test_bot.send("!unsubscribe stream.logal.dev")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "Subscription removed! This room will no "
            "longer receive notifications for stream.logal.dev."
        )

    async def test_unsubscribe_not_subscribed(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """Reject unsubscribe from a non-subscribed stream."""
        await maubot_test_bot.send("!unsubscribe unknown.com")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is already not subscribed to notifications for unknown.com."
        )


class TestSubscriptionsCommand:
    """Subscriptions listing command end-to-end via maubot."""

    async def test_no_subscriptions(self, maubot_test_bot, maubot_plugin) -> None:
        """Show help text when no subscriptions exist."""
        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is not subscribed to any Owncast "
            "instances.\n\nTo subscribe to an Owncast "
            "instance, use `!subscribe <domain>`"
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_online_stream(self, maubot_test_bot, maubot_plugin) -> None:
        """Show stream details including title and duration."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Update stream state to be online with a name
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="stream.logal.dev",
                name="Test Stream",
                title="Playing Games",
                last_connect_time="2026-01-01T12:00:00Z",
            )
        )

        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "**Subscriptions for this room (1):**\n\n"
            "● **Test Stream** \n"
            "  ○ Title: Playing Games\n"
            "  ○ Status: Online for 71 days\n"
            "  ○ Link: https://stream.logal.dev\n"
            "To unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_offline_stream(self, maubot_test_bot, maubot_plugin) -> None:
        """Show offline status for non-live streams."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Update stream state to be offline
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="stream.logal.dev",
                name="Test Stream",
                last_disconnect_time="2026-01-01T10:00:00Z",
            )
        )

        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "**Subscriptions for this room (1):**\n\n"
            "● **Test Stream** \n"
            "  ○ Status: Offline for 71 days\n"
            "  ○ Link: https://stream.logal.dev\n"
            "To unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )

    async def test_shows_offline_stream_without_disconnect_time(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """Show offline status without duration before first poll completes."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Stream row exists with no state yet - query subscriptions immediately
        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "**Subscriptions for this room (1):**\n\n"
            "● **stream.logal.dev** \n"
            "  ○ Status: Offline\n"
            "  ○ Link: https://stream.logal.dev\n"
            "To unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )

    async def test_shows_unknown_stream(self, maubot_test_bot, maubot_plugin) -> None:
        """Show unknown status when instance has been unreachable."""
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Increment failure counter past the unknown threshold
        for _ in range(UNKNOWN_STATUS_THRESHOLD + 1):
            await maubot_plugin.stream_repo.increment_failure_counter(
                "stream.logal.dev"
            )

        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "**Subscriptions for this room (1):**\n\n"
            "● **stream.logal.dev** \n"
            "  ○ Status: Unknown (instance unreachable)\n"
            "  ○ Link: https://stream.logal.dev\n"
            "To unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_multiple_subscriptions(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """List subscriptions alphabetically with mixed statuses."""
        # Subscribe in reverse alphabetical order to verify sorted output
        with aioresponses() as mocked:
            mocked.get(
                f"https://beta.com{OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            mocked.get(
                f"https://alpha.com{OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe beta.com")
            await maubot_test_bot.send("!subscribe alpha.com")

        # Set alpha online, beta offline
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="alpha.com",
                name="Alpha Stream",
                title="Streaming Live",
                last_connect_time="2026-03-13T10:00:00Z",
            )
        )
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="beta.com",
                name="Beta Stream",
                last_disconnect_time="2026-03-12T18:00:00Z",
            )
        )

        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 3
        assert maubot_test_bot.responded[2].content.body == (
            "**Subscriptions for this room (2):**\n\n"
            "● **Alpha Stream**\n"
            "  \n"
            "  ○ Title: Streaming Live\n"
            "  ○ Status: Online for 2 hours\n"
            "  ○ Link: https://alpha.com\n"
            "● **Beta Stream**\n"
            "  \n"
            "  ○ Status: Offline for 18 hours\n"
            "  ○ Link: https://beta.com\n"
            "To unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )


class TestLiveCommand:
    """Live streams listing command end-to-end via maubot."""

    async def test_no_subscriptions(self, maubot_test_bot, maubot_plugin) -> None:
        """Show help text when no subscriptions exist."""
        await maubot_test_bot.send("!live")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is not subscribed to any Owncast "
            "instances.\n\nTo subscribe to an Owncast "
            "instance, use `!subscribe <domain>`"
        )

    async def test_no_live_streams(self, maubot_test_bot, maubot_plugin) -> None:
        """Show 'no live' message when all streams are offline."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Update stream state to offline
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="stream.logal.dev",
                name="Test Stream",
                last_disconnect_time="2026-01-01T10:00:00Z",
            )
        )

        await maubot_test_bot.send("!live")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "No subscribed Owncast instances are currently "
            "live.\n\nUse `!subscriptions` to list all "
            "subscriptions."
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_live_stream(self, maubot_test_bot, maubot_plugin) -> None:
        """Show live stream with title and duration."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Update stream state to online
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="stream.logal.dev",
                name="Test Stream",
                title="Playing Games",
                last_connect_time="2026-01-01T12:00:00Z",
            )
        )

        await maubot_test_bot.send("!live")

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "**Live Owncast instances (1):**\n\n"
            "● **Test Stream** \n"
            "  ○ Title: Playing Games\n"
            "  ○ Online for 71 days\n"
            "  ○ Link: https://stream.logal.dev"
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_multiple_live_streams(
        self, maubot_test_bot, maubot_plugin
    ) -> None:
        """List live streams alphabetically with different durations."""
        # Subscribe in reverse alphabetical order to verify sorted output
        with aioresponses() as mocked:
            mocked.get(
                f"https://beta.com{OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            mocked.get(
                f"https://alpha.com{OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe beta.com")
            await maubot_test_bot.send("!subscribe alpha.com")

        # Set both streams online with different connect times
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="alpha.com",
                name="Alpha Stream",
                title="Morning Show",
                last_connect_time="2026-03-13T10:00:00Z",
            )
        )
        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="beta.com",
                name="Beta Stream",
                title="Evening Vibes",
                last_connect_time="2026-03-13T06:00:00Z",
            )
        )

        await maubot_test_bot.send("!live")

        assert len(maubot_test_bot.responded) == 3
        assert maubot_test_bot.responded[2].content.body == (
            "**Live Owncast instances (2):**\n\n"
            "● **Alpha Stream**\n"
            "  \n"
            "  ○ Title: Morning Show\n"
            "  ○ Online for 2 hours\n"
            "  ○ Link: https://alpha.com\n"
            "● **Beta Stream**\n"
            "  \n"
            "  ○ Title: Evening Vibes\n"
            "  ○ Online for 6 hours\n"
            "  ○ Link: https://beta.com"
        )
