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

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
import time_machine
from aioresponses import aioresponses

from owncastsentry.commands import (
    _SECONDS_PER_DAY,
    _SECONDS_PER_HOUR,
    _SECONDS_PER_MINUTE,
    _escape_markdown,
    _format_duration,
    _sanitize_for_markdown,
)
from owncastsentry.owncast_client import _OWNCAST_STATUS_PATH
from owncastsentry.types import UNKNOWN_STATUS_THRESHOLD, StreamState
from tests.conftest import VALID_STATUS_RESPONSE

if TYPE_CHECKING:
    from maubot.testing.bot import TestBot

    from owncastsentry import OwncastSentry


class TestEscapeMarkdown:
    """Markdown special character escaping."""

    @pytest.mark.parametrize(
        ("input_text", "expected"),
        [
            pytest.param("hello", "hello", id="plain-text-unchanged"),
            pytest.param("*bold*", "\\*bold\\*", id="asterisks"),
            pytest.param("_italic_", "\\_italic\\_", id="underscores"),
            pytest.param("[link](url)", "\\[link\\]\\(url\\)", id="link-syntax"),
            pytest.param("`code`", "\\`code\\`", id="backticks"),
            pytest.param("# heading", "\\# heading", id="heading"),
            pytest.param("> quote", "\\> quote", id="blockquote"),
            pytest.param("<html>", "\\<html\\>", id="angle-brackets"),
            pytest.param("a & b", "a \\& b", id="ampersand"),
            pytest.param("a\\b", "a\\\\b", id="backslash"),
            pytest.param("", "", id="empty-string"),
        ],
    )
    def test_escapes_special_chars(self, input_text: str, expected: str) -> None:
        """Escape Markdown special characters in input text."""
        assert _escape_markdown(input_text) == expected


class TestSanitizeForMarkdown:
    """Markdown sanitization combining whitespace normalization and escaping."""

    def test_removes_newlines_and_escapes(self) -> None:
        """Normalize newlines and escape Markdown special characters."""
        result = _sanitize_for_markdown("*bold*\nnew line")
        assert result == "\\*bold\\* new line"

    def test_empty_string(self) -> None:
        """Return empty string unchanged."""
        assert _sanitize_for_markdown("") == ""


class TestFormatDuration:
    """Elapsed time calculation from ISO timestamps."""

    _NOW = datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        ("seconds_ago", "expected"),
        [
            pytest.param(1, "1 second", id="singular-second"),
            pytest.param(30, "30 seconds", id="plural-seconds"),
            pytest.param(_SECONDS_PER_MINUTE, "1 minute", id="singular-minute"),
            pytest.param(2 * _SECONDS_PER_MINUTE, "2 minutes", id="plural-minutes"),
            pytest.param(_SECONDS_PER_HOUR, "1 hour", id="singular-hour"),
            pytest.param(2 * _SECONDS_PER_HOUR, "2 hours", id="plural-hours"),
            pytest.param(_SECONDS_PER_DAY, "1 day", id="singular-day"),
            pytest.param(2 * _SECONDS_PER_DAY, "2 days", id="plural-days"),
        ],
    )
    def test_formats_duration(self, seconds_ago: int, expected: str) -> None:
        """Format a timestamp into a human-readable duration."""
        timestamp = (self._NOW - timedelta(seconds=seconds_ago)).isoformat()
        result = _format_duration(timestamp, self._NOW)
        assert result == expected

    def test_invalid_timestamp(self) -> None:
        """Return 'unknown duration' for unparsable timestamps."""
        assert _format_duration("not-a-timestamp", self._NOW) == "unknown duration"

    def test_naive_timestamp(self) -> None:
        """Return 'unknown duration' for timestamps without timezone information."""
        assert _format_duration("2026-03-13T11:59:00", self._NOW) == "unknown duration"

    def test_future_timestamp(self) -> None:
        """Return 'unknown duration' for timestamps in the future."""
        timestamp = (self._NOW + timedelta(seconds=1)).isoformat()
        assert _format_duration(timestamp, self._NOW) == "unknown duration"


@pytest.mark.usefixtures("maubot_plugin")
class TestSubscribeCommand:
    """Subscribe command end-to-end via maubot."""

    async def test_subscribe_valid_stream(self, maubot_test_bot: TestBot) -> None:
        """Subscribe to a valid Owncast stream."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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

    async def test_subscribe_invalid_stream(self, maubot_test_bot: TestBot) -> None:
        """Reject subscription to an invalid Owncast instance."""
        status_url = f"https://invalid.com{_OWNCAST_STATUS_PATH}"
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

    async def test_subscribe_already_subscribed(self, maubot_test_bot: TestBot) -> None:
        """Reject duplicate subscription in the same room."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
        self, maubot_test_bot: TestBot
    ) -> None:
        """Skip validation when the domain already has subscriptions."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        # Subscribe from a different room. The existing subscribed domain skips
        # validation, so an empty aioresponses context will raise ConnectionError
        # if any HTTP request is attempted.
        with aioresponses():
            await maubot_test_bot.send(
                "!subscribe stream.logal.dev", room_id="!otherroom:example.com"
            )

        assert len(maubot_test_bot.responded) == 2
        assert maubot_test_bot.responded[1].content.body == (
            "Subscription added! This room will receive notifications when "
            "stream.logal.dev goes live."
        )


@pytest.mark.usefixtures("maubot_plugin")
class TestUnsubscribeCommand:
    """Unsubscribe command end-to-end via maubot."""

    async def test_unsubscribe_existing(self, maubot_test_bot: TestBot) -> None:
        """Unsubscribe from a subscribed stream."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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

    async def test_unsubscribe_not_subscribed(self, maubot_test_bot: TestBot) -> None:
        """Reject unsubscribe from a non-subscribed stream."""
        await maubot_test_bot.send("!unsubscribe unknown.com")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is already not subscribed to notifications for unknown.com."
        )


@pytest.mark.usefixtures("maubot_plugin")
class TestSubscriptionsCommand:
    """Subscriptions listing command end-to-end via maubot."""

    async def test_no_subscriptions(self, maubot_test_bot: TestBot) -> None:
        """Show help text when no subscriptions exist."""
        await maubot_test_bot.send("!subscriptions")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is not subscribed to any Owncast "
            "instances.\n\nTo subscribe to an Owncast "
            "instance, use `!subscribe <domain>`"
        )

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_online_stream(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Show stream details including title and duration."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
    async def test_escapes_markdown_in_stream_name_and_title(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Render stream name and title as literal text in command output."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
        with aioresponses() as mocked:
            mocked.get(
                status_url,
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await maubot_test_bot.send("!subscribe stream.logal.dev")

        await maubot_plugin.stream_repo.update(
            StreamState(
                domain="stream.logal.dev",
                name="*Bold* [link](https://evil.example)\nName",
                title="`code` > quote #tag",
                last_connect_time="2026-01-01T12:00:00Z",
            )
        )

        await maubot_test_bot.send("!subscriptions")

        content = maubot_test_bot.responded[1].content
        assert "● ***Bold* [link](https://evil.example) Name**" in content.body
        assert "  ○ Title: `code` > quote #tag" in content.body
        assert content.formatted_body is not None
        assert '<a href="https://evil.example">' not in content.formatted_body
        assert (
            "<strong>*Bold* [link](https://evil.example) Name</strong>"
            in content.formatted_body
        )
        assert "Title: `code` &gt; quote #tag" in content.formatted_body

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_shows_offline_stream(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Show offline status for non-live streams."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
        self, maubot_test_bot: TestBot
    ) -> None:
        """Show offline status without duration before first poll completes."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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

    async def test_shows_unknown_stream(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Show unknown status when instance has been unreachable."""
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """List subscriptions ordered by domain with mixed statuses."""
        # Subscribe in reverse domain order to verify domain-sorted output
        with aioresponses() as mocked:
            mocked.get(
                f"https://beta.com{_OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            mocked.get(
                f"https://alpha.com{_OWNCAST_STATUS_PATH}",
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


@pytest.mark.usefixtures("maubot_plugin")
class TestLiveCommand:
    """Live streams listing command end-to-end via maubot."""

    async def test_no_subscriptions(self, maubot_test_bot: TestBot) -> None:
        """Show help text when no subscriptions exist."""
        await maubot_test_bot.send("!live")

        assert len(maubot_test_bot.responded) == 1
        assert maubot_test_bot.responded[0].content.body == (
            "This room is not subscribed to any Owncast "
            "instances.\n\nTo subscribe to an Owncast "
            "instance, use `!subscribe <domain>`"
        )

    async def test_no_live_streams(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Show 'no live' message when all streams are offline."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
    async def test_shows_live_stream(
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """Show live stream with title and duration."""
        # Subscribe first
        status_url = f"https://stream.logal.dev{_OWNCAST_STATUS_PATH}"
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
        self, maubot_test_bot: TestBot, maubot_plugin: OwncastSentry
    ) -> None:
        """List live streams ordered by domain with different durations."""
        # Subscribe in reverse domain order to verify domain-sorted output
        with aioresponses() as mocked:
            mocked.get(
                f"https://beta.com{_OWNCAST_STATUS_PATH}",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            mocked.get(
                f"https://alpha.com{_OWNCAST_STATUS_PATH}",
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
