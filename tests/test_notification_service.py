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

"""Tests for the notification service."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pytest

from owncastsentry.notification_service import NotificationService
from owncastsentry.utils import SECONDS_BETWEEN_NOTIFICATIONS
from tests.conftest import _StubMatrixClient

if TYPE_CHECKING:
    from owncastsentry.database import StreamRepository, SubscriptionRepository


def _make_service(
    *,
    client: _StubMatrixClient,
    subscription_repo: SubscriptionRepository,
) -> NotificationService:
    """Build a NotificationService with a stub client and real repo."""
    return NotificationService(
        client=client,
        subscription_repo=subscription_repo,
        logger=logging.getLogger("test"),
    )


class TestCanNotify:
    """Rate-limiting logic for notification cooldowns."""

    def test_first_notification_allowed(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Allow the first notification for an unseen domain."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        assert service._can_notify("example.com") is True

    def test_within_cooldown_blocked(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Block notifications within the cooldown window."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        service.notification_timers_cache["example.com"] = time.monotonic()
        assert service._can_notify("example.com") is False

    def test_after_cooldown_allowed(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Allow notifications after the cooldown window expires."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        # Subtract an extra second to ensure the cooldown has fully elapsed
        service.notification_timers_cache["example.com"] = (
            time.monotonic() - SECONDS_BETWEEN_NOTIFICATIONS - 1
        )
        assert service._can_notify("example.com") is True


class TestGetLastNotificationTime:
    """Last notification timestamp retrieval."""

    def test_returns_cached_value(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return the cached timestamp for a known domain."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        service.notification_timers_cache["example.com"] = 12345.0
        assert service.get_last_notification_time("example.com") == 12345.0

    def test_returns_zero_for_unknown(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return 0 for a domain that has never been notified."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        assert service.get_last_notification_time("unknown.com") == 0


class TestFormatMessage:
    """Notification message formatting."""

    @pytest.mark.parametrize(
        ("name", "title", "domain", "tags", "title_change", "expected"),
        [
            pytest.param(
                "My Stream",
                "Playing Games",
                "example.com",
                [],
                False,
                "🎥 My Stream is now live!\n"
                "Stream Title: Playing Games\n"
                "\n"
                "To tune in, visit: https://example.com/",
                id="go-live-with-title",
            ),
            pytest.param(
                "My Stream",
                "New Title",
                "example.com",
                [],
                True,
                "📝 My Stream has changed its stream title!\n"
                "Stream Title: New Title\n"
                "\n"
                "To tune in, visit: https://example.com/",
                id="title-change",
            ),
            pytest.param(
                "My Stream",
                "",
                "example.com",
                [],
                False,
                "🎥 My Stream is now live!\n\nTo tune in, visit: https://example.com/",
                id="go-live-no-title",
            ),
            pytest.param(
                "",
                "Title",
                "example.com",
                [],
                False,
                "🎥 example.com is now live!\n"
                "Stream Title: Title\n"
                "\n"
                "To tune in, visit: https://example.com/",
                id="name-fallback-to-domain",
            ),
            pytest.param(
                "Stream",
                "Title",
                "example.com",
                ["gaming", "fun"],
                False,
                "🎥 Stream is now live!\n"
                "Stream Title: Title\n"
                "\n"
                "To tune in, visit: https://example.com/\n"
                "\n"
                "#gaming #fun",
                id="with-tags",
            ),
            pytest.param(
                "Stream",
                "Title",
                "example.com",
                [".hidden", "visible"],
                False,
                "🎥 Stream is now live!\n"
                "Stream Title: Title\n"
                "\n"
                "To tune in, visit: https://example.com/\n"
                "\n"
                "#visible",
                id="dot-prefix-tag-filtered",
            ),
            pytest.param(
                "Stream",
                "Title",
                "example.com",
                [".secret"],
                False,
                "🎥 Stream is now live!\n"
                "Stream Title: Title\n"
                "\n"
                "To tune in, visit: https://example.com/",
                id="all-tags-dot-prefixed",
            ),
        ],
    )
    def test_format_message(
        self,
        name: str,
        title: str,
        domain: str,
        tags: list[str],
        title_change: bool,
        expected: str,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Format the notification message with expected content."""
        service = _make_service(
            client=_StubMatrixClient(), subscription_repo=subscription_repo
        )
        result = service._format_message(name, title, domain, tags, title_change)
        assert result == expected


class TestNotifyStreamLive:
    """End-to-end notification sending."""

    async def test_sends_to_all_subscribed_rooms(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send notifications to every room subscribed to the domain."""
        client = _StubMatrixClient()
        service = _make_service(client=client, subscription_repo=subscription_repo)

        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room1:matrix.org")
        await subscription_repo.add("example.com", "!room2:matrix.org")

        await service.notify_stream_live("example.com", "Stream", "Title", ["tag"])

        assert len(client.sent_messages) == 2
        room_ids = {msg.room_id for msg in client.sent_messages}
        assert room_ids == {"!room1:matrix.org", "!room2:matrix.org"}
        expected_body = (
            "🎥 Stream is now live!\n"
            "Stream Title: Title\n"
            "\n"
            "To tune in, visit: https://example.com/\n"
            "\n"
            "#tag"
        )
        for msg in client.sent_messages:
            assert msg.content.body == expected_body

    async def test_skips_when_rate_limited(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip sending when the domain is within the rate-limit cooldown."""
        client = _StubMatrixClient()
        service = _make_service(client=client, subscription_repo=subscription_repo)
        service.notification_timers_cache["example.com"] = time.monotonic()

        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room1:matrix.org")

        await service.notify_stream_live("example.com", "Stream", "Title", [])

        assert len(client.sent_messages) == 0

    async def test_counts_failures(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send to remaining rooms after a delivery failure."""
        client = _StubMatrixClient()
        client.should_fail_for_rooms.add("!bad:matrix.org")
        service = _make_service(client=client, subscription_repo=subscription_repo)

        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!bad:matrix.org")
        await subscription_repo.add("example.com", "!good:matrix.org")

        await service.notify_stream_live("example.com", "Stream", "Title", [])

        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].room_id == "!good:matrix.org"
        assert client.sent_messages[0].content.body == (
            "🎥 Stream is now live!\n"
            "Stream Title: Title\n"
            "\n"
            "To tune in, visit: https://example.com/"
        )


class TestSendCleanupWarning:
    """Cleanup warning notification sending."""

    async def test_sends_warning_to_all_rooms(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send cleanup warning to all subscribed rooms."""
        client = _StubMatrixClient()
        service = _make_service(client=client, subscription_repo=subscription_repo)

        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room1:matrix.org")

        await service.send_cleanup_warning("example.com")

        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "⚠️  Warning: Subscription Cleanup Scheduled\n"
            "\n"
            "The Owncast instance at example.com has been "
            "unreachable for 83 days. If it remains "
            "unreachable for 7 more days "
            "(90 days total), this subscription "
            "will be automatically removed."
        )


class TestSendCleanupDeletion:
    """Cleanup deletion notification sending."""

    async def test_sends_deletion_to_all_rooms(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send cleanup deletion notice to all subscribed rooms."""
        client = _StubMatrixClient()
        service = _make_service(client=client, subscription_repo=subscription_repo)

        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room1:matrix.org")

        await service.send_cleanup_deletion("example.com")

        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "🗑️  Subscription Automatically Removed\n"
            "\n"
            "The Owncast instance at example.com has been "
            "unreachable for 90 days and has been "
            "automatically removed from subscriptions in this "
            "room.\n"
            "\n"
            "If the instance comes online again and you want to "
            "resubscribe, run `!subscribe example.com`."
        )
