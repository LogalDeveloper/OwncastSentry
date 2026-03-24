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

"""Tests for the stream monitor."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from owncastsentry.models import StreamConfig, StreamState
from owncastsentry.notification_service import NotificationService
from owncastsentry.stream_monitor import StreamMonitor
from owncastsentry.utils import (
    CLEANUP_DELETE_THRESHOLD,
    CLEANUP_WARNING_THRESHOLD,
    SECONDS_BETWEEN_NOTIFICATIONS,
)
from tests.conftest import _StubMatrixClient, _StubOwncastClient

if TYPE_CHECKING:
    from owncastsentry.database import StreamRepository, SubscriptionRepository


def _make_monitor(
    *,
    owncast_client: _StubOwncastClient,
    stream_repo: StreamRepository,
    subscription_repo: SubscriptionRepository,
    client: _StubMatrixClient,
) -> tuple[StreamMonitor, NotificationService]:
    """Build a StreamMonitor with stubs and a real NotificationService."""
    logger = logging.getLogger("test")
    notification_service = NotificationService(
        client=client,
        subscription_repo=subscription_repo,
        logger=logger,
    )
    monitor = StreamMonitor(
        owncast_client=owncast_client,
        stream_repo=stream_repo,
        subscription_repo=subscription_repo,
        notification_service=notification_service,
        logger=logger,
    )
    return monitor, notification_service


async def _seed_stream(
    stream_repo: StreamRepository,
    subscription_repo: SubscriptionRepository,
    *,
    domain: str = "example.com",
    room_id: str = "!room:matrix.org",
    name: str | None = "Test Stream",
    title: str | None = "Test Title",
    last_connect_time: str | None = None,
    last_disconnect_time: str | None = None,
) -> None:
    """Insert a stream and subscription into the database."""
    await stream_repo.create(domain)
    state = StreamState(
        domain=domain,
        name=name,
        title=title,
        last_connect_time=last_connect_time,
        last_disconnect_time=last_disconnect_time,
    )
    await stream_repo.update(state)
    await subscription_repo.add(domain, room_id)


class TestUpdateAllStreams:
    """Parallel stream update orchestration."""

    async def test_returns_correct_counts(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return an UpdateResult with correct success and failure counts."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="ok.com", last_connect_time="2026-01-01T00:00:00Z"
            ),
            stream_config=StreamConfig(name="OK"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(stream_repo, subscription_repo, domain="ok.com")

        result = await monitor.update_all_streams(["ok.com"])
        assert result.total_streams == 1
        assert result.successful_checks == 1
        assert result.failed_checks == 0
        assert owncast.queried_domains == ["ok.com"]


class TestUpdateStreamBackoff:
    """Backoff logic that skips queries for failing streams."""

    async def test_skips_query_on_backoff(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip the HTTP query and increment the counter during backoff."""
        owncast = _StubOwncastClient()
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(stream_repo, subscription_repo, domain="fail.com")

        # Set failure counter to 5 (should skip odd cycles)
        for _ in range(5):
            await stream_repo.increment_failure_counter("fail.com")

        result = await monitor.update_stream("fail.com")
        assert result is True
        assert owncast.state_call_count == 0

        # Counter should have been incremented to 6
        state = await stream_repo.get_by_domain("fail.com")
        assert state is not None
        assert state.failure_counter == 6


class TestUpdateStreamFirstUpdate:
    """First state update of a newly subscribed stream."""

    async def test_no_notification_on_first_live(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip notifications when a stream is already live on first update."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="new.com",
                last_connect_time="2026-01-01T00:00:00Z",
                last_disconnect_time="2025-12-31T00:00:00Z",
            ),
            stream_config=StreamConfig(name="New Stream"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed with no connect/disconnect times (brand new)
        await stream_repo.create("new.com")
        await subscription_repo.add("new.com", "!room:matrix.org")

        result = await monitor.update_stream("new.com")
        assert result is True
        assert len(client.sent_messages) == 0

    async def test_no_notification_on_first_offline(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip notifications when a stream is offline on first update."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="new.com",
                last_disconnect_time="2025-12-31T00:00:00Z",
            ),
            stream_config=StreamConfig(name="New Stream"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await stream_repo.create("new.com")
        await subscription_repo.add("new.com", "!room:matrix.org")

        result = await monitor.update_stream("new.com")
        assert result is True
        assert len(client.sent_messages) == 0


class TestUpdateStreamGoesLive:
    """Stream transitioning from offline to online."""

    async def test_sends_go_live_notification(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send go-live with stream name and tags."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="live.com",
                title="Now Streaming",
                last_connect_time="2026-01-01T12:00:00Z",
                last_disconnect_time="2026-01-01T10:00:00Z",
            ),
            stream_config=StreamConfig(name="Live Stream", tags=["gaming"]),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as offline (has disconnect but no connect)
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="live.com",
            last_disconnect_time="2026-01-01T10:00:00Z",
        )

        # Set offline timer to long ago so it's not a brief outage
        monitor.offline_timer_cache["live.com"] = 0

        result = await monitor.update_stream("live.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "🎥 Live Stream is now live!\n"
            "Stream Title: Now Streaming\n"
            "\n"
            "To tune in, visit: https://live.com/\n"
            "\n"
            "#gaming"
        )

    async def test_falls_back_when_config_fetch_fails(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Use domain as name in go-live when config fetch fails."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="live.com",
                title="Now Streaming",
                last_connect_time="2026-01-01T12:00:00Z",
                last_disconnect_time="2026-01-01T10:00:00Z",
            ),
            stream_config=None,
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="live.com",
            last_disconnect_time="2026-01-01T10:00:00Z",
        )

        monitor.offline_timer_cache["live.com"] = 0

        result = await monitor.update_stream("live.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "🎥 live.com is now live!\n"
            "Stream Title: Now Streaming\n"
            "\n"
            "To tune in, visit: https://live.com/"
        )


class TestUpdateStreamBriefOffline:
    """Stream that was briefly offline and comes back."""

    async def test_no_notification_same_title(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip notification for a brief outage with no title change."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="brief.com",
                title="Same Title",
                last_connect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=StreamConfig(name="Brief Stream"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as offline with same title
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="brief.com",
            title="Same Title",
            last_disconnect_time="2026-01-01T11:55:00Z",
        )

        # Recently offline (within cooldown)
        monitor.offline_timer_cache["brief.com"] = time.monotonic() - 60

        result = await monitor.update_stream("brief.com")
        assert result is True
        assert len(client.sent_messages) == 0

    async def test_title_change_notification(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send a title-change notification for a brief outage with a new title."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="brief.com",
                title="New Title",
                last_connect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=StreamConfig(name="Brief Stream"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as offline with old title
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="brief.com",
            title="Old Title",
            last_disconnect_time="2026-01-01T11:55:00Z",
        )

        # Recently offline (within cooldown)
        monitor.offline_timer_cache["brief.com"] = time.monotonic() - 60

        result = await monitor.update_stream("brief.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "📝 Brief Stream has changed its stream title!\n"
            "Stream Title: New Title\n"
            "\n"
            "To tune in, visit: https://brief.com/"
        )


class TestUpdateStreamTitleChange:
    """Mid-session title change while stream stays online."""

    async def test_falls_back_when_config_fetch_fails(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Use domain as name in title-change when config fails."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="title.com",
                title="Updated Title",
                last_connect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=None,
        )
        client = _StubMatrixClient()
        monitor, notification_service = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="title.com",
            title="Original Title",
            last_connect_time="2026-01-01T12:00:00Z",
        )

        monitor.offline_timer_cache["title.com"] = 0
        # Subtract an extra second to ensure the cooldown has fully elapsed
        notification_service.notification_timers_cache["title.com"] = (
            time.monotonic() - SECONDS_BETWEEN_NOTIFICATIONS - 1
        )

        result = await monitor.update_stream("title.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "📝 title.com has changed its stream title!\n"
            "Stream Title: Updated Title\n"
            "\n"
            "To tune in, visit: https://title.com/"
        )

    async def test_sends_title_change(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send a title-change notification when the title changes mid-stream."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="title.com",
                title="Updated Title",
                last_connect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=StreamConfig(name="Title Stream"),
        )
        client = _StubMatrixClient()
        monitor, notification_service = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as online with different title
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="title.com",
            title="Original Title",
            last_connect_time="2026-01-01T12:00:00Z",
        )

        # Last notification was long enough ago to pass rate limiting,
        # but more recent than the offline timer (so title-change fires)
        monitor.offline_timer_cache["title.com"] = 0
        # Subtract an extra second to ensure the cooldown has fully elapsed
        notification_service.notification_timers_cache["title.com"] = (
            time.monotonic() - SECONDS_BETWEEN_NOTIFICATIONS - 1
        )

        result = await monitor.update_stream("title.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "📝 Title Stream has changed its stream title!\n"
            "Stream Title: Updated Title\n"
            "\n"
            "To tune in, visit: https://title.com/"
        )

    async def test_sends_go_live_when_last_notification_before_offline(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send go-live instead of title-change after an offline gap."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="title.com",
                title="Updated Title",
                last_connect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=StreamConfig(name="Title Stream"),
        )
        client = _StubMatrixClient()
        monitor, notification_service = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="title.com",
            title="Original Title",
            last_connect_time="2026-01-01T12:00:00Z",
        )

        # Offline timer is MORE recent than last notification,
        # and both are old enough to pass rate limiting
        now = time.monotonic()
        monitor.offline_timer_cache["title.com"] = (
            now - SECONDS_BETWEEN_NOTIFICATIONS - 100
        )
        notification_service.notification_timers_cache["title.com"] = (
            now - SECONDS_BETWEEN_NOTIFICATIONS - 200
        )

        result = await monitor.update_stream("title.com")
        assert result is True
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "🎥 Title Stream is now live!\n"
            "Stream Title: Updated Title\n"
            "\n"
            "To tune in, visit: https://title.com/"
        )


class TestUpdateStreamGoesOffline:
    """Stream transitioning from online to offline."""

    async def test_records_offline_time(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Record the offline time in the cache when a stream goes offline."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="offline.com",
                title="Title",
                last_disconnect_time="2026-01-01T12:00:00Z",
            ),
            stream_config=StreamConfig(name="Offline Stream"),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as online
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="offline.com",
            last_connect_time="2026-01-01T10:00:00Z",
        )
        monitor.offline_timer_cache["offline.com"] = 0

        before = time.monotonic()
        result = await monitor.update_stream("offline.com")
        after = time.monotonic()

        assert result is True
        assert before <= monitor.offline_timer_cache["offline.com"] <= after
        assert len(client.sent_messages) == 0


class TestUpdateStreamConnectionFailure:
    """Connection failure handling."""

    async def test_returns_false_and_increments_counter(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return False and increment the failure counter on connection failure."""
        owncast = _StubOwncastClient(stream_state=None)
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="fail.com",
            last_disconnect_time="2026-01-01T00:00:00Z",
        )

        result = await monitor.update_stream("fail.com")
        assert result is False

        state = await stream_repo.get_by_domain("fail.com")
        assert state is not None
        assert state.failure_counter == 1


class TestCheckCleanupThresholds:
    """Auto-cleanup warning and deletion thresholds."""

    async def test_sends_warning_at_threshold(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Send a cleanup warning when the failure counter hits the 83-day threshold."""
        owncast = _StubOwncastClient()
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(stream_repo, subscription_repo, domain="warn.com")

        await monitor._check_cleanup_thresholds("warn.com", CLEANUP_WARNING_THRESHOLD)

        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "⚠️  Warning: Subscription Cleanup Scheduled\n"
            "\n"
            "The Owncast instance at warn.com has been "
            "unreachable for 83 days. If it remains "
            "unreachable for 7 more days "
            "(90 days total), this subscription "
            "will be automatically removed."
        )

    async def test_deletes_at_threshold(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Delete all subscriptions and the stream record at the 90-day threshold."""
        owncast = _StubOwncastClient()
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(stream_repo, subscription_repo, domain="delete.com")

        await monitor._check_cleanup_thresholds("delete.com", CLEANUP_DELETE_THRESHOLD)

        # Deletion notification sent
        assert len(client.sent_messages) == 1
        assert client.sent_messages[0].content.body == (
            "🗑️  Subscription Automatically Removed\n"
            "\n"
            "The Owncast instance at delete.com has been "
            "unreachable for 90 days and has been "
            "automatically removed from subscriptions in this "
            "room.\n"
            "\n"
            "If the instance comes online again and you want to "
            "resubscribe, run `!subscribe delete.com`."
        )

        # Stream and subscriptions removed from DB
        assert await stream_repo.get_by_domain("delete.com") is None
        rooms = await subscription_repo.get_subscribed_rooms("delete.com")
        assert rooms == []

    async def test_no_action_below_thresholds(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Take no action when the counter is below both thresholds."""
        owncast = _StubOwncastClient()
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(stream_repo, subscription_repo, domain="ok.com")

        await monitor._check_cleanup_thresholds("ok.com", 100)

        assert len(client.sent_messages) == 0
        assert await stream_repo.get_by_domain("ok.com") is not None


class TestUpdateStreamNoStateChange:
    """Stream that stays offline across updates with no state change."""

    async def test_skips_database_write(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Skip the database write when stream state has not changed."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="stable.com",
                title="Same Title",
                last_disconnect_time="2026-01-01T12:00:00Z",
            ),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # Seed as offline with same disconnect time and title
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="stable.com",
            title="Same Title",
            last_disconnect_time="2026-01-01T12:00:00Z",
        )

        result = await monitor.update_stream("stable.com")
        assert result is True
        assert len(client.sent_messages) == 0
        # Config should not have been fetched since no DB update was needed
        assert owncast.config_call_count == 0


class TestUpdateStreamFailureCounterReset:
    """Failure counter reset on successful fetch."""

    async def test_resets_counter_on_success(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Reset the failure counter to zero after a successful fetch."""
        owncast = _StubOwncastClient(
            stream_state=StreamState(
                domain="recover.com",
                title="Title",
                last_disconnect_time="2026-01-01T12:00:00Z",
            ),
        )
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="recover.com",
            title="Title",
            last_disconnect_time="2026-01-01T12:00:00Z",
        )

        # Simulate prior failures (counter=4 still passes backoff)
        for _ in range(4):
            await stream_repo.increment_failure_counter("recover.com")

        state = await stream_repo.get_by_domain("recover.com")
        assert state is not None
        assert state.failure_counter == 4

        result = await monitor.update_stream("recover.com")
        assert result is True

        state = await stream_repo.get_by_domain("recover.com")
        assert state is not None
        assert state.failure_counter == 0


class TestUpdateAllStreamsMixed:
    """Mixed success/failure results from parallel updates."""

    async def test_counts_mixed_results(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return correct counts with one failure and one backoff skip."""
        # Stub returns None, so all actual fetches fail
        owncast = _StubOwncastClient(stream_state=None)
        client = _StubMatrixClient()
        monitor, _ = _make_monitor(
            owncast_client=owncast,
            stream_repo=stream_repo,
            subscription_repo=subscription_repo,
            client=client,
        )

        # "fail.com" will be fetched and fail (counter=0, no backoff)
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="fail.com",
            last_disconnect_time="2026-01-01T00:00:00Z",
        )

        # "skip.com" will be skipped via backoff (returns True)
        await _seed_stream(
            stream_repo,
            subscription_repo,
            domain="skip.com",
            room_id="!room2:matrix.org",
            last_disconnect_time="2026-01-01T00:00:00Z",
        )
        for _ in range(5):
            await stream_repo.increment_failure_counter("skip.com")

        result = await monitor.update_all_streams(["fail.com", "skip.com"])
        assert result.total_streams == 2
        assert result.successful_checks == 1
        assert result.failed_checks == 1
