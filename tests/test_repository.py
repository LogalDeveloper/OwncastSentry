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

"""Tests for database repository classes."""

from typing import TYPE_CHECKING

import pytest

from owncastsentry.types import (
    UNKNOWN_STATUS_THRESHOLD,
    AlreadySubscribedError,
    NotSubscribedError,
    StreamState,
)

if TYPE_CHECKING:
    from owncastsentry.repository import StreamRepository, SubscriptionRepository


class TestStreamExists:
    """Stream existence checks."""

    async def test_returns_true_for_existing_stream(
        self, stream_repo: StreamRepository
    ) -> None:
        """Return True when the stream exists in the database."""
        await stream_repo.create("example.com")
        assert await stream_repo.exists("example.com") is True

    async def test_returns_false_for_missing_stream(
        self, stream_repo: StreamRepository
    ) -> None:
        """Return False when the stream does not exist in the database."""
        assert await stream_repo.exists("missing.com") is False


class TestStreamCreate:
    """Stream creation behavior."""

    async def test_returns_true_when_created(
        self, stream_repo: StreamRepository
    ) -> None:
        """Return True when a stream row is inserted."""
        assert await stream_repo.create("example.com") is True

    async def test_returns_false_when_existing(
        self, stream_repo: StreamRepository
    ) -> None:
        """Return False when a stream row already exists."""
        await stream_repo.create("example.com")
        assert await stream_repo.create("example.com") is False


class TestStreamDelete:
    """Stream record deletion."""

    async def test_removes_stream_record(self, stream_repo: StreamRepository) -> None:
        """Remove the stream record so get_by_domain returns None."""
        await stream_repo.create("example.com")
        await stream_repo.delete("example.com")
        assert await stream_repo.get_by_domain("example.com") is None


class TestGetSubscribedStreamsForRoom:
    """Subscribed stream lookup by room."""

    async def test_returns_all_domains_for_room(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return all domains a room is subscribed to."""
        await stream_repo.create("alpha.com")
        await stream_repo.create("beta.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.add("beta.com", "!room1:example.com")

        result = await subscription_repo.get_subscribed_streams_for_room(
            "!room1:example.com"
        )
        assert sorted(result) == ["alpha.com", "beta.com"]

    async def test_returns_empty_list_for_unsubscribed_room(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return an empty list when the room has no subscriptions."""
        result = await subscription_repo.get_subscribed_streams_for_room(
            "!nobody:example.com"
        )
        assert result == []


class TestHasRoomSubscriptions:
    """Room subscription existence checks."""

    async def test_returns_true_for_subscribed_room(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return True when the room has at least one subscription."""
        await subscription_repo.add("alpha.com", "!room1:example.com")

        assert await subscription_repo.has_room_subscriptions("!room1:example.com")

    async def test_returns_false_for_unsubscribed_room(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return False when the room has no subscriptions."""
        assert not await subscription_repo.has_room_subscriptions("!nobody:example.com")


class TestGetRoomSubscriptions:
    """Resolved room subscription lookup."""

    async def test_returns_sorted_stream_states_and_skips_missing_rows(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return sorted resolved subscriptions and skip missing stream rows."""
        await stream_repo.create("beta.example")
        await stream_repo.update(StreamState(domain="beta.example", name="Beta"))
        await stream_repo.create("alpha.example")
        await stream_repo.update(StreamState(domain="alpha.example", name="Alpha"))
        await subscription_repo.add("beta.example", "!room:example.com")
        await subscription_repo.add("missing.example", "!room:example.com")
        await subscription_repo.add("alpha.example", "!room:example.com")

        subscriptions = await subscription_repo.get_room_subscriptions(
            "!room:example.com"
        )

        assert [subscription.domain for subscription in subscriptions] == [
            "alpha.example",
            "beta.example",
        ]
        assert [subscription.stream_state.name for subscription in subscriptions] == [
            "Alpha",
            "Beta",
        ]

    async def test_returns_empty_list_for_unsubscribed_room(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return an empty list when the room has no resolved subscriptions."""
        subscriptions = await subscription_repo.get_room_subscriptions(
            "!nobody:example.com"
        )
        assert subscriptions == []


class TestGetLiveRoomSubscriptions:
    """Resolved live room subscription lookup."""

    async def test_returns_online_streams_and_skips_inactive_states(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return only streams whose derived status is ONLINE."""
        await stream_repo.create("offline.example")
        await stream_repo.update(
            StreamState(
                domain="offline.example",
                name="Offline",
                last_disconnect_time="2026-01-01T00:00:00Z",
            )
        )
        await stream_repo.create("online.example")
        await stream_repo.update(
            StreamState(
                domain="online.example",
                name="Online",
                last_connect_time="2026-01-01T00:00:00Z",
            )
        )
        await stream_repo.create("unknown.example")
        await stream_repo.update(
            StreamState(
                domain="unknown.example",
                name="Unknown",
                last_connect_time="2026-01-01T00:00:00Z",
            )
        )
        for _ in range(UNKNOWN_STATUS_THRESHOLD + 1):
            await stream_repo.increment_failure_counter("unknown.example")

        await subscription_repo.add("offline.example", "!room:example.com")
        await subscription_repo.add("online.example", "!room:example.com")
        await subscription_repo.add("unknown.example", "!room:example.com")

        subscriptions = await subscription_repo.get_live_room_subscriptions(
            "!room:example.com"
        )

        assert [subscription.domain for subscription in subscriptions] == [
            "online.example"
        ]

    async def test_returns_empty_list_for_room_with_no_live_streams(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return an empty list when no subscribed streams are live."""
        await stream_repo.create("offline.example")
        await subscription_repo.add("offline.example", "!room:example.com")

        subscriptions = await subscription_repo.get_live_room_subscriptions(
            "!room:example.com"
        )

        assert subscriptions == []


class TestAddSubscription:
    """Subscription creation behavior."""

    async def test_adds_subscription(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Add a subscription row."""
        await subscription_repo.add("alpha.com", "!room1:example.com")
        assert await subscription_repo.get_subscribed_rooms("alpha.com") == [
            "!room1:example.com"
        ]

    async def test_raises_when_existing(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Raise AlreadySubscribedError when a subscription already exists."""
        await subscription_repo.add("alpha.com", "!room1:example.com")
        with pytest.raises(AlreadySubscribedError):
            await subscription_repo.add("alpha.com", "!room1:example.com")


class TestRemoveSubscription:
    """Subscription removal behavior."""

    async def test_removes_subscription(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Remove an existing subscription row."""
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.remove("alpha.com", "!room1:example.com")
        assert await subscription_repo.get_subscribed_rooms("alpha.com") == []

    async def test_raises_when_missing(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Raise NotSubscribedError when no subscription exists."""
        with pytest.raises(NotSubscribedError):
            await subscription_repo.remove("alpha.com", "!room1:example.com")


class TestGetAllSubscribedDomains:
    """Unique subscribed domain retrieval."""

    async def test_returns_each_domain_once(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return each domain once even with multiple subscriptions."""
        await stream_repo.create("alpha.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.add("alpha.com", "!room2:example.com")

        result = await subscription_repo.get_all_subscribed_domains()
        assert result == ["alpha.com"]

    async def test_returns_empty_list_with_no_subscriptions(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return an empty list when there are no subscriptions."""
        result = await subscription_repo.get_all_subscribed_domains()
        assert result == []


class TestCountByDomain:
    """Subscription count by domain."""

    async def test_returns_correct_count(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return the correct subscription count for a domain."""
        await stream_repo.create("alpha.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.add("alpha.com", "!room2:example.com")

        assert await subscription_repo.count_by_domain("alpha.com") == 2

    async def test_returns_zero_for_unknown_domain(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return 0 for a domain with no subscriptions."""
        assert await subscription_repo.count_by_domain("unknown.com") == 0


class TestCountByDomains:
    """Bulk subscription counts by domain."""

    async def test_returns_counts_for_requested_domains(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return counts for requested domains and zero for missing domains."""
        await stream_repo.create("alpha.com")
        await stream_repo.create("beta.com")
        await stream_repo.create("ignored.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.add("alpha.com", "!room2:example.com")
        await subscription_repo.add("beta.com", "!room3:example.com")
        await subscription_repo.add("ignored.com", "!room4:example.com")

        assert await subscription_repo.count_by_domains(
            ["beta.com", "missing.com", "alpha.com"]
        ) == {
            "beta.com": 1,
            "missing.com": 0,
            "alpha.com": 2,
        }

    async def test_returns_empty_dict_for_empty_domain_list(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return an empty mapping when no domains are requested."""
        assert await subscription_repo.count_by_domains([]) == {}


class TestDeleteAllForDomain:
    """Bulk subscription deletion by domain."""

    async def test_deletes_all_subscriptions_and_returns_count(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Delete all subscriptions for the domain and return the count."""
        await stream_repo.create("alpha.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")
        await subscription_repo.add("alpha.com", "!room2:example.com")

        deleted = await subscription_repo.delete_all_for_domain("alpha.com")
        assert deleted == 2

        rooms = await subscription_repo.get_subscribed_rooms("alpha.com")
        assert rooms == []

    async def test_returns_zero_for_unknown_domain(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return 0 when deleting subscriptions for an unknown domain."""
        assert await subscription_repo.delete_all_for_domain("unknown.com") == 0
