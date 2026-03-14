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

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from owncastsentry.database import StreamRepository, SubscriptionRepository


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
