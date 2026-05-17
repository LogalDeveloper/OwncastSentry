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

"""Tests for subscription manager business logic."""

import logging
from typing import TYPE_CHECKING

import pytest

from owncastsentry.subscription_manager import SubscriptionManager, _domainify
from owncastsentry.types import (
    UNKNOWN_STATUS_THRESHOLD,
    AlreadySubscribedError,
    InvalidOwncastInstanceError,
    NotSubscribedError,
    StreamState,
)

if TYPE_CHECKING:
    from owncastsentry.repository import StreamRepository, SubscriptionRepository


class TestDomainify:
    """Domain extraction and sanitization from user input."""

    @pytest.mark.parametrize(
        ("input_url", "expected"),
        [
            pytest.param("example.com", "example.com", id="bare-domain"),
            pytest.param("  example.com  ", "example.com", id="surrounding-whitespace"),
            pytest.param("https://example.com", "example.com", id="https-url"),
            pytest.param("http://example.com", "example.com", id="http-url"),
            pytest.param("https://example.com:8080", "example.com", id="url-with-port"),
            pytest.param(
                "https://example.com/path/to/page",
                "example.com",
                id="url-with-path",
            ),
            pytest.param(
                "user@stream.logal.dev",
                "stream.logal.dev",
                id="email-style",
            ),
            pytest.param(
                "matrix@notify@stream.logal.dev",
                "stream.logal.dev",
                id="last-at-sign-wins",
            ),
            pytest.param("EXAMPLE.COM", "example.com", id="uppercase"),
            pytest.param("exam!ple.com", "example.com", id="special-chars-stripped"),
            pytest.param(".example.com.", "example.com", id="leading-trailing-dots"),
            pytest.param("-example.com-", "example.com", id="leading-trailing-hyphens"),
            pytest.param(
                "sub.domain.example.com",
                "sub.domain.example.com",
                id="subdomain",
            ),
        ],
    )
    def test_extracts_domain(self, input_url: str, expected: str) -> None:
        """Extract and sanitize the domain from various input formats."""
        assert _domainify(input_url) == expected


class _StubOwncastClient:
    """Owncast client stub for validation-only manager tests."""

    def __init__(self, *, valid: bool = True) -> None:
        """Initialize the stub with a fixed validation result."""
        self.valid = valid
        self.validated_domains: list[str] = []

    async def validate_instance(self, domain: str) -> bool:
        """Record the domain and return the configured validation result."""
        self.validated_domains.append(domain)
        return self.valid


@pytest.fixture
def owncast_client() -> _StubOwncastClient:
    """Return a validation-only Owncast client stub."""
    return _StubOwncastClient()


@pytest.fixture
def manager(
    owncast_client: _StubOwncastClient,
    stream_repo: StreamRepository,
    subscription_repo: SubscriptionRepository,
) -> SubscriptionManager:
    """SubscriptionManager built directly for unit tests."""
    return SubscriptionManager(
        owncast_client=owncast_client,  # type: ignore[arg-type]
        stream_repo=stream_repo,
        subscription_repo=subscription_repo,
        logger=logging.getLogger("test"),
    )


class TestManagerSubscribe:
    """SubscriptionManager subscribe workflow."""

    async def test_first_subscription_validates_and_creates_stream(
        self,
        manager: SubscriptionManager,
        owncast_client: _StubOwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """First subscription validates the instance and creates stream state."""
        domain = await manager.subscribe(
            "!room:example.com", "https://Stream.Example/foo"
        )

        assert domain == "stream.example"
        assert owncast_client.validated_domains == ["stream.example"]
        assert await stream_repo.exists("stream.example") is True
        assert await subscription_repo.get_subscribed_rooms("stream.example") == [
            "!room:example.com"
        ]

    async def test_invalid_first_subscription_raises(
        self,
        manager: SubscriptionManager,
        owncast_client: _StubOwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Invalid first-time Owncast validation raises a domain error."""
        owncast_client.valid = False

        with pytest.raises(InvalidOwncastInstanceError) as exc_info:
            await manager.subscribe("!room:example.com", "bad.example")

        assert exc_info.value.domain == "bad.example"
        assert owncast_client.validated_domains == ["bad.example"]
        assert await stream_repo.exists("bad.example") is False
        assert await subscription_repo.get_subscribed_rooms("bad.example") == []

    async def test_duplicate_subscription_raises(
        self,
        manager: SubscriptionManager,
        owncast_client: _StubOwncastClient,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Duplicate room subscription raises AlreadySubscribedError."""
        await manager.subscribe("!room:example.com", "stream.example")

        with pytest.raises(AlreadySubscribedError) as exc_info:
            await manager.subscribe("!room:example.com", "stream.example")

        assert exc_info.value.domain == "stream.example"
        assert owncast_client.validated_domains == ["stream.example"]
        assert await subscription_repo.get_subscribed_rooms("stream.example") == [
            "!room:example.com"
        ]

    async def test_existing_stream_new_room_skips_validation(
        self,
        manager: SubscriptionManager,
        owncast_client: _StubOwncastClient,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Known domains skip remote validation for additional room subscriptions."""
        await manager.subscribe("!room1:example.com", "stream.example")
        owncast_client.valid = False

        domain = await manager.subscribe("!room2:example.com", "stream.example")

        assert domain == "stream.example"
        assert owncast_client.validated_domains == ["stream.example"]
        rooms = await subscription_repo.get_subscribed_rooms("stream.example")
        assert sorted(rooms) == [
            "!room1:example.com",
            "!room2:example.com",
        ]


class TestManagerUnsubscribe:
    """SubscriptionManager unsubscribe workflow."""

    async def test_removes_existing_subscription(
        self,
        manager: SubscriptionManager,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Existing room subscription is removed and its domain is returned."""
        await manager.subscribe("!room:example.com", "stream.example")

        domain = await manager.unsubscribe("!room:example.com", "stream.example")

        assert domain == "stream.example"
        assert await subscription_repo.get_subscribed_rooms("stream.example") == []

    async def test_missing_subscription_raises(
        self,
        manager: SubscriptionManager,
    ) -> None:
        """Removing a non-existent subscription raises NotSubscribedError."""
        with pytest.raises(NotSubscribedError) as exc_info:
            await manager.unsubscribe("!room:example.com", "missing.example")

        assert exc_info.value.domain == "missing.example"


class TestManagerListings:
    """SubscriptionManager room listing behavior."""

    async def test_list_room_subscriptions_returns_sorted_stream_states(
        self,
        manager: SubscriptionManager,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return sorted room subscriptions and skip missing stream rows."""
        await stream_repo.create("beta.example")
        await stream_repo.update(StreamState(domain="beta.example", name="Beta"))
        await stream_repo.create("alpha.example")
        await stream_repo.update(StreamState(domain="alpha.example", name="Alpha"))
        await subscription_repo.add("beta.example", "!room:example.com")
        await subscription_repo.add("missing.example", "!room:example.com")
        await subscription_repo.add("alpha.example", "!room:example.com")

        subscriptions = await manager.list_room_subscriptions("!room:example.com")

        assert [subscription.domain for subscription in subscriptions] == [
            "alpha.example",
            "beta.example",
        ]
        assert [subscription.stream_state.name for subscription in subscriptions] == [
            "Alpha",
            "Beta",
        ]

    async def test_list_live_room_subscriptions_filters_online_streams(
        self,
        manager: SubscriptionManager,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Live listing includes only subscriptions with ONLINE stream status."""
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

        subscriptions = await manager.list_live_room_subscriptions("!room:example.com")

        assert [subscription.domain for subscription in subscriptions] == [
            "online.example"
        ]
