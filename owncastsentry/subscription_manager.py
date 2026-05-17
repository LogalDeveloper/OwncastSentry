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

"""Business logic for managing room stream subscriptions."""

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from .types import (
    InvalidOwncastInstanceError,
    RoomSubscription,
)

if TYPE_CHECKING:
    import logging

    from .owncast_client import OwncastClient
    from .repository import StreamRepository, SubscriptionRepository


_DOMAIN_CLEANUP_RE = re.compile(r"[^a-z0-9.-]")


def _domainify(url: str) -> str:
    """Extract and sanitize a domain from user input."""
    url = url.strip()

    if "@" in url:
        url = url.rsplit("@", 1)[1]

    if not url.startswith(("http://", "https://", "//")):
        url = f"//{url}"

    parsed = urlparse(url)
    domain = (parsed.netloc or parsed.path).lower()
    domain = domain.partition(":")[0].partition("/")[0]
    return _DOMAIN_CLEANUP_RE.sub("", domain).strip(".-")


class SubscriptionManager:
    """Coordinates subscription use cases between handlers and repositories."""

    def __init__(
        self,
        owncast_client: OwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
        logger: logging.Logger,
    ) -> None:
        """Initialize the subscription manager."""
        self.owncast_client = owncast_client
        self.stream_repo = stream_repo
        self.subscription_repo = subscription_repo
        self.log = logger

    async def subscribe(self, room_id: str, url: str) -> str:
        """Subscribe a room to stream notifications and return the stream domain.

        :param room_id: Matrix room ID to subscribe.
        :param url: User-supplied Owncast URL, domain, or Fediverse-style address.
        :return: Normalized stream domain.
        :raises InvalidOwncastInstanceError: If first-time validation fails.
        :raises AlreadySubscribedError: If the room is already subscribed.
        """
        stream_domain = _domainify(url)

        subscription_count = await self.subscription_repo.count_by_domain(stream_domain)
        if subscription_count == 0:
            is_valid = await self.owncast_client.validate_instance(stream_domain)
            if not is_valid:
                raise InvalidOwncastInstanceError(stream_domain)

        await self.subscription_repo.add(stream_domain, room_id)

        if await self.stream_repo.create(stream_domain):
            self.log.info("[%s] Discovered new stream!", stream_domain)

        self.log.info("[%s] Subscription added for room %s.", stream_domain, room_id)
        return stream_domain

    async def unsubscribe(self, room_id: str, url: str) -> str:
        """Remove a room subscription and return the stream domain.

        :param room_id: Matrix room ID to unsubscribe.
        :param url: User-supplied Owncast URL, domain, or Fediverse-style address.
        :return: Normalized stream domain.
        :raises NotSubscribedError: If no subscription was removed.
        """
        stream_domain = _domainify(url)
        await self.subscription_repo.remove(stream_domain, room_id)

        self.log.info("[%s] Subscription removed for room %s.", stream_domain, room_id)
        return stream_domain

    async def list_room_subscriptions(self, room_id: str) -> list[RoomSubscription]:
        """Return stream subscriptions for a room with display state attached."""
        return await self.subscription_repo.get_room_subscriptions(room_id)

    async def has_room_subscriptions(self, room_id: str) -> bool:
        """Return whether the room has any stream subscriptions."""
        return await self.subscription_repo.has_room_subscriptions(room_id)

    async def list_live_room_subscriptions(
        self, room_id: str
    ) -> list[RoomSubscription]:
        """Return only currently online stream subscriptions for a room."""
        return await self.subscription_repo.get_live_room_subscriptions(room_id)
