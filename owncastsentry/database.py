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

"""Database repository classes for OwncastSentry."""

from typing import TYPE_CHECKING

from .models import StreamState

if TYPE_CHECKING:
    from mautrix.util.async_db import Database


class StreamRepository:
    """Repository for managing stream data in the database."""

    def __init__(self, database: Database):
        """Initialize the stream repository.

        :param database: The maubot database instance.
        """
        self.db = database

    async def get_by_domain(self, domain: str) -> StreamState | None:
        """Get a stream's state by domain.

        :param domain: The stream domain.
        :return: StreamState if found, None otherwise.
        """
        query = "SELECT * FROM streams WHERE domain=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            row = await conn.fetchrow(query, domain)
            return StreamState.from_db_row(row) if row else None

    async def create(self, domain: str) -> None:
        """Create a new stream entry in the database.

        :param domain: The stream domain.
        """
        query = "INSERT INTO streams (domain) VALUES ($1)"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(query, domain)

    async def update(self, state: StreamState) -> None:
        """Update a stream's state in the database.

        :param state: The StreamState to save.
        """
        query = """UPDATE streams
                   SET name=$1, title=$2, last_connect_time=$3, last_disconnect_time=$4
                   WHERE domain=$5"""
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(
                query,
                state.name,
                state.title,
                state.last_connect_time,
                state.last_disconnect_time,
                state.domain,
            )

    async def exists(self, domain: str) -> bool:
        """Check if a stream exists in the database.

        :param domain: The stream domain.
        :return: True if exists, False otherwise.
        """
        result = await self.get_by_domain(domain)
        return result is not None

    async def increment_failure_counter(self, domain: str) -> None:
        """Increment the failure counter for a stream by 1.

        :param domain: The stream domain.
        """
        query = """UPDATE streams
                   SET failure_counter = failure_counter + 1
                   WHERE domain=$1"""
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(query, domain)

    async def reset_failure_counter(self, domain: str) -> None:
        """Reset the failure counter for a stream to 0.

        :param domain: The stream domain.
        """
        query = """UPDATE streams
                   SET failure_counter = 0
                   WHERE domain=$1"""
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(query, domain)

    async def delete(self, domain: str) -> None:
        """Delete a stream record from the database.

        :param domain: The stream domain.
        """
        query = "DELETE FROM streams WHERE domain=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(query, domain)


class SubscriptionRepository:
    """Repository for managing stream subscriptions in the database."""

    def __init__(self, database: Database):
        """Initialize the subscription repository.

        :param database: The maubot database instance.
        """
        self.db = database

    async def add(self, domain: str, room_id: str) -> None:
        """Add a subscription for a room to a stream.

        :param domain: The stream domain.
        :param room_id: The Matrix room ID.
        :raises sqlite3.IntegrityError: If subscription already exists.
        """
        query = "INSERT INTO subscriptions (stream_domain, room_id) VALUES ($1, $2)"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            await conn.execute(query, domain, room_id)

    async def remove(self, domain: str, room_id: str) -> int:
        """Remove a subscription for a room from a stream.

        :param domain: The stream domain.
        :param room_id: The Matrix room ID.
        :return: Number of rows deleted (0 or 1).
        """
        query = "DELETE FROM subscriptions WHERE stream_domain=$1 AND room_id=$2"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            result = await conn.execute(query, domain, room_id)
            return int(result.rowcount)

    async def get_subscribed_rooms(self, domain: str) -> list[str]:
        """Get all room IDs subscribed to a stream.

        :param domain: The stream domain.
        :return: List of room IDs.
        """
        query = "SELECT room_id FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            results = await conn.fetch(query, domain)
            return [row["room_id"] for row in results]

    async def get_subscribed_streams_for_room(self, room_id: str) -> list[str]:
        """Get all stream domains that a room is subscribed to.

        :param room_id: The Matrix room ID.
        :return: List of stream domains.
        """
        query = "SELECT stream_domain FROM subscriptions WHERE room_id=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            results = await conn.fetch(query, room_id)
            return [row["stream_domain"] for row in results]

    async def get_all_subscribed_domains(self) -> list[str]:
        """Get all unique stream domains that have at least one subscription.

        :return: List of stream domains.
        """
        query = "SELECT DISTINCT stream_domain FROM subscriptions"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            results = await conn.fetch(query)
            return [row["stream_domain"] for row in results]

    async def count_by_domain(self, domain: str) -> int:
        """Count the number of subscriptions for a given stream domain.

        :param domain: The stream domain.
        :return: Number of subscriptions.
        """
        query = "SELECT COUNT(*) FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            result = await conn.fetchrow(query, domain)
            return int(result[0])

    async def delete_all_for_domain(self, domain: str) -> int:
        """Delete all subscriptions for a given stream domain.

        :param domain: The stream domain.
        :return: Number of subscriptions deleted.
        """
        query = "DELETE FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:  # type: ignore[var-annotated]
            result = await conn.execute(query, domain)
            return int(result.rowcount)
