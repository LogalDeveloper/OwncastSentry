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

"""Repository and schema upgrade definitions for OwncastSentry."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from mautrix.util.async_db import Connection, UpgradeTable

from .types import (
    UNKNOWN_STATUS_THRESHOLD,
    AlreadySubscribedError,
    NotSubscribedError,
    RoomSubscription,
    StreamState,
    format_status_since,
)

if TYPE_CHECKING:
    from mautrix.util.async_db import Database


upgrade_table = UpgradeTable()


def _has_legacy_timestamp(value: Any) -> bool:
    """Return whether a legacy timestamp value carries usable content."""
    return value is not None and str(value).strip() != ""


def _normalize_legacy_status_since(value: Any) -> str | None:
    """Normalize a legacy timestamp value to the canonical UTC format."""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return None
    return format_status_since(parsed.astimezone(UTC))


@upgrade_table.register(  # type: ignore[arg-type, call-arg, untyped-decorator]
    description="Initial revision"
)
async def upgrade_v1(conn: Connection) -> None:
    """Create the initial database schema.

    Creates the streams and subscriptions tables.

    :param conn: A connection to run the v1 database migration on.
    """
    await conn.execute(
        """CREATE TABLE "streams" (
            "domain"    TEXT NOT NULL UNIQUE,
            "name"  TEXT,
            "title" TEXT,
            "last_connect_time" TEXT,
            "last_disconnect_time"  TEXT,
            PRIMARY KEY("domain")
        )"""
    )

    await conn.execute(
        """CREATE TABLE "subscriptions" (
            "stream_domain" INTEGER NOT NULL,
            "room_id"   TEXT NOT NULL,
            UNIQUE("room_id","stream_domain")
        )"""
    )


@upgrade_table.register(  # type: ignore[arg-type, call-arg, untyped-decorator]
    description="Fix stream_domain column type from INTEGER to TEXT"
)
async def upgrade_v2(conn: Connection) -> None:
    """Upgrade database schema to version 2 format.

    Fixes the stream_domain column type in the subscriptions table
    from INTEGER to TEXT.

    :param conn: A connection to run the v2 database migration on.
    """
    await conn.execute(
        """CREATE TABLE "subscriptions_new" (
            "stream_domain" TEXT NOT NULL,
            "room_id"   TEXT NOT NULL,
            UNIQUE("room_id","stream_domain")
        )"""
    )

    await conn.execute(
        """INSERT INTO subscriptions_new (stream_domain, room_id)
           SELECT stream_domain, room_id FROM subscriptions"""
    )

    await conn.execute("DROP TABLE subscriptions")
    await conn.execute("ALTER TABLE subscriptions_new RENAME TO subscriptions")


@upgrade_table.register(  # type: ignore[arg-type, call-arg, untyped-decorator]
    description="Add failure_counter column for backoff and auto-cleanup"
)
async def upgrade_v3(conn: Connection) -> None:
    """Upgrade database schema to version 3 format.

    Adds the failure_counter column to track connection failures
    for backoff and auto-cleanup.

    :param conn: A connection to run the v3 database migration on.
    """
    await conn.execute(
        """ALTER TABLE streams ADD COLUMN failure_counter INTEGER DEFAULT 0"""
    )


@upgrade_table.register(  # type: ignore[arg-type, call-arg, untyped-decorator]
    description="Store online status and current status timestamp"
)
async def upgrade_v4(conn: Connection) -> None:
    """Upgrade database schema to version 4 format.

    Replaces separate connect and disconnect timestamp columns with an
    authoritative online flag and a timestamp for the current status.

    :param conn: A connection to run the v4 database migration on.
    """
    await conn.execute(
        """CREATE TABLE "streams_new" (
            "domain"    TEXT NOT NULL UNIQUE,
            "name"  TEXT,
            "title" TEXT,
            "online" BOOLEAN NOT NULL DEFAULT false,
            "status_since" TEXT,
            "failure_counter" INTEGER DEFAULT 0,
            PRIMARY KEY("domain")
        )"""
    )

    rows = await conn.fetch(
        """SELECT domain, name, title, last_connect_time, last_disconnect_time,
                  failure_counter
           FROM streams"""
    )
    for row in rows:
        online = _has_legacy_timestamp(row["last_connect_time"])
        legacy_timestamp = (
            row["last_connect_time"] if online else row["last_disconnect_time"]
        )
        await conn.execute(
            """INSERT INTO streams_new (
                   domain, name, title, online, status_since, failure_counter
               )
               VALUES ($1, $2, $3, $4, $5, $6)""",
            row["domain"],
            row["name"],
            row["title"],
            online,
            _normalize_legacy_status_since(legacy_timestamp),
            row["failure_counter"],
        )

    await conn.execute("DROP TABLE streams")
    await conn.execute("ALTER TABLE streams_new RENAME TO streams")


def get_upgrade_table() -> UpgradeTable:
    """Return the repository upgrade table with registered migrations."""
    return upgrade_table


class StreamRepository:
    """Repository for managing stream data in the database."""

    def __init__(self, database: Database) -> None:
        """Initialize the stream repository.

        :param database: The maubot database instance.
        """
        self.db: Any = database

    async def create(self, domain: str) -> bool:
        """Create a new stream entry in the database.

        :param domain: The stream domain.
        :return: True if created, False if the stream already existed.
        """
        query = """INSERT INTO streams (domain)
                   VALUES ($1)
                   ON CONFLICT (domain) DO NOTHING"""
        async with self.db.acquire() as conn:
            result = await conn.execute(query, domain)
            return int(result.rowcount) > 0

    async def get_by_domain(self, domain: str) -> StreamState | None:
        """Get a stream's state by domain.

        :param domain: The stream domain.
        :return: StreamState if found, None otherwise.
        """
        query = "SELECT * FROM streams WHERE domain=$1"
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(query, domain)
            return StreamState.from_db_row(row) if row else None

    async def exists(self, domain: str) -> bool:
        """Check if a stream exists in the database.

        :param domain: The stream domain.
        :return: True if exists, False otherwise.
        """
        result = await self.get_by_domain(domain)
        return result is not None

    async def update(self, state: StreamState) -> None:
        """Update a stream's state in the database.

        This updates display/state fields only. Failure counters are
        updated through dedicated methods.

        :param state: The StreamState to save.
        """
        query = """UPDATE streams
                   SET name=$1, title=$2, online=$3, status_since=$4
                   WHERE domain=$5"""
        async with self.db.acquire() as conn:
            await conn.execute(
                query,
                state.name,
                state.title,
                state.online,
                state.status_since,
                state.domain,
            )

    async def delete(self, domain: str) -> None:
        """Delete a stream record from the database.

        :param domain: The stream domain.
        """
        query = "DELETE FROM streams WHERE domain=$1"
        async with self.db.acquire() as conn:
            await conn.execute(query, domain)

    async def increment_failure_counter(self, domain: str) -> None:
        """Increment the failure counter for a stream by 1.

        :param domain: The stream domain.
        """
        query = """UPDATE streams
                   SET failure_counter = failure_counter + 1
                   WHERE domain=$1"""
        async with self.db.acquire() as conn:
            await conn.execute(query, domain)

    async def reset_failure_counter(self, domain: str) -> None:
        """Reset the failure counter for a stream to 0.

        :param domain: The stream domain.
        """
        query = """UPDATE streams
                   SET failure_counter = 0
                   WHERE domain=$1 AND failure_counter != 0"""
        async with self.db.acquire() as conn:
            await conn.execute(query, domain)


class SubscriptionRepository:
    """Repository for managing stream subscriptions in the database."""

    def __init__(self, database: Database) -> None:
        """Initialize the subscription repository.

        :param database: The maubot database instance.
        """
        self.db: Any = database

    async def add(self, domain: str, room_id: str) -> None:
        """Add a subscription for a room to a stream.

        :param domain: The stream domain.
        :param room_id: The Matrix room ID.
        :raises AlreadySubscribedError: If subscription already exists.
        """
        query = """INSERT INTO subscriptions (stream_domain, room_id)
                   VALUES ($1, $2)
                   ON CONFLICT (room_id, stream_domain) DO NOTHING"""
        async with self.db.acquire() as conn:
            result = await conn.execute(query, domain, room_id)
            if int(result.rowcount) == 0:
                raise AlreadySubscribedError(domain)

    async def remove(self, domain: str, room_id: str) -> None:
        """Remove a subscription for a room from a stream.

        :param domain: The stream domain.
        :param room_id: The Matrix room ID.
        :raises NotSubscribedError: If no subscription exists.
        """
        query = "DELETE FROM subscriptions WHERE stream_domain=$1 AND room_id=$2"
        async with self.db.acquire() as conn:
            result = await conn.execute(query, domain, room_id)
            if int(result.rowcount) == 0:
                raise NotSubscribedError(domain)

    async def delete_all_for_domain(self, domain: str) -> int:
        """Delete all subscriptions for a given stream domain.

        :param domain: The stream domain.
        :return: Number of subscriptions deleted.
        """
        query = "DELETE FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:
            result = await conn.execute(query, domain)
            return int(result.rowcount)

    async def get_subscribed_rooms(self, domain: str) -> list[str]:
        """Get all room IDs subscribed to a stream.

        :param domain: The stream domain.
        :return: List of room IDs.
        """
        query = "SELECT room_id FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:
            results = await conn.fetch(query, domain)
            return [row["room_id"] for row in results]

    async def get_subscribed_streams_for_room(self, room_id: str) -> list[str]:
        """Get all stream domains that a room is subscribed to.

        :param room_id: The Matrix room ID.
        :return: List of stream domains.
        """
        query = "SELECT stream_domain FROM subscriptions WHERE room_id=$1"
        async with self.db.acquire() as conn:
            results = await conn.fetch(query, room_id)
            return [row["stream_domain"] for row in results]

    async def has_room_subscriptions(self, room_id: str) -> bool:
        """Check whether a room has any subscriptions."""
        query = "SELECT 1 FROM subscriptions WHERE room_id=$1 LIMIT 1"
        async with self.db.acquire() as conn:
            result = await conn.fetchrow(query, room_id)
            return result is not None

    async def get_room_subscriptions(self, room_id: str) -> list[RoomSubscription]:
        """Get resolved stream subscriptions for a room ordered by domain.

        :param room_id: The Matrix room ID.
        :return: Subscriptions with stream state attached.
        """
        query = """SELECT streams.*
                   FROM subscriptions
                   JOIN streams ON streams.domain = subscriptions.stream_domain
                   WHERE subscriptions.room_id=$1
                   ORDER BY streams.domain"""
        async with self.db.acquire() as conn:
            results = await conn.fetch(query, room_id)
            return [
                RoomSubscription(
                    domain=row["domain"],
                    stream_state=StreamState.from_db_row(row),
                )
                for row in results
            ]

    async def get_live_room_subscriptions(self, room_id: str) -> list[RoomSubscription]:
        """Get resolved live stream subscriptions for a room ordered by domain."""
        query = """SELECT streams.*
                   FROM subscriptions
                   JOIN streams ON streams.domain = subscriptions.stream_domain
                   WHERE subscriptions.room_id=$1
                     AND streams.online=true
                     AND streams.failure_counter <= $2
                   ORDER BY streams.domain"""
        async with self.db.acquire() as conn:
            results = await conn.fetch(query, room_id, UNKNOWN_STATUS_THRESHOLD)
            return [
                RoomSubscription(
                    domain=row["domain"],
                    stream_state=StreamState.from_db_row(row),
                )
                for row in results
            ]

    async def get_all_subscribed_domains(self) -> list[str]:
        """Get all unique stream domains that have at least one subscription.

        :return: List of stream domains.
        """
        query = "SELECT DISTINCT stream_domain FROM subscriptions"
        async with self.db.acquire() as conn:
            results = await conn.fetch(query)
            return [row["stream_domain"] for row in results]

    async def count_by_domain(self, domain: str) -> int:
        """Count the number of subscriptions for a given stream domain.

        :param domain: The stream domain.
        :return: Number of subscriptions.
        """
        query = "SELECT COUNT(*) FROM subscriptions WHERE stream_domain=$1"
        async with self.db.acquire() as conn:
            result = await conn.fetchrow(query, domain)
            return int(result[0])

    async def count_by_domains(self, domains: list[str]) -> dict[str, int]:
        """Count subscriptions for each requested stream domain.

        :param domains: The stream domains to count subscriptions for.
        :return: Mapping from each requested domain to its subscription count.
        """
        if not domains:
            return {}

        counts = dict.fromkeys(domains, 0)
        query = """SELECT stream_domain, COUNT(*) AS subscription_count
                   FROM subscriptions
                   GROUP BY stream_domain"""
        async with self.db.acquire() as conn:
            results = await conn.fetch(query)

        for row in results:
            domain = row["stream_domain"]
            if domain in counts:
                counts[domain] = int(row["subscription_count"])
        return counts
