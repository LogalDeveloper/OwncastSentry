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

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from mautrix.util.async_db import Database

from owncastsentry.repository import _normalize_legacy_status_since, get_upgrade_table
from owncastsentry.types import (
    UNKNOWN_STATUS_THRESHOLD,
    AlreadySubscribedError,
    NotSubscribedError,
)

if TYPE_CHECKING:
    from pathlib import Path

    from owncastsentry.repository import StreamRepository, SubscriptionRepository


class TestStreamSchema:
    """Streams table schema after migrations."""

    async def test_uses_online_and_status_since_columns(
        self, database: Database
    ) -> None:
        """Create the current schema without legacy connect/disconnect columns."""
        async with database.acquire() as conn:
            rows = await conn.fetch("PRAGMA table_info(streams)")

        assert [row["name"] for row in rows] == [
            "domain",
            "name",
            "title",
            "online",
            "status_since",
            "failure_counter",
        ]

    async def test_stream_constraint_metadata(self, database: Database) -> None:
        """Declare stream primary key and non-null constrained columns."""
        async with database.acquire() as conn:
            rows = await conn.fetch("PRAGMA table_info(streams)")
            columns = {row["name"]: row for row in rows}

            assert columns["domain"]["pk"] == 1
            assert columns["domain"]["notnull"] == 1
            assert columns["online"]["notnull"] == 1
            assert columns["failure_counter"]["notnull"] == 1

    @pytest.mark.parametrize(
        ("query", "params"),
        [
            pytest.param(
                "INSERT INTO streams (domain) VALUES ($1)",
                (None,),
                id="null-domain",
            ),
            pytest.param(
                "INSERT INTO streams (domain) VALUES ($1)",
                ("",),
                id="blank-domain",
            ),
            pytest.param(
                "INSERT INTO streams (domain) VALUES ($1)",
                ("   ",),
                id="whitespace-domain",
            ),
            pytest.param(
                "INSERT INTO streams (domain, online) VALUES ($1, $2)",
                ("bad-online.example", 2),
                id="invalid-online",
            ),
            pytest.param(
                "INSERT INTO streams (domain, online) VALUES ($1, $2)",
                ("missing-status.example", 1),
                id="online-without-status",
            ),
            pytest.param(
                """INSERT INTO streams (domain, failure_counter)
                   VALUES ($1, $2)""",
                ("bad-counter.example", -1),
                id="negative-failure-counter",
            ),
        ],
    )
    async def test_rejects_invalid_stream_values(
        self,
        database: Database,
        query: str,
        params: tuple[object, ...],
    ) -> None:
        """Reject stream rows that violate current value constraints."""
        async with database.acquire() as conn:
            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(query, *params)


class TestSubscriptionSchema:
    """Subscriptions table schema after migrations."""

    async def test_subscription_constraint_metadata(self, database: Database) -> None:
        """Declare subscription key, foreign key, index, and lifecycle triggers."""
        async with database.acquire() as conn:
            rows = await conn.fetch("PRAGMA table_info(subscriptions)")
            columns = {row["name"]: row for row in rows}
            foreign_keys = await conn.fetch("PRAGMA foreign_key_list(subscriptions)")
            indexes = await conn.fetch("PRAGMA index_list(subscriptions)")
            index_names = {row["name"] for row in indexes}
            domain_index_columns = await conn.fetch(
                "PRAGMA index_info(subscriptions_stream_domain_idx)"
            )
            triggers = await conn.fetch(
                "SELECT name FROM sqlite_schema WHERE type='trigger'"
            )
            trigger_names = {row["name"] for row in triggers}

            assert columns["room_id"]["pk"] == 1
            assert columns["stream_domain"]["pk"] == 2
            assert columns["room_id"]["notnull"] == 1
            assert columns["stream_domain"]["notnull"] == 1
            assert "subscriptions_stream_domain_idx" in index_names
            assert [row["name"] for row in domain_index_columns] == ["stream_domain"]
            assert len(foreign_keys) == 1
            assert foreign_keys[0]["table"] == "streams"
            assert foreign_keys[0]["from"] == "stream_domain"
            assert foreign_keys[0]["to"] == "domain"
            assert foreign_keys[0]["on_delete"] == "CASCADE"
            assert "create_stream_for_subscription" in trigger_names
            assert "delete_unsubscribed_stream" in trigger_names

    @pytest.mark.parametrize(
        ("stream_domain", "room_id"),
        [
            pytest.param(None, "!null-domain:example.com", id="null-domain"),
            pytest.param("", "!blank-domain:example.com", id="blank-domain"),
            pytest.param("   ", "!blank-domain:example.com", id="whitespace-domain"),
            pytest.param("example.com", None, id="null-room"),
            pytest.param("example.com", "", id="blank-room"),
            pytest.param("example.com", "   ", id="whitespace-room"),
        ],
    )
    async def test_rejects_invalid_subscription_values(
        self,
        database: Database,
        stream_domain: str | None,
        room_id: str | None,
    ) -> None:
        """Reject subscription rows that violate current constraints."""
        async with database.acquire() as conn:
            await conn.execute(
                "INSERT INTO streams (domain) VALUES ($1)",
                "example.com",
            )

            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(
                    """INSERT INTO subscriptions (stream_domain, room_id)
                       VALUES ($1, $2)""",
                    stream_domain,
                    room_id,
                )

    async def test_subscription_insert_creates_stream_parent(
        self, database: Database
    ) -> None:
        """Create the stream parent row from the subscription insert trigger."""
        async with database.acquire() as conn:
            await conn.execute(
                """INSERT INTO subscriptions (stream_domain, room_id)
                   VALUES ($1, $2)""",
                "missing.example",
                "!room:example.com",
            )
            stream = await conn.fetchrow(
                "SELECT * FROM streams WHERE domain=$1",
                "missing.example",
            )

        assert stream is not None
        assert stream["online"] == 0
        assert stream["status_since"] is None
        assert stream["failure_counter"] == 0

    async def test_subscription_insert_rolls_back_created_stream_on_failure(
        self, database: Database
    ) -> None:
        """Roll back the trigger-created stream when the subscription is invalid."""
        async with database.acquire() as conn:
            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(
                    """INSERT INTO subscriptions (stream_domain, room_id)
                       VALUES ($1, $2)""",
                    "rollback.example",
                    "",
                )

            stream = await conn.fetchrow(
                "SELECT * FROM streams WHERE domain=$1",
                "rollback.example",
            )
            subscription = await conn.fetchrow(
                """SELECT * FROM subscriptions
                   WHERE stream_domain=$1""",
                "rollback.example",
            )

        assert stream is None
        assert subscription is None

    async def test_subscription_primary_key_rejects_duplicates(
        self, database: Database
    ) -> None:
        """Reject duplicate room/domain subscription rows."""
        async with database.acquire() as conn:
            await conn.execute(
                "INSERT INTO streams (domain) VALUES ($1)",
                "example.com",
            )
            await conn.execute(
                """INSERT INTO subscriptions (stream_domain, room_id)
                   VALUES ($1, $2)""",
                "example.com",
                "!room:example.com",
            )

            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(
                    """INSERT INTO subscriptions (stream_domain, room_id)
                       VALUES ($1, $2)""",
                    "example.com",
                    "!room:example.com",
                )


class TestSubscriptionLifecycleConstraints:
    """Database-owned cleanup behavior between streams and subscriptions."""

    async def test_stream_delete_cascades_subscriptions(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Delete dependent subscription rows when a stream is deleted."""
        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room:example.com")

        await stream_repo.delete("example.com")

        assert await subscription_repo.get_subscribed_rooms("example.com") == []

    async def test_last_subscription_delete_removes_stream(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Delete the stream row when its last subscription is removed."""
        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room:example.com")

        await subscription_repo.remove("example.com", "!room:example.com")

        assert await stream_repo.get_by_domain("example.com") is None

    async def test_subscription_delete_keeps_stream_with_remaining_rooms(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Keep stream state while at least one subscription remains."""
        await stream_repo.create("example.com")
        await subscription_repo.add("example.com", "!room1:example.com")
        await subscription_repo.add("example.com", "!room2:example.com")

        await subscription_repo.remove("example.com", "!room1:example.com")

        assert await stream_repo.get_by_domain("example.com") is not None
        assert await subscription_repo.get_subscribed_rooms("example.com") == [
            "!room2:example.com"
        ]


class TestConstraintMigration:
    """Migration behavior for legacy data that did not have constraints."""

    async def test_converts_legacy_status_timestamps_from_v3(
        self, tmp_path: Path
    ) -> None:
        """Convert legacy connect/disconnect timestamps during v4 migration."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (3);

                CREATE TABLE streams (
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    title TEXT,
                    last_connect_time TEXT,
                    last_disconnect_time TEXT,
                    failure_counter INTEGER DEFAULT 0,
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    UNIQUE(room_id, stream_domain)
                );

                INSERT INTO streams (
                    domain,
                    last_connect_time,
                    last_disconnect_time,
                    failure_counter
                )
                VALUES
                    (
                        'live.example',
                        '2026-05-21T19:06:24Z',
                        '2026-05-20T00:00:00+00:00',
                        2
                    ),
                    (
                        'offline.example',
                        NULL,
                        '2026-05-17T21:23:27-04:00',
                        3
                    ),
                    (
                        'blank-connect.example',
                        '',
                        '2026-05-20T12:34:56+02:00',
                        4
                    ),
                    (
                        'invalid-timestamp.example',
                        NULL,
                        'not a timestamp',
                        5
                    );

                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES
                    ('live.example', '!live:example.com'),
                    ('offline.example', '!offline:example.com'),
                    ('blank-connect.example', '!blank:example.com'),
                    ('invalid-timestamp.example', '!invalid:example.com');
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT domain, online, status_since, failure_counter
                       FROM streams
                       ORDER BY domain"""
                )
        finally:
            await db.stop()

        streams = {row["domain"]: row for row in rows}
        assert streams["live.example"]["online"] == 1
        assert streams["live.example"]["status_since"] == "2026-05-21T19:06:24+00:00"
        assert streams["live.example"]["failure_counter"] == 2
        assert streams["offline.example"]["online"] == 0
        assert streams["offline.example"]["status_since"] == "2026-05-18T01:23:27+00:00"
        assert streams["offline.example"]["failure_counter"] == 3
        assert streams["blank-connect.example"]["online"] == 0
        assert (
            streams["blank-connect.example"]["status_since"]
            == "2026-05-20T10:34:56+00:00"
        )
        assert streams["invalid-timestamp.example"]["online"] == 0
        assert streams["invalid-timestamp.example"]["status_since"] is None

    async def test_preserves_legacy_orphaned_subscription(self, tmp_path: Path) -> None:
        """Create placeholder streams before enforcing subscription foreign keys."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (4);

                CREATE TABLE streams (
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    title TEXT,
                    online BOOLEAN NOT NULL DEFAULT false,
                    status_since TEXT,
                    failure_counter INTEGER DEFAULT 0,
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    UNIQUE(room_id, stream_domain)
                );

                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES ('missing.example', '!room:example.com');
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                stream = await conn.fetchrow(
                    "SELECT * FROM streams WHERE domain=$1",
                    "missing.example",
                )
                subscription = await conn.fetchrow(
                    """SELECT * FROM subscriptions
                       WHERE stream_domain=$1 AND room_id=$2""",
                    "missing.example",
                    "!room:example.com",
                )
        finally:
            await db.stop()

        assert stream is not None
        assert stream["online"] == 0
        assert stream["failure_counter"] == 0
        assert subscription is not None

    async def test_removes_legacy_stream_without_subscriptions(
        self, tmp_path: Path
    ) -> None:
        """Drop stream rows that are no longer referenced by subscriptions."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (4);

                CREATE TABLE streams (
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    title TEXT,
                    online BOOLEAN NOT NULL DEFAULT false,
                    status_since TEXT,
                    failure_counter INTEGER DEFAULT 0,
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    UNIQUE(room_id, stream_domain)
                );

                INSERT INTO streams (domain)
                VALUES ('subscribed.example'), ('orphaned.example');
                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES ('subscribed.example', '!room:example.com');
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                subscribed_stream = await conn.fetchrow(
                    "SELECT * FROM streams WHERE domain=$1",
                    "subscribed.example",
                )
                orphaned_stream = await conn.fetchrow(
                    "SELECT * FROM streams WHERE domain=$1",
                    "orphaned.example",
                )
        finally:
            await db.stop()

        assert subscribed_stream is not None
        assert orphaned_stream is None

    async def test_preserves_and_coerces_legacy_stream_state(
        self, tmp_path: Path
    ) -> None:
        """Keep subscribed stream state while coercing legacy constrained values."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (4);

                CREATE TABLE streams (
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    title TEXT,
                    online BOOLEAN NOT NULL DEFAULT false,
                    status_since TEXT,
                    failure_counter INTEGER DEFAULT 0,
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    UNIQUE(room_id, stream_domain)
                );

                INSERT INTO streams (
                    domain, name, title, online, status_since, failure_counter
                )
                VALUES
                    (
                        'preserved.example',
                        'Preserved Name',
                        'Preserved Title',
                        1,
                        '2026-01-01T12:00:00+00:00',
                        7
                    ),
                    ('truthy-online.example', NULL, NULL, 2, NULL, 0),
                    ('falsy-online.example', NULL, NULL, 0, NULL, 0),
                    ('negative-counter.example', NULL, NULL, 0, NULL, -3),
                    ('null-counter.example', NULL, NULL, 0, NULL, NULL);

                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES
                    ('preserved.example', '!preserved:example.com'),
                    ('truthy-online.example', '!truthy:example.com'),
                    ('falsy-online.example', '!falsy:example.com'),
                    ('negative-counter.example', '!negative:example.com'),
                    ('null-counter.example', '!null:example.com');
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT domain, name, title, online, status_since, failure_counter
                       FROM streams
                       ORDER BY domain"""
                )
        finally:
            await db.stop()

        streams = {row["domain"]: row for row in rows}
        preserved = streams["preserved.example"]
        assert preserved["name"] == "Preserved Name"
        assert preserved["title"] == "Preserved Title"
        assert preserved["online"] == 1
        assert preserved["status_since"] == "2026-01-01T12:00:00+00:00"
        assert preserved["failure_counter"] == 7
        assert streams["truthy-online.example"]["online"] == 0
        assert streams["truthy-online.example"]["status_since"] is None
        assert streams["falsy-online.example"]["online"] == 0
        assert streams["negative-counter.example"]["failure_counter"] == 0
        assert streams["null-counter.example"]["failure_counter"] == 0

    async def test_v6_repairs_online_stream_without_status_timestamp(
        self, tmp_path: Path
    ) -> None:
        """Repair v5 stream rows that were marked online without a timestamp."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (5);

                CREATE TABLE streams (
                    domain TEXT NOT NULL CHECK(length(trim(domain)) > 0),
                    name TEXT,
                    title TEXT,
                    online INTEGER NOT NULL DEFAULT 0 CHECK(online IN (0, 1)),
                    status_since TEXT,
                    failure_counter INTEGER NOT NULL DEFAULT 0
                        CHECK(failure_counter >= 0),
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT NOT NULL
                        CHECK(length(trim(stream_domain)) > 0),
                    room_id TEXT NOT NULL CHECK(length(trim(room_id)) > 0),
                    PRIMARY KEY(room_id, stream_domain),
                    FOREIGN KEY(stream_domain)
                        REFERENCES streams(domain)
                        ON DELETE CASCADE
                );

                CREATE INDEX subscriptions_stream_domain_idx
                    ON subscriptions(stream_domain);

                CREATE TRIGGER create_stream_for_subscription
                   BEFORE INSERT ON subscriptions
                   BEGIN
                       INSERT INTO streams (domain)
                       VALUES (NEW.stream_domain)
                       ON CONFLICT(domain) DO NOTHING;
                   END;

                CREATE TRIGGER delete_unsubscribed_stream
                   AFTER DELETE ON subscriptions
                   WHEN NOT EXISTS (
                       SELECT 1
                       FROM subscriptions
                       WHERE stream_domain = OLD.stream_domain
                   )
                   BEGIN
                       DELETE FROM streams
                       WHERE domain = OLD.stream_domain;
                   END;

                INSERT INTO streams (
                    domain, name, title, online, status_since, failure_counter
                )
                VALUES
                    (
                        'valid-live.example',
                        'Valid Live',
                        'Valid Title',
                        1,
                        '2026-01-01T12:00:00+00:00',
                        3
                    ),
                    ('missing-status.example', 'Missing Status', NULL, 1, NULL, 4),
                    ('offline.example', 'Offline', NULL, 0, NULL, 5);

                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES
                    ('valid-live.example', '!valid:example.com'),
                    ('missing-status.example', '!missing:example.com'),
                    ('offline.example', '!offline:example.com');
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT domain, name, title, online, status_since, failure_counter
                       FROM streams
                       ORDER BY domain"""
                )
                subscriptions = await conn.fetch(
                    """SELECT stream_domain, room_id
                       FROM subscriptions
                       ORDER BY stream_domain"""
                )
                foreign_key_errors = await conn.fetch("PRAGMA foreign_key_check")

                await conn.execute(
                    """INSERT INTO subscriptions (stream_domain, room_id)
                       VALUES ($1, $2)""",
                    "trigger-created.example",
                    "!trigger:example.com",
                )
                trigger_created_stream = await conn.fetchrow(
                    "SELECT * FROM streams WHERE domain=$1",
                    "trigger-created.example",
                )
        finally:
            await db.stop()

        streams = {row["domain"]: row for row in rows}
        valid_live = streams["valid-live.example"]
        assert valid_live["online"] == 1
        assert valid_live["status_since"] == "2026-01-01T12:00:00+00:00"
        assert valid_live["failure_counter"] == 3

        missing_status = streams["missing-status.example"]
        assert missing_status["online"] == 0
        assert missing_status["status_since"] is None
        assert missing_status["failure_counter"] == 4

        offline = streams["offline.example"]
        assert offline["online"] == 0
        assert offline["status_since"] is None
        assert offline["failure_counter"] == 5

        assert [(row["stream_domain"], row["room_id"]) for row in subscriptions] == [
            ("missing-status.example", "!missing:example.com"),
            ("offline.example", "!offline:example.com"),
            ("valid-live.example", "!valid:example.com"),
        ]
        assert foreign_key_errors == []
        assert trigger_created_stream is not None
        assert trigger_created_stream["online"] == 0
        assert trigger_created_stream["status_since"] is None

    async def test_removes_invalid_legacy_subscriptions(self, tmp_path: Path) -> None:
        """Drop legacy subscriptions with unusable domains or room IDs."""
        db_path = tmp_path / "legacy.db"
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE version (version INTEGER PRIMARY KEY);
                INSERT INTO version (version) VALUES (4);

                CREATE TABLE streams (
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    title TEXT,
                    online BOOLEAN NOT NULL DEFAULT false,
                    status_since TEXT,
                    failure_counter INTEGER DEFAULT 0,
                    PRIMARY KEY(domain)
                );

                CREATE TABLE subscriptions (
                    stream_domain TEXT,
                    room_id TEXT,
                    UNIQUE(room_id, stream_domain)
                );

                INSERT INTO streams (domain)
                VALUES ('valid.example'), ('invalid-room.example');
                INSERT INTO subscriptions (stream_domain, room_id)
                VALUES
                    ('valid.example', '!room:example.com'),
                    ('invalid-room.example', ''),
                    ('', '!blank-domain:example.com'),
                    (NULL, '!null-domain:example.com'),
                    ('null-room.example', NULL);
                """
            )

        db = Database.create(f"sqlite:///{db_path}", upgrade_table=get_upgrade_table())
        await db.start()
        try:
            async with db.acquire() as conn:
                streams = await conn.fetch("SELECT domain FROM streams")
                subscriptions = await conn.fetch(
                    "SELECT stream_domain, room_id FROM subscriptions"
                )
        finally:
            await db.stop()

        assert [row["domain"] for row in streams] == ["valid.example"]
        assert [(row["stream_domain"], row["room_id"]) for row in subscriptions] == [
            ("valid.example", "!room:example.com")
        ]


class TestNormalizeLegacyStatusSince:
    """Legacy timestamp normalization used by the v4 migration."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(
                "2026-05-21T19:06:24Z",
                "2026-05-21T19:06:24+00:00",
                id="utc-z",
            ),
            pytest.param(
                "2026-05-21T19:38:40+02:00",
                "2026-05-21T17:38:40+00:00",
                id="positive-offset",
            ),
            pytest.param(
                "2026-05-17T21:23:27-04:00",
                "2026-05-18T01:23:27+00:00",
                id="negative-offset",
            ),
            pytest.param(
                datetime(2026, 5, 21, 19, 6, 24, tzinfo=UTC),
                "2026-05-21T19:06:24+00:00",
                id="aware-datetime",
            ),
            pytest.param(
                datetime(2026, 5, 21, 19, 6, 24, tzinfo=UTC).replace(tzinfo=None),
                None,
                id="naive-datetime",
            ),
            pytest.param("", None, id="blank"),
            pytest.param(None, None, id="null"),
            pytest.param("not a timestamp", None, id="malformed"),
            pytest.param(123, None, id="unsupported-type"),
        ],
    )
    def test_normalizes_parseable_aware_timestamps(
        self, value: object, expected: str | None
    ) -> None:
        """Normalize parseable legacy values and ignore unusable ones."""
        assert _normalize_legacy_status_since(value) == expected


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


class TestStreamUpdate:
    """Partial stream state updates."""

    async def test_updates_only_supplied_fields(
        self, stream_repo: StreamRepository
    ) -> None:
        """Update supplied fields while leaving omitted fields unchanged."""
        await stream_repo.create("example.com")
        await stream_repo.update(
            "example.com",
            name="Original Name",
            title="Original Title",
            online=True,
            status_since="2026-01-01T12:00:00+00:00",
        )

        await stream_repo.update("example.com", title="Updated Title")

        state = await stream_repo.get_by_domain("example.com")
        assert state is not None
        assert state.name == "Original Name"
        assert state.title == "Updated Title"
        assert state.online is True
        assert state.status_since == "2026-01-01T12:00:00+00:00"

    async def test_can_set_nullable_field_to_null(
        self, stream_repo: StreamRepository
    ) -> None:
        """Treat None as a supplied NULL value, not an omitted field."""
        await stream_repo.create("example.com")
        await stream_repo.update(
            "example.com",
            name="Original Name",
            title="Original Title",
            status_since="2026-01-01T12:00:00+00:00",
        )

        await stream_repo.update("example.com", title=None)

        state = await stream_repo.get_by_domain("example.com")
        assert state is not None
        assert state.name == "Original Name"
        assert state.title is None
        assert state.status_since == "2026-01-01T12:00:00+00:00"

    async def test_accepts_no_supplied_fields(
        self, stream_repo: StreamRepository
    ) -> None:
        """Leave the row unchanged when no partial fields are supplied."""
        await stream_repo.create("example.com")
        await stream_repo.update(
            "example.com",
            title="Original Title",
            status_since="2026-01-01T12:00:00+00:00",
        )

        await stream_repo.update("example.com")

        state = await stream_repo.get_by_domain("example.com")
        assert state is not None
        assert state.title == "Original Title"
        assert state.status_since == "2026-01-01T12:00:00+00:00"

    async def test_rejects_online_without_status_since(
        self, stream_repo: StreamRepository
    ) -> None:
        """Reject online stream updates that leave the status timestamp unknown."""
        await stream_repo.create("example.com")

        with pytest.raises(sqlite3.IntegrityError):
            await stream_repo.update("example.com", online=True)


class TestStreamFailureCounter:
    """Stream failure counter updates."""

    async def test_reset_failure_counter_sets_counter_to_zero(
        self, stream_repo: StreamRepository
    ) -> None:
        """Reset a nonzero failure counter back to zero."""
        await stream_repo.create("example.com")
        await stream_repo.increment_failure_counter("example.com")
        await stream_repo.increment_failure_counter("example.com")

        await stream_repo.reset_failure_counter("example.com")

        state = await stream_repo.get_by_domain("example.com")
        assert state is not None
        assert state.failure_counter == 0


class TestHasRoomSubscriptions:
    """Room subscription existence checks."""

    async def test_returns_true_for_subscribed_room(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return True when the room has at least one subscription."""
        await stream_repo.create("alpha.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")

        assert await subscription_repo.has_room_subscriptions("!room1:example.com")

    async def test_returns_false_for_unsubscribed_room(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return False when the room has no subscriptions."""
        assert not await subscription_repo.has_room_subscriptions("!nobody:example.com")


class TestGetRoomSubscriptions:
    """Resolved room subscription lookup."""

    async def test_returns_sorted_stream_states(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return sorted resolved subscriptions."""
        await stream_repo.create("beta.example")
        await stream_repo.update("beta.example", name="Beta")
        await stream_repo.create("alpha.example")
        await stream_repo.update("alpha.example", name="Alpha")
        await subscription_repo.add("beta.example", "!room:example.com")
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
            "offline.example",
            name="Offline",
            status_since="2026-01-01T00:00:00+00:00",
        )
        await stream_repo.create("online.example")
        await stream_repo.update(
            "online.example",
            name="Online",
            online=True,
            status_since="2026-01-01T00:00:00+00:00",
        )
        await stream_repo.create("unknown.example")
        await stream_repo.update(
            "unknown.example",
            name="Unknown",
            online=True,
            status_since="2026-01-01T00:00:00+00:00",
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
        self,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Add a subscription row."""
        await subscription_repo.add("alpha.com", "!room1:example.com")
        assert await subscription_repo.get_subscribed_rooms("alpha.com") == [
            "!room1:example.com"
        ]

    async def test_creates_stream_parent(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Create the stream row when adding a subscription."""
        await subscription_repo.add("alpha.com", "!room1:example.com")

        assert await stream_repo.get_by_domain("alpha.com") is not None

    async def test_raises_when_existing(
        self,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Raise AlreadySubscribedError when a subscription already exists."""
        await subscription_repo.add("alpha.com", "!room1:example.com")
        with pytest.raises(AlreadySubscribedError):
            await subscription_repo.add("alpha.com", "!room1:example.com")


class TestRemoveSubscription:
    """Subscription removal behavior."""

    async def test_removes_subscription(
        self,
        subscription_repo: SubscriptionRepository,
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


class TestHasDomainSubscriptions:
    """Domain subscription existence checks."""

    async def test_returns_true_for_subscribed_domain(
        self,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
    ) -> None:
        """Return True when the domain has at least one subscription."""
        await stream_repo.create("alpha.com")
        await subscription_repo.add("alpha.com", "!room1:example.com")

        assert await subscription_repo.has_domain_subscriptions("alpha.com")

    async def test_returns_false_for_unknown_domain(
        self, subscription_repo: SubscriptionRepository
    ) -> None:
        """Return False when the domain has no subscriptions."""
        assert not await subscription_repo.has_domain_subscriptions("unknown.com")


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
