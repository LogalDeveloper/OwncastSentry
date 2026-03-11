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

"""Database migration definitions for OwncastSentry."""

from mautrix.util.async_db import Connection, UpgradeTable

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Initial revision")  # type: ignore[arg-type, call-arg, untyped-decorator]
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
    # Create new subscriptions table with correct schema
    await conn.execute(
        """CREATE TABLE "subscriptions_new" (
            "stream_domain" TEXT NOT NULL,
            "room_id"   TEXT NOT NULL,
            UNIQUE("room_id","stream_domain")
        )"""
    )

    # Copy all existing data from old table to new table
    await conn.execute(
        """INSERT INTO subscriptions_new (stream_domain, room_id)
           SELECT stream_domain, room_id FROM subscriptions"""
    )

    # Drop the old table and rename new table to original name
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
    # Add failure_counter column with default value of 0
    await conn.execute(
        """ALTER TABLE streams ADD COLUMN failure_counter INTEGER DEFAULT 0"""
    )


def get_upgrade_table() -> UpgradeTable:
    """Return the upgrade table with registered migrations."""
    return upgrade_table
