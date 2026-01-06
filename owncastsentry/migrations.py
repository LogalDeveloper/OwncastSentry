# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

from mautrix.util.async_db import UpgradeTable, Connection

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Initial revision")
async def upgrade_v1(conn: Connection) -> None:
    """
    Runs migrations to upgrade database schema to verison 1 format.
    Version 1 is the initial format of the database.

    :param conn: A connection to run the v1 database migration on.
    :return: Nothing.
    """
    await conn.execute(
        """CREATE TABLE "streams" (
	        "domain"	TEXT NOT NULL UNIQUE,
	        "name"	TEXT,
	        "title"	TEXT,
	        "last_connect_time"	TEXT,
	        "last_disconnect_time"	TEXT,
	        PRIMARY KEY("domain")
        )"""
    )

    await conn.execute(
        """CREATE TABLE "subscriptions" (
	        "stream_domain"	INTEGER NOT NULL,
	        "room_id"	TEXT NOT NULL,
	        UNIQUE("room_id","stream_domain")
        )"""
    )


def get_upgrade_table() -> UpgradeTable:
    """
    Helper function for retrieving the upgrade table.

    :return: The upgrade table with registered migrations.
    """
    return upgrade_table
