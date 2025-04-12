# Copyright 2025 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import sqlite3
import aiohttp
import json
import asyncio
import time

from maubot import Plugin, MessageEvent
from maubot.handlers import command
from mautrix.types import TextMessageEventContent, MessageType
from mautrix.util.async_db import UpgradeTable, Connection

from urllib.parse import urlparse


# Path to the GetStatus API call on Owncast instances
OWNCAST_STATUS_PATH = "/api/status"

# Path to GetWebConfig API call on Owncast instances
OWNCAST_CONFIG_PATH = "/api/config"

# User agent to send with all HTTP requests.
USER_AGENT = (
    "OwncastSentry/1.0.3 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)"
)

# Hard minimum amount of time between when notifications can be sent for a stream. Prevents spamming notifications for glitchy or malicious streams.
SECONDS_BETWEEN_NOTIFICATIONS = 20 * 60  # 20 minutes in seconds

# I'm not sure the best way to name or explain this variable, so let's just say what uses it:
#
# After a stream goes offline, a timer is started. Then, ...
# - If a stream comes back online with the same title within this time, no notification is sent.
# - If a stream comes back online with a different title, a rename notification is sent.
# - If this time period passes entirely and a stream comes back online after, it's treated as regular going live.
TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN = 7 * 60  # 7 minutes in seconds

# ===== DATABASE MIGRATIONS =====
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


# ===== MAIN BOT CLASS =====
class OwncastSentry(Plugin):
    # Helper variables for setting up a special HTTP ClientSession for monitoring streams.
    headers = {
        "User-Agent": USER_AGENT
    }  # Override User Agent to special static value to identify to servers our purpose.
    cookie_jar = aiohttp.DummyCookieJar()  # Ignore cookies.
    baseconnctor = aiohttp.TCPConnector(
        use_dns_cache=False, limit=1000, limit_per_host=1, keepalive_timeout=120
    )  # Set up timeouts to keep one long-lived connection open per monitored stream.
    timeouts = aiohttp.ClientTimeout(
        sock_connect=5, sock_read=5
    )  # Tighten up timeouts for faster responses to users in case of broken servers.

    # Final HTTP ClientSession for all requests to Owncast instances.
    session = aiohttp.ClientSession(
        headers=headers, cookie_jar=cookie_jar, timeout=timeouts, connector=baseconnctor
    )

    # Keeps track of when a notification was last sent for streams.
    notification_timers_cache = {}

    # Keeps track of when streams last went offline.
    offline_timer_cache = {}

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable | None:
        """
        Helper method for telling Maubot about our database migrations.

        :return: An UpgradeTable with our registered migrations.
        """
        return upgrade_table

    async def start(self) -> None:
        """
        Method called by Maubot upon startup of the instance.
        Registers a recurring task every minute to update the state of all subscribed streams.

        :return: Nothing.
        """
        self.sched.run_periodically(60, self.update_all_stream_states)

    @command.new(help="Subscribes to a new Owncast stream.")
    @command.argument("url")
    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """
        "!subscribe" command handler for users to subscribe a room to a given stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and subscribe to.
        :return: Nothing.
        """

        # Convert the user input to only a domain.
        stream_domain = self.domainify(url)

        # How many subscriptions already exist for this domain?
        query = "SELECT COUNT(*) FROM subscriptions WHERE stream_domain=$1"
        async with self.database.acquire() as connection:
            result = await connection.fetchrow(query, stream_domain)

        if result[0] == 0:
            # There are 0 subscriptions, we need to validate this domain is an Owncast stream.
            # Attempt to fetch the stream state from this domain.
            stream_state = await self.get_stream_state(stream_domain)
            if len(stream_state) == 0:
                # The stream state fetch returned nothing. Probably not an Owncast stream.
                await evt.reply(
                    "The URL you supplied does not appear to be a valid Owncast instance. You may have specified an invalid domain, or the instance is offline."
                )
                return

        # Try to add a new subscription for the requested stream domain in the room the command was executed in.
        try:
            query = "INSERT INTO subscriptions (stream_domain, room_id) VALUES ($1, $2)"
            async with self.database.acquire() as connection:
                await connection.execute(query, stream_domain, evt.room_id)
        except sqlite3.IntegrityError as exception:
            # Something weird happened... Was it due to attempting to insert a duplicate row?
            if "UNIQUE constraint failed" in exception.args[0]:
                # Yes, this is an expected condition. Tell the user the room is already subscribed and give up.
                await evt.reply(
                    "This room is already subscribed to notifications for "
                    + stream_domain
                    + "."
                )
                return
            else:
                # Nope... Something unexpected happened. Give up.
                self.log.error(
                    f"[{stream_domain}] An error occurred while attempting to add subscription in room {evt.room_id}: {e}"
                )
                raise exception

        # The subscription was successfully added! Try to add a placeholder row for the stream's state in the streams table.
        try:
            query = "INSERT INTO streams (domain) VALUES ($1)"

            async with self.database.acquire() as connection:
                await connection.execute(query, stream_domain)

            # The insert was successful, so this is the first time we're seeing this stream. Log it.
            self.log.info(f"[{stream_domain}] Discovered new stream!")
        except sqlite3.IntegrityError as exception:
            # Attempts to add rows for streams already known is an expected condition. What is anything except that?
            if "UNIQUE constraint failed" not in exception.args[0]:
                # Something unexpected happened. Give up.
                self.log.error(
                    f"[{stream_domain}] An error occurred while attempting to add stream information after adding subscription: {e}"
                )
                raise exception

        # All went well! We added a new subscription and (at least tried) to add a row for the stream state. Tell the user.
        self.log.info(f"[{stream_domain}] Subscription added for room {evt.room_id}.")
        await evt.reply(
            "Subscription added! This room will receive notifications when "
            + stream_domain
            + " goes live."
        )

    @command.new(help="Unsubscribes from an Owncast stream.")
    @command.argument("url")
    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """
        "!unsubscribe" command handler for users to unsubscribe a room from a given stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and unsubscribe from.
        :return: Nothing.
        """
        # Convert the user input to only a domain.
        stream_domain = self.domainify(url)

        # Attempt to delete the requested subscription from the database.
        query = "DELETE FROM subscriptions WHERE stream_domain=$1 AND room_id=$2"
        async with self.database.acquire() as connection:
            result = await connection.execute(query, stream_domain, evt.room_id)

        # Did it work?
        if result.rowcount == 1:
            # Yes, one row was deleted. Tell the user.
            self.log.info(
                f"[{stream_domain}] Subscription removed for room {evt.room_id}."
            )
            await evt.reply(
                "Subscription removed! This room will no longer receive notifications for "
                + stream_domain
                + "."
            )
        elif result.rowcount == 0:
            # No, nothing changed. Tell the user.
            await evt.reply(
                "This room is already not subscribed to notifications for "
                + stream_domain
                + "."
            )
        else:
            # Somehow more than 1 (or even less than 0 ???) rows were changed... Log it!
            self.log.error(
                "Encountered strange situation! Expected 0 or 1 rows on DELETE query for removing subscription; got "
                + result.rowcount
                + " instead. Something very bad may have happened!!!!"
            )

    async def update_all_stream_states(self) -> None:
        """
        Checks the status of all streams with active subscriptions.
        Updates for all streams are performed asynchronously, with the method returning when the slowest update completes.

        :return: Nothing.
        """
        self.log.debug("Updating all stream states...")

        # Make a list of all stream domains with an active subscription.
        query = "SELECT DISTINCT stream_domain FROM subscriptions"
        async with self.database.acquire() as connection:
            results = await connection.fetch(query)

        # Build a list of async tasks which update the state for each stream domain fetched.
        tasks = []
        for row in results:
            domain = row["stream_domain"]

            tasks.append(asyncio.create_task(self.update_stream_state(domain)))

        # Run the tasks in parallel.
        await asyncio.gather(*tasks)
        self.log.debug("Update complete.")

    async def update_stream_state(self, domain: str) -> None:
        """
        Updates the state of a given stream domain and sends notifications to subscribed Matrix rooms if it goes live.

        :param domain: The domain of the stream to update.
        :return: Nothing.
        """

        # A flag indicating whether to send a notification.
        # Used for the first state update of a brand-new stream to avoid sending notifications if its already live.
        first_update = True

        # A flag indicating whether to update the stream's state in the databased.
        # Used to avoid writing to the database when a stream's state hasn't changed at all.
        update_database = False

        # Holds the stream's latest configuration, if fetched and deemed necessary during the update process.
        stream_config = {}

        # Fetch the latest stream state from the server.
        new_state = await self.get_stream_state(domain)

        # Skip the update if the fetch failed for any reason.
        if new_state == {}:
            return

        # Fix possible race conditions with timers
        if domain not in self.offline_timer_cache:
            self.offline_timer_cache[domain] = 0
        if domain not in self.notification_timers_cache:
            self.notification_timers_cache[domain] = 0

        # Fetch the last known stream state from the database.
        query = "SELECT * FROM streams WHERE domain=$1"
        async with self.database.acquire() as connection:
            old_state = await connection.fetchrow(query, domain)

        # Does the last known stream state not have a value for the last connect and disconnect time?
        if (
            old_state["last_connect_time"] is None
            and old_state["last_disconnect_time"] is None
        ):
            # Yes, this is the first update. Don't send any notifications.
            update_database = True
            first_update = False

        # Does the latest stream state have a last connect time and the old state not have one?
        if (
            new_state["lastConnectTime"] is not None
            and old_state["last_connect_time"] is None
        ):
            # Yes! This stream is now live.
            update_database = True
            stream_config = await self.get_stream_config(domain)

            # Is this the first update for this stream?
            if not first_update:
                # Yes, only log it and stop any further work on this stream.
                self.log.info(
                    f"[{domain}] Stream is live, but performed first update. Will not notify subscribed rooms."
                )
                return

            self.log.info(f"[{domain}] Stream is now live!")

            # Calculate how many seconds since the stream last went offline
            seconds_since_last_offline = round(
                time.time() - self.offline_timer_cache[domain]
            )

            # Has this stream been offline for a short amount of time?
            if seconds_since_last_offline < TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN:
                # Yes. Did the stream title change?
                if old_state["title"] != new_state["streamTitle"]:
                    # Yes. The stream was only down for a short time, send a special notification indicating the stream changed its name.
                    await self.notify_rooms_of_stream_online(
                        domain,
                        stream_config["name"],
                        new_state["streamTitle"],
                        True,
                        stream_config["tags"],
                    )
                else:
                    # No. The stream was only down for a short time and didn't change its title. Don't send a notification.
                    self.log.info(
                        f"[{domain}] Not sending notifications. Stream was only offline for {seconds_since_last_offline} of {TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN} seconds and did not change its title."
                    )
            else:
                # This stream has been offline for a while. Send a normal notification.
                await self.notify_rooms_of_stream_online(
                    domain,
                    stream_config["name"],
                    new_state["streamTitle"],
                    False,
                    stream_config["tags"],
                )

        if (
            new_state["lastConnectTime"] is not None
            and old_state["last_connect_time"] is not None
        ):
            # Did the stream title change mid-session?
            if old_state["title"] != new_state["streamTitle"]:
                self.log.info(f"[{domain}] Stream title was changed!")
                update_database = True
                stream_config = await self.get_stream_config(domain)

                # This is a fun case to account for... Let's try and explain this.
                # Was the last time this stream sent a notification before it last went offline?
                if (
                    self.offline_timer_cache[domain]
                    > self.notification_timers_cache[domain]
                ):
                    # Yes. Send a regular go live notification.
                    # Why? A title change notification could be confusing to users in this case.
                    # How? If a stream goes offline before its next allowed notification, it'll get rate limited. If it then changes its title, this part of the code will send a title change notification. This can be a little confusing, so override to a normal go live notification in this case.
                    await self.notify_rooms_of_stream_online(
                        domain,
                        stream_config["name"],
                        new_state["streamTitle"],
                        False,
                        stream_config["tags"],
                    )
                else:
                    # No. Send a normal title change notification.
                    await self.notify_rooms_of_stream_online(
                        domain,
                        stream_config["name"],
                        new_state["streamTitle"],
                        True,
                        stream_config["tags"],
                    )

        # Does the latest stream state no longer have a last connect time but the old state does?
        elif (
            new_state["lastConnectTime"] is None
            and old_state["last_connect_time"] is not None
        ):
            # Yep. This stream is now offline. Log it.
            update_database = True
            stream_config = await self.get_stream_config(domain)
            self.offline_timer_cache[domain] = time.time()
            if first_update:
                self.log.info(f"[{domain}] Stream is now offline.")
            else:
                self.log.info(f"[{domain}] Stream is offline.")

        # Update the database with the current stream state, if needed.
        if update_database:
            self.log.debug(f"[{domain}] Updating stream state in database...")
            update_query = "UPDATE streams SET name=$1, title=$2, last_connect_time=$3, last_disconnect_time=$4 WHERE domain=$5"
            async with self.database.acquire() as connection:
                await connection.execute(
                    update_query,
                    stream_config["name"],
                    new_state["streamTitle"],
                    new_state["lastConnectTime"],
                    new_state["lastDisconnectTime"],
                    domain,
                )

        # All done.
        self.log.debug(f"[{domain}] State update completed.")

    async def notify_rooms_of_stream_online(
        self, domain: str, name: str, title: str, title_change: bool, tags: list[str]
    ) -> None:
        """
        Sends notifications to rooms with subscriptions to the provided stream domain.

        :param domain: The domain of the stream to send notifications for.
        :param title: The title of the stream to include in the message.
        :param renamed: Whether or not this is for a stream changing its title rather than going live.
        :return: Nothing.
        """

        # Has enough time passed since the last notification was sent?
        if domain in self.notification_timers_cache:
            seconds_since_last_notification = round(
                time.time() - self.notification_timers_cache[domain]
            )
            if seconds_since_last_notification < SECONDS_BETWEEN_NOTIFICATIONS:
                self.log.info(
                    f"[{domain}] Not sending notifications. Only {seconds_since_last_notification} of required {SECONDS_BETWEEN_NOTIFICATIONS} seconds has passed since last notification."
                )
                return

        # Yes. Log the current time and proceed with sending notifications.
        self.notification_timers_cache[domain] = time.time()

        # Get a list of room IDs with active subscriptions to the stream domain.
        query = "SELECT room_id FROM subscriptions WHERE stream_domain=$1"
        async with self.database.acquire() as connection:
            results = await connection.fetch(query, domain)

        # Set up some counters for a high level statistics.
        successful_notifications = 0
        failed_notifications = 0

        # Time to start building the body text for the notifications.

        # Turns out it is possible to set an empty name for an Owncast stream if you know how.
        # We'll account for that... Just in case.
        stream_name = name if name else domain

        # If the stream changed its title, send a different title change notification.
        if title_change:
            self.log.info(f"[{domain}] Sending title change notifications...")
            body_text = "📝 " + name + " has changed its stream title!"
        else:
            self.log.info(f"[{domain}] Sending going live notifications...")
            body_text = "🎥 " + name + " is now live!"

        # Streams can have no title. If there is none, don't even mention it.
        if title != "":
            body_text += "\nStream Title: " + title

        body_text += "\n\nTo tune in, visit: https://" + domain + "/"

        if "tags" and len(tags) > 0:
            body_text += "\n\n"
            body_text += " ".join("#" + tag for tag in tags)

        # Iterate over the subscribed rooms and try to send a message to each.
        # TODO: This should probably be made async.
        for rows in results:
            room_id = rows["room_id"]
            try:
                # Try and send a message.
                content = TextMessageEventContent(
                    msgtype=MessageType.TEXT, body=body_text
                )
                await self.client.send_message(room_id, content)

                # It worked! Increment the counter.
                successful_notifications += 1
            except Exception as exception:
                # Something didn't work. Log it and move on to the next one.
                self.log.warning(
                    f"[{domain}] Failed to send notification message to room [{room_id}]: {exception}"
                )
                failed_notifications += 1

        # All done!
        self.log.info(
            f"[{domain}] Completed sending notifications! {successful_notifications} succeeded, {failed_notifications} failed."
        )

    # ========== HELPER METHODS ==========
    async def get_stream_state(self, domain):
        """
        Get the current stream state for a given domain.
        HTTPS on port 443 is assumed, no other protocols or ports are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A dictionary containing the lastConnectTime, lastDisconnectTime, streamTitle, and online values if available. An empty dictionary if an error occurred.
        """
        self.log.debug(f"[{domain}] Fetching current stream state...")
        # Build a URL to the status API in Owncast. (https://owncast.online/api/latest/#tag/Internal/operation/GetStatus)
        # Only use HTTPS, even if the user specified something else.
        status_url = "https://" + domain + OWNCAST_STATUS_PATH

        # Make a request to the endpoint.
        try:
            response = await self.session.request(
                "GET", status_url, allow_redirects=False
            )
        except Exception as e:
            self.log.warning(
                f"[{domain}] Error making GET request to {OWNCAST_STATUS_PATH}: {e}"
            )
            return {}

        # Check the response code is success
        # TODO: Handle 429 rate limiting?
        if response.status != 200:
            self.log.warning(
                f"[{domain}] Response to request on {OWNCAST_STATUS_PATH} was not 200, got {response.status} instead."
            )
            return {}

        # Try and interpret the response as JSON.
        try:
            new_state = json.loads(await response.read())
        except Exception as e:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as could not be interpreted as JSON: {e}"
            )
            return {}

        # Validate the response to ensure it contains all the basic info needed to function.
        if "lastConnectTime" not in new_state:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as it does not have last connect time parameter."
            )
            return {}
        elif "lastDisconnectTime" not in new_state:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as it does not have last disconnect time parameter."
            )
            return {}
        elif "streamTitle" not in new_state:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as it does not have stream title parameter."
            )
            return {}
        elif "online" not in new_state:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as it does not have online status parameter."
            )
            return {}

        return new_state

    async def get_stream_config(self, domain):
        """
        Get the current stream config for a given domain.
        HTTPS on port 443 is assumed, no other protocols or ports are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A dictionary containing the stream's configuration.
        """
        self.log.debug(f"[{domain}] Fetching current stream config...")
        # Build a URL to the config API in Owncast. (https://owncast.online/api/latest/#tag/Internal/operation/GetWebConfig)
        # Only use HTTPS, even if the user specified something else.
        status_url = "https://" + domain + OWNCAST_CONFIG_PATH

        # Make a request to the endpoint.
        try:
            response = await self.session.request(
                "GET", status_url, allow_redirects=False
            )
        except Exception as e:
            self.log.warning(
                f"[{domain}] Error making GET request to {OWNCAST_STATUS_PATH}: {e}"
            )
            return {}

        # Check the response code is success
        # TODO: Handle 429 rate limiting?
        if response.status != 200:
            self.log.warning(
                f"[{domain}] Response to request on {OWNCAST_STATUS_PATH} was not 200, got {response.status} instead."
            )
            return {}

        # Try and interpret the response as JSON.
        try:
            config = json.loads(await response.read())
        except Exception as e:
            self.log.warning(
                f"[{domain}] Rejecting response to request on {OWNCAST_STATUS_PATH} as could not be interpreted as JSON: {e}"
            )
            return {}

        return config

    def domainify(self, url) -> str:
        """
        Take a given URL and convert it to just the domain.

        :param url:
        :return:
        """
        # Take whatever input the user provided and try to turn it into just the domain.
        # Examples:
        # "stream.logal.dev" -> "stream.logal.dev"
        # "https://stream.logal.dev" -> "stream.logal.dev"
        # "stream.logal.dev/embed/chat/readwrite" -> "stream.logal.dev"
        # "https://stream.logal.dev/abcdefghijklmno/123456789" -> "stream.logal.dev"
        # "notify@stream.logal.dev" -> "stream.logal.dev"

        parsed_url = urlparse(url)
        domain = (parsed_url.netloc or parsed_url.path).lower()

        if "@" in domain:
            return domain.split("@")[-1]

        return domain
