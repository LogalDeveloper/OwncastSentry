# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import sqlite3
from datetime import datetime, timezone
from maubot import MessageEvent
from mautrix.types import TextMessageEventContent, MessageType

from .owncast_client import OwncastClient
from .database import StreamRepository, SubscriptionRepository
from .utils import domainify


class CommandHandler:
    """Handles bot commands for subscribing to streams."""

    def __init__(
        self,
        owncast_client: OwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
        logger,
    ):
        """
        Initialize the command handler.

        :param owncast_client: Client for making API calls to Owncast instances
        :param stream_repo: Repository for stream data
        :param subscription_repo: Repository for subscription data
        :param logger: Logger instance
        """
        self.owncast_client = owncast_client
        self.stream_repo = stream_repo
        self.subscription_repo = subscription_repo
        self.log = logger

    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """
        "!subscribe" command handler for users to subscribe a room to a given stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and subscribe to.
        :return: Nothing.
        """
        # Convert the user input to only a domain
        stream_domain = domainify(url)

        # How many subscriptions already exist for this domain?
        subscription_count = await self.subscription_repo.count_by_domain(stream_domain)

        if subscription_count == 0:
            # There are 0 subscriptions, we need to validate this domain is an Owncast stream.
            is_valid = await self.owncast_client.validate_instance(stream_domain)
            if not is_valid:
                # The stream state fetch returned nothing. Probably not an Owncast stream.
                await evt.reply(
                    "The URL you supplied does not appear to be a valid Owncast instance. You may have specified an invalid domain, or the instance is offline."
                )
                return

        # Try to add a new subscription for the requested stream domain in the room the command was executed in
        try:
            await self.subscription_repo.add(stream_domain, evt.room_id)
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
                    f"[{stream_domain}] An error occurred while attempting to add subscription in room {evt.room_id}: {exception}"
                )
                raise exception

        # The subscription was successfully added! Try to add a placeholder row for the stream's state in the streams table.
        try:
            await self.stream_repo.create(stream_domain)
            # The insert was successful, so this is the first time we're seeing this stream. Log it.
            self.log.info(f"[{stream_domain}] Discovered new stream!")
        except sqlite3.IntegrityError as exception:
            # Attempts to add rows for streams already known is an expected condition. What is anything except that?
            if "UNIQUE constraint failed" not in exception.args[0]:
                # Something unexpected happened. Give up.
                self.log.error(
                    f"[{stream_domain}] An error occurred while attempting to add stream information after adding subscription: {exception}"
                )
                raise exception

        # All went well! We added a new subscription and (at least tried) to add a row for the stream state. Tell the user.
        self.log.info(f"[{stream_domain}] Subscription added for room {evt.room_id}.")
        await evt.reply(
            "Subscription added! This room will receive notifications when "
            + stream_domain
            + " goes live."
        )

    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """
        "!unsubscribe" command handler for users to unsubscribe a room from a given stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and unsubscribe from.
        :return: Nothing.
        """
        # Convert the user input to only a domain
        stream_domain = domainify(url)

        # Attempt to delete the requested subscription from the database
        result = await self.subscription_repo.remove(stream_domain, evt.room_id)

        # Did it work?
        if result == 1:
            # Yes, one row was deleted. Tell the user.
            self.log.info(
                f"[{stream_domain}] Subscription removed for room {evt.room_id}."
            )
            await evt.reply(
                "Subscription removed! This room will no longer receive notifications for "
                + stream_domain
                + "."
            )
        elif result == 0:
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
                + str(result)
                + " instead. Something very bad may have happened!!!!"
            )

    def _format_duration(self, timestamp_str: str) -> str:
        """
        Calculate and format the duration from a timestamp to now.

        :param timestamp_str: ISO 8601 timestamp string
        :return: Formatted duration string (e.g., "1 hour", "2 days")
        """
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            delta = now - timestamp

            seconds = int(delta.total_seconds())
            if seconds < 60:
                return f"{seconds} second{'s' if seconds != 1 else ''}"
            elif seconds < 3600:
                minutes = seconds // 60
                return f"{minutes} minute{'s' if minutes != 1 else ''}"
            elif seconds < 86400:
                hours = seconds // 3600
                return f"{hours} hour{'s' if hours != 1 else ''}"
            else:
                days = seconds // 86400
                return f"{days} day{'s' if days != 1 else ''}"
        except Exception:
            return "unknown duration"

    async def subscriptions(self, evt: MessageEvent) -> None:
        """
        "!subscriptions" command handler for listing all stream subscriptions in the current room.

        :param evt: MessageEvent of the message calling the command.
        :return: Nothing.
        """
        # Get all stream domains this room is subscribed to
        subscribed_domains = await self.subscription_repo.get_subscribed_streams_for_room(
            evt.room_id
        )

        # Check if there are no subscriptions
        if not subscribed_domains:
            await evt.reply("This room is not subscribed to any Owncast instances.\n\nTo subscribe to an Owncast instance, use `!subscribe <domain>`", markdown=True)
            return

        # Build the response message body as Markdown
        body_text = f"**Subscriptions for this room ({len(subscribed_domains)}):**\n\n"

        for domain in subscribed_domains:
            # Get the stream state from the database
            stream_state = await self.stream_repo.get_by_domain(domain)

            if stream_state is None:
                # Stream exists in subscriptions but not in streams table (shouldn't happen)
                body_text += f"- **{domain}**  \n"
                body_text += f"  - Status: Unknown  \n"
                body_text += f"  - Link: https://{domain}\n\n"
                continue

            # Determine stream name (use domain as fallback)
            stream_name = stream_state.name if stream_state.name else domain

            # Start building this stream's entry with stream name as main bullet
            body_text += f"- **{stream_name}**  \n"

            # Add title if stream is online (as a sub-bullet)
            if stream_state.online and stream_state.title:
                body_text += f"  - Title: {stream_state.title}  \n"

            # Determine status and duration (as a sub-bullet)
            if stream_state.online:
                # Stream is online - use last_connect_time
                if stream_state.last_connect_time:
                    duration = self._format_duration(stream_state.last_connect_time)
                    body_text += f"  - Status: Online for {duration}  \n"
                else:
                    body_text += f"  - Status: Online  \n"
            else:
                # Stream is offline - use last_disconnect_time
                if stream_state.last_disconnect_time:
                    duration = self._format_duration(stream_state.last_disconnect_time)
                    body_text += f"  - Status: Offline for {duration}  \n"
                else:
                    body_text += f"  - Status: Offline  \n"

            # Add stream link (as a sub-bullet)
            body_text += f"  - Link: https://{domain}\n\n"

        # Add help text for unsubscribing
        body_text += "\nTo unsubscribe from any of these Owncast instances, use `!unsubscribe <domain>`"

        # Send the response as Markdown
        await evt.reply(body_text, markdown=True)

    async def live(self, evt: MessageEvent) -> None:
        """
        "!live" command handler for listing only currently live streams in the current room.

        :param evt: MessageEvent of the message calling the command.
        :return: Nothing.
        """
        # Get all stream domains this room is subscribed to
        subscribed_domains = await self.subscription_repo.get_subscribed_streams_for_room(
            evt.room_id
        )

        # Check if there are no subscriptions
        if not subscribed_domains:
            await evt.reply("This room is not subscribed to any Owncast instances.\n\nTo subscribe to an Owncast instance, use `!subscribe <domain>`", markdown=True)
            return

        # Filter for only live streams
        live_streams = []
        for domain in subscribed_domains:
            stream_state = await self.stream_repo.get_by_domain(domain)
            if stream_state and stream_state.online:
                live_streams.append((domain, stream_state))

        # Check if there are no live streams
        if not live_streams:
            await evt.reply("No subscribed Owncast instances are currently live.\n\nUse `!subscriptions` to list all subscriptions.", markdown=True)
            return

        # Build the response message body as Markdown
        body_text = f"**Live Owncast instances ({len(live_streams)}):**\n\n"

        for domain, stream_state in live_streams:
            # Determine stream name (use domain as fallback)
            stream_name = stream_state.name if stream_state.name else domain

            # Start building this stream's entry with stream name as main bullet
            body_text += f"- **{stream_name}**  \n"

            # Add title (should be present for live streams)
            if stream_state.title:
                body_text += f"  - Title: {stream_state.title}  \n"

            # Add status with duration
            if stream_state.last_connect_time:
                duration = self._format_duration(stream_state.last_connect_time)
                body_text += f"  - Online for {duration}  \n"

            # Add stream link
            body_text += f"  - Link: https://{domain}\n\n"

        # Send the response as Markdown
        await evt.reply(body_text.rstrip(), markdown=True)
