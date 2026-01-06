# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import sqlite3
from maubot import MessageEvent

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
