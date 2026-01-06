# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

from maubot import Plugin, MessageEvent
from maubot.handlers import command
from mautrix.util.async_db import UpgradeTable

from .migrations import get_upgrade_table
from .owncast_client import OwncastClient
from .database import StreamRepository, SubscriptionRepository
from .notification_service import NotificationService
from .stream_monitor import StreamMonitor
from .commands import CommandHandler


class OwncastSentry(Plugin):
    """Main plugin class for OwncastSentry."""

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable | None:
        """
        Helper method for telling Maubot about our database migrations.

        :return: An UpgradeTable with our registered migrations.
        """
        return get_upgrade_table()

    async def start(self) -> None:
        """
        Method called by Maubot upon startup of the instance.
        Initializes all services and registers a recurring task every minute to update the state of all subscribed streams.

        :return: Nothing.
        """
        # Initialize the Owncast API client
        self.owncast_client = OwncastClient(self.log)

        # Initialize repositories
        self.stream_repo = StreamRepository(self.database)
        self.subscription_repo = SubscriptionRepository(self.database)

        # Initialize notification service
        self.notification_service = NotificationService(
            self.client, self.subscription_repo, self.log
        )

        # Initialize stream monitor
        self.stream_monitor = StreamMonitor(
            self.owncast_client,
            self.stream_repo,
            self.notification_service,
            self.log,
        )

        # Initialize command handler
        self.command_handler = CommandHandler(
            self.owncast_client,
            self.stream_repo,
            self.subscription_repo,
            self.log,
        )

        # Schedule periodic stream state updates every 60 seconds
        self.sched.run_periodically(60, self._update_all_stream_states)

    async def _update_all_stream_states(self) -> None:
        """
        Wrapper method for updating all stream states.
        Fetches list of subscribed domains and delegates to StreamMonitor.

        :return: Nothing.
        """
        # Get list of all stream domains with active subscriptions
        subscribed_domains = await self.subscription_repo.get_all_subscribed_domains()

        # Delegate to stream monitor
        await self.stream_monitor.update_all_streams(subscribed_domains)

    @command.new(help="Subscribes to a new Owncast stream.")
    @command.argument("url")
    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """
        Command handler that delegates to CommandHandler.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and subscribe to.
        :return: Nothing.
        """
        await self.command_handler.subscribe(evt, url)

    @command.new(help="Unsubscribes from an Owncast stream.")
    @command.argument("url")
    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """
        Command handler that delegates to CommandHandler.

        :param evt: MessageEvent of the message calling the command.
        :param url: A string containing the user supplied URL to a stream to try and unsubscribe from.
        :return: Nothing.
        """
        await self.command_handler.unsubscribe(evt, url)

    async def stop(self) -> None:
        """
        Method called by Maubot upon shutdown of the instance.
        Closes the HTTP session.

        :return: Nothing.
        """
        await self.owncast_client.close()
