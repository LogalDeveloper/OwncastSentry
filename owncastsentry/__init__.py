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

"""OwncastSentry maubot plugin."""

from typing import TYPE_CHECKING

from maubot import MessageEvent, Plugin  # type: ignore[attr-defined]
from maubot.handlers import command

from .commands import CommandHandler
from .config import Config
from .database import StreamRepository, SubscriptionRepository
from .health_checker import HealthChecker
from .migrations import get_upgrade_table
from .notification_service import NotificationService
from .owncast_client import OwncastClient
from .stream_monitor import StreamMonitor

if TYPE_CHECKING:
    from mautrix.util.async_db import Database, UpgradeTable
    from mautrix.util.config import BaseProxyConfig


class OwncastSentry(Plugin):
    """Main plugin class for OwncastSentry."""

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable | None:
        """Return the database upgrade table for Maubot."""
        return get_upgrade_table()

    @classmethod
    def get_config_class(cls) -> type[BaseProxyConfig]:
        """Return the configuration class for Maubot."""
        return Config

    async def start(self) -> None:
        """Initialize all services and register recurring tasks.

        Registers a recurring task every minute to update the state of
        all subscribed streams.
        """
        # Load configuration
        config: Config = self.config  # type: ignore[assignment]
        config.load_and_update()
        db: Database = self.database  # type: ignore[assignment]

        # Initialize the Owncast API client
        self.owncast_client = OwncastClient(self.log, str(self.loader.meta.version))

        # Initialize repositories
        self.stream_repo = StreamRepository(db)
        self.subscription_repo = SubscriptionRepository(db)

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

        # Initialize health checker
        self.health_checker = HealthChecker(
            db,
            self.owncast_client,
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
        """Update all stream states and perform health check."""
        # Get list of all stream domains with active subscriptions
        subscribed_domains = await self.subscription_repo.get_all_subscribed_domains()

        # Delegate to stream monitor and get results
        update_result = await self.stream_monitor.update_all_streams(subscribed_domains)

        # Perform health check
        config: Config = self.config  # type: ignore[assignment]
        await self.health_checker.perform_health_check(
            update_result,
            config.health_check_endpoint,
        )

    @command.new(help="Subscribes to a new Owncast stream.")
    @command.argument("url")
    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """Delegate subscribe command to CommandHandler."""
        await self.command_handler.subscribe(evt, url)

    @command.new(help="Unsubscribes from an Owncast stream.")
    @command.argument("url")
    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """Delegate unsubscribe command to CommandHandler."""
        await self.command_handler.unsubscribe(evt, url)

    @command.new(help="Lists all stream subscriptions in this room.")
    async def subscriptions(self, evt: MessageEvent) -> None:
        """Delegate subscriptions command to CommandHandler."""
        await self.command_handler.subscriptions(evt)

    @command.new(help="Lists currently live streams in this room.")
    async def live(self, evt: MessageEvent) -> None:
        """Delegate live command to CommandHandler."""
        await self.command_handler.live(evt)

    async def stop(self) -> None:
        """Clean up resources by closing the HTTP session."""
        await self.owncast_client.close()
