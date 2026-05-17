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

from aiohttp import web
from maubot import MessageEvent, Plugin  # type: ignore[attr-defined]
from maubot.handlers import command
from prometheus_client.exposition import choose_encoder

from .commands import CommandHandler
from .config import Config
from .metrics import ErrorSource, MetricsService
from .notification_service import NotificationService
from .owncast_client import OwncastClient
from .repository import StreamRepository, SubscriptionRepository, get_upgrade_table
from .stream_monitor import StreamMonitor
from .subscription_manager import SubscriptionManager

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

        # Initialize metrics service and register web endpoint (if enabled)
        self.metrics_service = MetricsService()
        self.metrics_service.set_build_info(str(self.loader.meta.version))
        if config.metrics_enabled and self.webapp is not None:
            self.webapp.add_route(
                method="GET", path="/metrics", handler=self._metrics_endpoint
            )

        # Initialize the Owncast API client
        self.owncast_client = OwncastClient(
            self.log, str(self.loader.meta.version), metrics=self.metrics_service
        )
        self.metrics_service.register_open_connections_gauge(
            lambda: self.owncast_client.open_connection_count
        )

        # Initialize repositories
        self.stream_repo = StreamRepository(db)
        self.subscription_repo = SubscriptionRepository(db)

        # Initialize notification service
        self.notification_service = NotificationService(
            self.client,
            self.subscription_repo,
            self.log,
            metrics=self.metrics_service,
        )

        # Initialize stream monitor
        self.stream_monitor = StreamMonitor(
            self.owncast_client,
            self.stream_repo,
            self.subscription_repo,
            self.notification_service,
            self.log,
            metrics=self.metrics_service,
        )

        # Initialize subscription manager
        self.subscription_manager = SubscriptionManager(
            self.owncast_client,
            self.stream_repo,
            self.subscription_repo,
            self.log,
        )

        # Initialize command handler
        self.command_handler = CommandHandler(
            self.subscription_manager,
        )

        # Schedule periodic stream state updates every 60 seconds
        self.sched.run_periodically(60, self._update_all_stream_states)

    async def _update_all_stream_states(self) -> None:
        """Update all stream states."""
        try:
            # Get list of all stream domains with active subscriptions
            subscribed_domains = (
                await self.subscription_repo.get_all_subscribed_domains()
            )

            # Delegate to stream monitor
            await self.stream_monitor.update_all_streams(subscribed_domains)
        except Exception:
            self.metrics_service.record_error(ErrorSource.SCHEDULER_LOOP)
            self.log.exception("Unhandled exception in scheduler loop.")

    @command.new(help="Subscribes to a new Owncast stream.")
    @command.argument("url")
    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """Delegate subscribe command to CommandHandler."""
        try:
            await self.command_handler.subscribe(evt, url)
        except Exception:
            self.metrics_service.record_error(ErrorSource.COMMAND)
            self.log.exception("Unhandled exception in subscribe command.")
            await evt.reply("An unexpected error occurred. Please try again later.")

    @command.new(help="Unsubscribes from an Owncast stream.")
    @command.argument("url")
    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """Delegate unsubscribe command to CommandHandler."""
        try:
            await self.command_handler.unsubscribe(evt, url)
        except Exception:
            self.metrics_service.record_error(ErrorSource.COMMAND)
            self.log.exception("Unhandled exception in unsubscribe command.")
            await evt.reply("An unexpected error occurred. Please try again later.")

    @command.new(help="Lists all stream subscriptions in this room.")
    async def subscriptions(self, evt: MessageEvent) -> None:
        """Delegate subscriptions command to CommandHandler."""
        try:
            await self.command_handler.subscriptions(evt)
        except Exception:
            self.metrics_service.record_error(ErrorSource.COMMAND)
            self.log.exception("Unhandled exception in subscriptions command.")
            await evt.reply("An unexpected error occurred. Please try again later.")

    @command.new(help="Lists currently live streams in this room.")
    async def live(self, evt: MessageEvent) -> None:
        """Delegate live command to CommandHandler."""
        try:
            await self.command_handler.live(evt)
        except Exception:
            self.metrics_service.record_error(ErrorSource.COMMAND)
            self.log.exception("Unhandled exception in live command.")
            await evt.reply("An unexpected error occurred. Please try again later.")

    async def _metrics_endpoint(self, request: web.Request) -> web.Response:
        """Serve Prometheus metrics."""
        accept = request.headers.get("Accept", "")
        encoder, content_type = choose_encoder(accept)
        output = encoder(self.metrics_service.registry)
        response = web.Response(body=output)
        response.headers["Content-Type"] = content_type
        return response

    async def stop(self) -> None:
        """Clean up resources by closing the HTTP session."""
        await self.owncast_client.close()
