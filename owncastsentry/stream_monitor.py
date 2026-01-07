# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import asyncio
import time

from .owncast_client import OwncastClient
from .database import StreamRepository, SubscriptionRepository
from .notification_service import NotificationService
from .models import StreamState
from .utils import (
    TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN,
    CLEANUP_WARNING_THRESHOLD,
    CLEANUP_DELETE_THRESHOLD,
    should_query_stream,
)


class StreamMonitor:
    """Monitors Owncast streams and detects state changes."""

    def __init__(
        self,
        owncast_client: OwncastClient,
        stream_repo: StreamRepository,
        notification_service: NotificationService,
        logger,
    ):
        """
        Initialize the stream monitor.

        :param owncast_client: Client for making API calls to Owncast instances
        :param stream_repo: Repository for stream data
        :param notification_service: Service for sending notifications
        :param logger: Logger instance
        """
        self.owncast_client = owncast_client
        self.stream_repo = stream_repo
        self.notification_service = notification_service
        self.log = logger

        # Cache for tracking when streams last went offline
        self.offline_timer_cache = {}

    async def update_all_streams(self, subscribed_domains: list[str]) -> None:
        """
        Checks the status of all streams with active subscriptions.
        Updates for all streams are performed asynchronously, with the method returning when the slowest update completes.

        :param subscribed_domains: List of stream domains to update
        :return: Nothing.
        """
        self.log.debug("Updating all stream states...")

        # Build a list of async tasks which update the state for each stream domain
        tasks = []
        for domain in subscribed_domains:
            tasks.append(asyncio.create_task(self.update_stream(domain)))

        # Run the tasks in parallel
        await asyncio.gather(*tasks)
        self.log.debug("Update complete.")

    async def update_stream(self, domain: str) -> None:
        """
        Updates the state of a given stream domain and sends notifications to subscribed Matrix rooms if it goes live.
        Implements progressive backoff for connection failures and auto-cleanup for dead instances.

        :param domain: The domain of the stream to update.
        :return: Nothing.
        """
        # Fetch the current stream state from database to check failure_counter
        old_state = await self.stream_repo.get_by_domain(domain)
        failure_counter = old_state.failure_counter if old_state else 0

        # Check if we should query this stream based on backoff schedule
        if not should_query_stream(failure_counter):
            # Skip this cycle, increment counter to track time passage
            await self.stream_repo.increment_failure_counter(domain)
            self.log.debug(
                f"[{domain}] Skipping query due to backoff (counter={failure_counter + 1})"
            )
            # Check cleanup thresholds even when skipping query
            await self._check_cleanup_thresholds(domain, failure_counter + 1)
            return

        # A flag indicating whether this is the first state update of a brand-new stream to avoid sending notifications if its already live.
        first_update = False

        # A flag indicating whether to update the stream's state in the database.
        # Used to avoid writing to the database when a stream's state hasn't changed at all.
        update_database = False

        # Holds the stream's latest configuration, if fetched and deemed necessary during the update process.
        stream_config = None

        # Fetch the latest stream state from the server
        new_state = await self.owncast_client.get_stream_state(domain)

        # If the fetch failed, increment failure counter and skip the update
        if new_state is None:
            await self.stream_repo.increment_failure_counter(domain)
            self.log.warning(
                f"[{domain}] Connection failure (counter={failure_counter + 1})"
            )
            # Check cleanup thresholds after connection failure
            await self._check_cleanup_thresholds(domain, failure_counter + 1)
            return

        # Fetch succeeded! Reset failure counter
        await self.stream_repo.reset_failure_counter(domain)

        # Fix possible race conditions with timers
        if domain not in self.offline_timer_cache:
            self.offline_timer_cache[domain] = 0
        if domain not in self.notification_service.notification_timers_cache:
            self.notification_service.notification_timers_cache[domain] = 0

        # Does the last known stream state not have a value for the last connect and disconnect time?
        if (
            old_state.last_connect_time is None
            and old_state.last_disconnect_time is None
        ):
            # Yes, this is the first update. Don't send any notifications.
            update_database = True
            first_update = True

        # Does the latest stream state have a last connect time and the old state not have one?
        if (
            new_state.last_connect_time is not None
            and old_state.last_connect_time is None
        ):
            # Yes! This stream is now live.
            update_database = True
            stream_config = await self.owncast_client.get_stream_config(domain)

            self.log.info(f"[{domain}] Stream is now live!")

            # Calculate how many seconds since the stream last went offline
            seconds_since_last_offline = round(
                time.time() - self.offline_timer_cache[domain]
            )

            # Have we queried this stream before? (In other words, is this not the first state update ever?)
            if not first_update:
                # Use fallback values if config fetch failed
                stream_name = stream_config.name if stream_config else domain
                stream_tags = stream_config.tags if stream_config else []

                # Yes. Has this stream been offline for a short amount of time?
                if seconds_since_last_offline < TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN:
                    # Yes. Did the stream title change?
                    if old_state.title != new_state.title:
                        # Yes. The stream was only down for a short time, send a special notification indicating the stream changed its name.
                        await self.notification_service.notify_stream_live(
                            domain,
                            stream_name,
                            new_state.title,
                            stream_tags,
                            title_change=True,
                        )
                    else:
                        # No. The stream was only down for a short time and didn't change its title. Don't send a notification.
                        self.log.info(
                            f"[{domain}] Not sending notifications. Stream was only offline for {seconds_since_last_offline} of {TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN} seconds and did not change its title."
                        )
                else:
                    # This stream has been offline for a while. Send a normal notification.
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title,
                        stream_tags,
                        title_change=False,
                    )
            else:
                # No, this is the first time we're querying
                self.log.info(
                    f"[{domain}] Not sending notifications. This is the first state update for this stream."
                )

        if (
            new_state.last_connect_time is not None
            and old_state.last_connect_time is not None
        ):
            # Did the stream title change mid-session?
            if old_state.title != new_state.title:
                self.log.info(f"[{domain}] Stream title was changed!")
                update_database = True
                stream_config = await self.owncast_client.get_stream_config(domain)

                # Use fallback values if config fetch failed
                stream_name = stream_config.name if stream_config else domain
                stream_tags = stream_config.tags if stream_config else []

                # This is a fun case to account for... Let's try and explain this.
                # Was the last time this stream sent a notification before it last went offline?
                if (
                    self.offline_timer_cache[domain]
                    > self.notification_service.notification_timers_cache[domain]
                ):
                    # Yes. Send a regular go live notification.
                    # Why? A title change notification could be confusing to users in this case.
                    # How? If a stream goes offline before its next allowed notification, it'll get rate limited. If it then changes its title, this part of the code will send a title change notification. This can be a little confusing, so override to a normal go live notification in this case.
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title,
                        stream_tags,
                        title_change=False,
                    )
                else:
                    # No. Send a normal title change notification.
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title,
                        stream_tags,
                        title_change=True,
                    )

        # Does the latest stream state no longer have a last connect time but the old state does?
        elif (
            new_state.last_connect_time is None
            and old_state.last_connect_time is not None
        ):
            # Yep. This stream is now offline. Log it.
            update_database = True
            self.offline_timer_cache[domain] = time.time()
            if first_update:
                self.log.info(f"[{domain}] Stream is offline.")
            else:
                self.log.info(f"[{domain}] Stream is now offline.")

        # Update the database with the current stream state, if needed.
        if update_database:
            # Ensure we have the stream config before updating the database
            if stream_config is None:
                stream_config = await self.owncast_client.get_stream_config(domain)

            # Use fallback value if config fetch failed
            stream_name = stream_config.name if stream_config else ""

            self.log.debug(f"[{domain}] Updating stream state in database...")

            # Create updated state object (title already truncated in new_state)
            updated_state = StreamState(
                domain=domain,
                name=stream_name,
                title=new_state.title,
                last_connect_time=new_state.last_connect_time,
                last_disconnect_time=new_state.last_disconnect_time,
            )

            await self.stream_repo.update(updated_state)

        # All done.
        self.log.debug(f"[{domain}] State update completed.")

    async def _check_cleanup_thresholds(self, domain: str, counter: int) -> None:
        """
        Check if a domain has hit cleanup warning or deletion thresholds.

        :param domain: The domain to check
        :param counter: The current failure counter value
        :return: Nothing
        """
        # Check for 83-day warning threshold
        if counter == CLEANUP_WARNING_THRESHOLD:
            self.log.warning(
                f"[{domain}] Reached 83-day warning threshold. Sending cleanup warning."
            )
            await self.notification_service.send_cleanup_warning(domain)

        # Check for 90-day deletion threshold
        if counter >= CLEANUP_DELETE_THRESHOLD:
            self.log.warning(
                f"[{domain}] Reached 90-day deletion threshold. Removing all subscriptions."
            )
            # Send deletion notification
            await self.notification_service.send_cleanup_deletion(domain)

            # Delete all subscriptions for this domain
            from .database import SubscriptionRepository

            subscription_repo = SubscriptionRepository(self.stream_repo.db)
            deleted_count = await subscription_repo.delete_all_for_domain(domain)

            # Delete the stream record
            await self.stream_repo.delete(domain)

            self.log.info(
                f"[{domain}] Cleanup complete. Deleted {deleted_count} subscriptions and stream record."
            )
