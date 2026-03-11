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

"""Stream monitoring service for OwncastSentry."""

import asyncio
import time
from typing import TYPE_CHECKING

from .health_checker import UpdateResult
from .models import StreamState
from .utils import (
    CLEANUP_DELETE_THRESHOLD,
    CLEANUP_WARNING_THRESHOLD,
    TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN,
    should_query_stream,
)

if TYPE_CHECKING:
    import logging

    from .database import StreamRepository, SubscriptionRepository
    from .notification_service import NotificationService
    from .owncast_client import OwncastClient


class StreamMonitor:
    """Monitors Owncast streams and detects state changes."""

    def __init__(
        self,
        owncast_client: OwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
        notification_service: NotificationService,
        logger: logging.Logger,
    ) -> None:
        """Initialize the stream monitor.

        :param owncast_client: Client for making API calls to Owncast instances.
        :param stream_repo: Repository for stream data.
        :param subscription_repo: Repository for subscription data.
        :param notification_service: Service for sending notifications.
        :param logger: Logger instance for debugging.
        """
        self.owncast_client = owncast_client
        self.stream_repo = stream_repo
        self.subscription_repo = subscription_repo
        self.notification_service = notification_service
        self.log = logger

        # Cache for tracking when streams last went offline
        self.offline_timer_cache: dict[str, float] = {}

    async def update_all_streams(self, subscribed_domains: list[str]) -> UpdateResult:
        """Check the status of all streams with active subscriptions.

        Updates for all streams are performed asynchronously, with the
        method returning when the slowest update completes.

        :param subscribed_domains: List of stream domains to update.
        :return: UpdateResult with success/failure counts.
        """
        self.log.debug("Updating all stream states...")

        total_streams = len(subscribed_domains)

        # Build a list of async tasks for each stream domain
        tasks = [
            asyncio.create_task(self.update_stream(domain))
            for domain in subscribed_domains
        ]

        # Run the tasks in parallel and collect results
        results = await asyncio.gather(*tasks)

        # Count successes and failures
        successful_checks = sum(1 for result in results if result is True)
        failed_checks = sum(1 for result in results if result is False)

        self.log.debug(
            f"Update complete. {successful_checks}/{total_streams} succeeded, "
            f"{failed_checks} failed."
        )

        return UpdateResult(
            total_streams=total_streams,
            successful_checks=successful_checks,
            failed_checks=failed_checks,
        )

    async def update_stream(self, domain: str) -> bool:
        """Update the state of a stream and send notifications as needed.

        Sends notifications to subscribed Matrix rooms if a stream goes
        live. Implements progressive backoff for connection failures and
        auto-cleanup for dead instances.

        :param domain: The domain of the stream to update.
        :return: True if check succeeded or was skipped, False on failure.
        """
        # Fetch the current stream state from database to check failure_counter
        old_state = await self.stream_repo.get_by_domain(domain)
        failure_counter = old_state.failure_counter if old_state else 0

        # Check if we should query this stream based on backoff schedule
        if not should_query_stream(failure_counter):
            # Skip this cycle, increment counter to track time passage
            await self.stream_repo.increment_failure_counter(domain)
            self.log.debug(
                f"[{domain}] Skipping query due to backoff "
                f"(counter={failure_counter + 1})"
            )
            # Check cleanup thresholds even when skipping query
            await self._check_cleanup_thresholds(domain, failure_counter + 1)
            # Backoff is expected behavior, not a failure
            return True

        # Defensive check: old_state should always exist here since
        # the stream is in the DB
        if old_state is None:
            return False

        # Flag: first state update of a brand-new stream to avoid
        # sending notifications if it's already live.
        first_update = False

        # Flag: whether to update the stream's state in the database.
        # Used to avoid writes when state hasn't changed at all.
        update_database = False

        # The stream's latest configuration, if fetched during update.
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
            # Actual connection failure
            return False

        # Fetch succeeded! Reset failure counter
        await self.stream_repo.reset_failure_counter(domain)

        # Initialize timer cache entries to prevent KeyError on first access
        if domain not in self.offline_timer_cache:
            self.offline_timer_cache[domain] = 0

        # Does the last known stream state lack connect/disconnect?
        if (
            old_state.last_connect_time is None
            and old_state.last_disconnect_time is None
        ):
            # Yes, this is the first update. Don't send any notifications.
            update_database = True
            first_update = True

        # Does the new state have a connect time but the old one not?
        if (
            new_state.last_connect_time is not None
            and old_state.last_connect_time is None
        ):
            # Yes! This stream is now live.
            update_database = True
            stream_config = await self.owncast_client.get_stream_config(domain)

            self.log.info(f"[{domain}] Stream is now live!")

            # Calculate seconds since the stream last went offline
            seconds_since_last_offline = round(
                time.time() - self.offline_timer_cache[domain]
            )

            # Have we queried this stream before?
            if not first_update:
                # Use fallback values if config fetch failed
                stream_name = stream_config.name if stream_config else domain
                stream_tags = stream_config.tags if stream_config else []

                # Has this stream been offline for a short time?
                if seconds_since_last_offline < TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN:
                    # Did the stream title change?
                    if old_state.title != new_state.title:
                        # Stream was briefly down; send title
                        # change notification.
                        await self.notification_service.notify_stream_live(
                            domain,
                            stream_name,
                            new_state.title or "",
                            stream_tags,
                            title_change=True,
                        )
                    else:
                        # Briefly offline, no title change. Skip.
                        self.log.info(
                            f"[{domain}] Not sending "
                            f"notifications. Stream was only "
                            f"offline for "
                            f"{seconds_since_last_offline} of "
                            f"{TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN}"
                            f" seconds and did not change its "
                            f"title."
                        )
                else:
                    # Offline for a while. Send a normal notification.
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title or "",
                        stream_tags,
                        title_change=False,
                    )
            else:
                # No, this is the first time we're querying
                self.log.info(
                    f"[{domain}] Not sending notifications. "
                    f"This is the first state update for "
                    f"this stream."
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

                # Was the last notification sent before the stream
                # last went offline? If so, send a regular go-live
                # instead of a title change to avoid confusion.
                if (
                    self.offline_timer_cache[domain]
                    > self.notification_service.get_last_notification_time(domain)
                ):
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title or "",
                        stream_tags,
                        title_change=False,
                    )
                else:
                    # No. Send a normal title change notification.
                    await self.notification_service.notify_stream_live(
                        domain,
                        stream_name,
                        new_state.title or "",
                        stream_tags,
                        title_change=True,
                    )

        # Did the stream go offline (old had connect, new doesn't)?
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

        # Update the database with current stream state, if needed.
        if update_database:
            # Ensure we have the stream config before updating
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
        return True

    async def _check_cleanup_thresholds(self, domain: str, counter: int) -> None:
        """Check if a domain hit cleanup warning or deletion thresholds.

        :param domain: The domain to check.
        :param counter: The current failure counter value.
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
                f"[{domain}] Reached 90-day deletion threshold."
                f" Removing all subscriptions."
            )
            # Send deletion notification
            await self.notification_service.send_cleanup_deletion(domain)

            # Delete all subscriptions for this domain
            deleted_count = await self.subscription_repo.delete_all_for_domain(domain)

            # Delete the stream record
            await self.stream_repo.delete(domain)

            self.log.info(
                f"[{domain}] Cleanup complete. "
                f"Deleted {deleted_count} subscriptions "
                f"and stream record."
            )
