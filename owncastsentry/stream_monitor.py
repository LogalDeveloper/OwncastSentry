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

from .types import StreamState, UpdateResult

if TYPE_CHECKING:
    import logging

    from .metrics import MetricsService
    from .notification_service import NotificationService
    from .owncast_client import OwncastClient
    from .repository import StreamRepository, SubscriptionRepository


_TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN = 7 * 60

_CLEANUP_WARNING_THRESHOLD = 83 * 24 * 60
_CLEANUP_DELETE_THRESHOLD = 90 * 24 * 60
_QUERY_EVERY_MINUTE_FAILURE_LIMIT = 4
_QUERY_EVERY_2_MINUTES_FAILURE_LIMIT = 9
_QUERY_EVERY_3_MINUTES_FAILURE_LIMIT = 14
_QUERY_EVERY_5_MINUTES_FAILURE_LIMIT = 29
_QUERY_EVERY_2_MINUTES_INTERVAL = 2
_QUERY_EVERY_3_MINUTES_INTERVAL = 3
_QUERY_EVERY_5_MINUTES_INTERVAL = 5
_QUERY_EVERY_15_MINUTES_INTERVAL = 15


def _should_query_stream(failure_counter: int) -> bool:
    """Determine if a stream should be queried based on failure count."""
    if failure_counter <= _QUERY_EVERY_MINUTE_FAILURE_LIMIT:
        return True
    if failure_counter <= _QUERY_EVERY_2_MINUTES_FAILURE_LIMIT:
        return failure_counter % _QUERY_EVERY_2_MINUTES_INTERVAL == 0
    if failure_counter <= _QUERY_EVERY_3_MINUTES_FAILURE_LIMIT:
        return failure_counter % _QUERY_EVERY_3_MINUTES_INTERVAL == 0
    if failure_counter <= _QUERY_EVERY_5_MINUTES_FAILURE_LIMIT:
        return failure_counter % _QUERY_EVERY_5_MINUTES_INTERVAL == 0
    return failure_counter % _QUERY_EVERY_15_MINUTES_INTERVAL == 0


class StreamMonitor:
    """Monitors Owncast streams and detects state changes."""

    def __init__(
        self,
        owncast_client: OwncastClient,
        stream_repo: StreamRepository,
        subscription_repo: SubscriptionRepository,
        notification_service: NotificationService,
        logger: logging.Logger,
        metrics: MetricsService,
    ) -> None:
        """Initialize the stream monitor.

        :param owncast_client: Client for making API calls to Owncast instances.
        :param stream_repo: Repository for stream data.
        :param subscription_repo: Repository for subscription data.
        :param notification_service: Service for sending notifications.
        :param logger: Logger instance for debugging.
        :param metrics: Metrics service for recording Prometheus metrics.
        """
        self.owncast_client = owncast_client
        self.stream_repo = stream_repo
        self.subscription_repo = subscription_repo
        self.notification_service = notification_service
        self.log = logger
        self.metrics = metrics

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

        # Run all stream updates concurrently and collect results.
        results = await asyncio.gather(
            *(self.update_stream(domain) for domain in subscribed_domains),
            return_exceptions=True,
        )

        # Count successes and failures
        successful_checks = 0
        failed_checks = 0
        for domain, result in zip(subscribed_domains, results, strict=True):
            if isinstance(result, BaseException):
                self.log.exception(
                    "[%s] Unhandled exception during stream update.",
                    domain,
                    exc_info=result,
                )
                failed_checks += 1
            elif result:
                successful_checks += 1
            else:
                failed_checks += 1

        self.log.debug(
            "Update complete. %s/%s succeeded, %s failed.",
            successful_checks,
            total_streams,
            failed_checks,
        )

        subscription_counts = await self.subscription_repo.count_by_domains(
            subscribed_domains
        )
        for domain in subscribed_domains:
            self.metrics.set_subscription_count(
                domain, subscription_counts.get(domain, 0)
            )

        return UpdateResult(
            total_streams=total_streams,
            successful_checks=successful_checks,
            failed_checks=failed_checks,
        )

    async def update_stream(self, domain: str) -> bool:
        """Update the state of a stream and send notifications as needed.

        Sends notifications to subscribed Matrix rooms when a stream goes
        live or changes title. Implements progressive backoff for connection
        failures and auto-cleanup for dead instances.

        :param domain: The domain of the stream to update.
        :return: True if check succeeded or was skipped, False on failure.
        """
        # Fetch the current stream state from database to check failure_counter
        old_state = await self.stream_repo.get_by_domain(domain)
        failure_counter = old_state.failure_counter if old_state else 0

        if old_state is None:
            return True

        # Check if we should query this stream based on backoff schedule
        if not _should_query_stream(failure_counter):
            # Skip this cycle, increment counter to track time passage
            await self.stream_repo.increment_failure_counter(domain)
            self.log.debug(
                "[%s] Skipping query due to backoff (counter=%s)",
                domain,
                failure_counter + 1,
            )
            # Check cleanup thresholds even when skipping query
            await self._check_cleanup_thresholds(domain, failure_counter + 1)
            updated_state = await self.stream_repo.get_by_domain(domain)
            if updated_state is not None:
                self.metrics.set_stream_status(domain, updated_state.status)
                self.metrics.set_check_failures(domain, failure_counter + 1)
            # Backoff is expected behavior, not a failure
            return True

        # Flag: no status timestamp has been recorded yet, so suppress
        # notifications for a stream's first observed state.
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
                "[%s] Connection failure (counter=%s)",
                domain,
                failure_counter + 1,
            )
            # Check cleanup thresholds after connection failure
            await self._check_cleanup_thresholds(domain, failure_counter + 1)
            updated_state = await self.stream_repo.get_by_domain(domain)
            if updated_state is not None:
                self.metrics.set_stream_status(domain, updated_state.status)
                self.metrics.set_check_failures(domain, failure_counter + 1)
            # Actual connection failure
            return False

        # Fetch succeeded! Reset failure counter
        await self.stream_repo.reset_failure_counter(domain)
        self.metrics.set_check_failures(domain, 0)

        # Initialize timer cache entries to prevent KeyError on first access
        self.offline_timer_cache.setdefault(domain, 0)

        if old_state.status_since is None:
            # No stream history has been recorded yet. Don't send notifications.
            update_database = True
            first_update = True

        if first_update:
            self.log.info(
                "[%s] Not sending notifications. This is the first state "
                "update for this stream.",
                domain,
            )

        # Did the stream become publicly online?
        elif new_state.online and not old_state.online:
            # Yes! This stream is now live.
            update_database = True
            stream_config = await self.owncast_client.get_stream_config(domain)

            self.log.info("[%s] Stream is now live!", domain)

            # Calculate seconds since the stream last went offline
            seconds_since_last_offline = round(
                time.monotonic() - self.offline_timer_cache[domain]
            )

            # Use fallback values if config fetch failed
            stream_name = stream_config.name if stream_config else domain
            stream_tags = stream_config.tags if stream_config else ()

            # Has this stream been offline for a short time?
            if seconds_since_last_offline < _TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN:
                # Did the stream title change?
                if old_state.title != new_state.title:
                    # Stream was briefly down; send title change notification.
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
                        "[%s] Not sending notifications. Stream was only "
                        "offline for %s of %s seconds and did not change "
                        "its title.",
                        domain,
                        seconds_since_last_offline,
                        _TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN,
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

        elif new_state.online and old_state.online:
            # Did the stream title change mid-session?
            if old_state.title != new_state.title:
                self.log.info("[%s] Stream title was changed!", domain)
                update_database = True
                stream_config = await self.owncast_client.get_stream_config(domain)

                # Use fallback values if config fetch failed
                stream_name = stream_config.name if stream_config else domain
                stream_tags = stream_config.tags if stream_config else ()

                # If the offline marker is newer than the last recorded
                # notification, send a regular go-live instead of a title
                # change to avoid confusion.
                if self.offline_timer_cache[
                    domain
                ] > self.notification_service.get_last_notification_time(domain):
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

        # Did the stream go offline?
        elif not new_state.online and old_state.online:
            # Yep. This stream is now offline. Log it.
            update_database = True
            self.offline_timer_cache[domain] = time.monotonic()
            self.log.info("[%s] Stream is now offline.", domain)

        # Update the database with current stream state, if needed.
        if update_database:
            # Ensure we have the stream config before updating
            if stream_config is None:
                stream_config = await self.owncast_client.get_stream_config(domain)

            # Use fallback value if config fetch failed
            stream_name = stream_config.name if stream_config else ""

            self.log.debug("[%s] Updating stream state in database...", domain)

            if first_update or old_state.online != new_state.online:
                status_since = new_state.status_since
            else:
                status_since = old_state.status_since

            # Create updated state object (title already truncated in new_state)
            updated_state = StreamState(
                domain=domain,
                name=stream_name,
                title=new_state.title,
                online=new_state.online,
                status_since=status_since,
            )

            await self.stream_repo.update(updated_state)

        # All done.
        self.log.debug("[%s] State update completed.", domain)
        self.metrics.set_stream_status(domain, new_state.status)
        return True

    async def _check_cleanup_thresholds(self, domain: str, counter: int) -> None:
        """Check if a domain hit cleanup warning or deletion thresholds.

        :param domain: The domain to check.
        :param counter: The current failure counter value.
        """
        # Check for 83-day warning threshold
        if counter == _CLEANUP_WARNING_THRESHOLD:
            self.log.warning(
                "[%s] Reached 83-day warning threshold. Sending cleanup warning.",
                domain,
            )
            await self.notification_service.send_cleanup_warning(domain)

        # Check for 90-day deletion threshold
        if counter >= _CLEANUP_DELETE_THRESHOLD:
            self.log.warning(
                "[%s] Reached 90-day deletion threshold. Removing all subscriptions.",
                domain,
            )
            # Send deletion notification
            await self.notification_service.send_cleanup_deletion(domain)

            # Delete all subscriptions for this domain
            deleted_count = await self.subscription_repo.delete_all_for_domain(domain)

            # Delete the stream record
            await self.stream_repo.delete(domain)
            self.offline_timer_cache.pop(domain, None)
            self.notification_service.clear_notification_state(domain)

            self.log.info(
                "[%s] Cleanup complete. Deleted %s subscriptions and stream record.",
                domain,
                deleted_count,
            )
            self.metrics.remove_stream(domain)
