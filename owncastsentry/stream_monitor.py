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
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum, auto
from typing import TYPE_CHECKING

from .types import (
    StreamConfigObservation,
    StreamState,
    StreamStateObservation,
    StreamStatus,
    UpdateResult,
)

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


class _StreamTransitionKind(Enum):
    """High-level status transition from stored state to fresh observation."""

    FIRST_OBSERVATION = auto()
    WENT_LIVE = auto()
    WENT_OFFLINE = auto()
    TITLE_CHANGED = auto()
    STATUS_UNCHANGED = auto()


@dataclass(frozen=True, slots=True)
class _StreamTransition:
    """Classified stream transition and timestamp metadata."""

    kind: _StreamTransitionKind
    status_since: str | None


class _NotificationKind(Enum):
    """Live/title notification action for a transition."""

    NONE = auto()
    LIVE = auto()
    TITLE_CHANGE = auto()

    @property
    def should_notify(self) -> bool:
        """Return whether a live/title-change notification should be attempted."""
        return self is not _NotificationKind.NONE


def _classify_transition(
    old_state: StreamState, observation: StreamStateObservation
) -> _StreamTransition:
    """Classify the transition from the stored state to the latest observation."""
    if old_state.status_since is None:
        return _StreamTransition(
            kind=_StreamTransitionKind.FIRST_OBSERVATION,
            status_since=observation.observed_at,
        )

    if old_state.online != observation.online:
        return _StreamTransition(
            kind=(
                _StreamTransitionKind.WENT_LIVE
                if observation.online
                else _StreamTransitionKind.WENT_OFFLINE
            ),
            status_since=observation.observed_at,
        )

    if old_state.online and old_state.title != observation.title:
        return _StreamTransition(
            kind=_StreamTransitionKind.TITLE_CHANGED,
            status_since=old_state.status_since,
        )

    return _StreamTransition(
        kind=_StreamTransitionKind.STATUS_UNCHANGED,
        status_since=old_state.status_since,
    )


def _classify_notification(
    transition: _StreamTransition,
    old_state: StreamState,
    observation: StreamStateObservation,
    *,
    offline_duration_seconds: int | None = None,
) -> _NotificationKind:
    """Classify the live/title notification policy for a transition.

    The caller supplies offline duration because it comes from the monitor's
    monotonic offline cache, not the transition.
    """
    match transition.kind:
        case _StreamTransitionKind.WENT_LIVE:
            if offline_duration_seconds is not None and (
                offline_duration_seconds < _TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN
            ):
                if old_state.title != observation.title:
                    return _NotificationKind.TITLE_CHANGE
                return _NotificationKind.NONE

            return _NotificationKind.LIVE

        case _StreamTransitionKind.TITLE_CHANGED:
            return _NotificationKind.TITLE_CHANGE

        case _:
            return _NotificationKind.NONE


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

        # Fetch the latest stream state from the server
        observation = await self.owncast_client.get_stream_state(domain)

        # If the fetch failed, increment failure counter and skip the update
        if observation is None:
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

        transition = _classify_transition(old_state, observation)
        # Notification policy may need runtime timing context in addition to
        # the stored state and latest observation.
        offline_duration_seconds: int | None = None

        match transition.kind:
            case _StreamTransitionKind.FIRST_OBSERVATION:
                self.log.info(
                    "[%s] Not sending notifications. This is the first state "
                    "update for this stream.",
                    domain,
                )
            case _StreamTransitionKind.WENT_LIVE:
                self.log.info("[%s] Stream is now live!", domain)
                offline_duration_seconds = round(
                    time.monotonic() - self.offline_timer_cache[domain]
                )
                # Brief reconnects with the same title are treated as transient
                # outages, not new live events.
                if (
                    offline_duration_seconds < _TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN
                    and old_state.title == observation.title
                ):
                    self.log.info(
                        "[%s] Not sending notifications. Stream was only offline for "
                        "%s of %s seconds and did not change its title.",
                        domain,
                        offline_duration_seconds,
                        _TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN,
                    )
            case _StreamTransitionKind.TITLE_CHANGED:
                self.log.info("[%s] Stream title was changed!", domain)
            case _StreamTransitionKind.WENT_OFFLINE:
                self.offline_timer_cache[domain] = time.monotonic()
                self.log.info("[%s] Stream is now offline.", domain)
            case _StreamTransitionKind.STATUS_UNCHANGED:
                pass

        notification_kind = _classify_notification(
            transition,
            old_state,
            observation,
            offline_duration_seconds=offline_duration_seconds,
        )

        stream_config: StreamConfigObservation | None = None
        observed_at = datetime.fromisoformat(observation.observed_at)
        hourly_config_refresh_due = (
            observed_at.tzinfo is not None and observed_at.astimezone(UTC).minute == 0
        )
        should_fetch_config = (
            notification_kind.should_notify
            or transition.kind is _StreamTransitionKind.FIRST_OBSERVATION
            or hourly_config_refresh_due
        )
        if should_fetch_config:
            stream_config = await self.owncast_client.get_stream_config(domain)

        if notification_kind.should_notify:
            stream_name = stream_config.name if stream_config else domain
            stream_tags = stream_config.tags if stream_config else ()
            is_title_change = notification_kind is _NotificationKind.TITLE_CHANGE

            await self.notification_service.notify_stream_live(
                domain,
                stream_name,
                observation.title or "",
                stream_tags,
                title_change=is_title_change,
            )

        self.log.debug("[%s] Saving stream state if changed...", domain)

        if stream_config is not None:
            await self.stream_repo.update(
                domain,
                name=stream_config.name,
                title=observation.title,
                online=observation.online,
                status_since=transition.status_since,
            )
        else:
            await self.stream_repo.update(
                domain,
                title=observation.title,
                online=observation.online,
                status_since=transition.status_since,
            )

        # All done.
        self.log.debug("[%s] State update completed.", domain)
        stream_status = (
            StreamStatus.ONLINE if observation.online else StreamStatus.OFFLINE
        )
        self.metrics.set_stream_status(domain, stream_status)
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
