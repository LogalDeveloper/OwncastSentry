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

"""Notification service for sending Matrix messages."""

import asyncio
import time
from typing import TYPE_CHECKING, Any

from mautrix.types import MessageType, TextMessageEventContent

from .metrics import NotificationType

if TYPE_CHECKING:
    import logging
    from collections.abc import Sequence

    from .metrics import MetricsService
    from .repository import SubscriptionRepository


_SECONDS_BETWEEN_NOTIFICATIONS = 20 * 60

_CLEANUP_WARNING_DAYS = 83
_CLEANUP_DELETE_DAYS = 90


def _sanitize_for_plain_text(text: str) -> str:
    """Sanitize text for plain text rendering."""
    if not text:
        return text

    return " ".join(text.split())


class NotificationService:
    """Service for sending Matrix notifications about stream events."""

    def __init__(
        self,
        client: Any,
        subscription_repo: SubscriptionRepository,
        logger: logging.Logger,
        metrics: MetricsService,
    ) -> None:
        """Initialize the notification service.

        :param client: The Matrix client for sending messages.
        :param subscription_repo: Repository for managing subscriptions.
        :param logger: Logger instance for debugging.
        :param metrics: Metrics service for recording counters.
        """
        self.client = client
        self.subscription_repo = subscription_repo
        self.log = logger
        self.metrics = metrics

        # Cache for tracking when notifications were last sent
        self.notification_timers_cache: dict[str, float] = {}

    async def notify_stream_live(
        self,
        domain: str,
        name: str,
        title: str,
        tags: Sequence[str],
        *,
        title_change: bool = False,
    ) -> None:
        """Send notifications to rooms subscribed to a stream.

        :param domain: The stream domain to send notifications for.
        :param name: The stream name to include in the message.
        :param title: The stream title to include in the message.
        :param tags: List of stream tags to include in the message.
        :param title_change: Whether this is a title change notification.
        """
        # Has enough time passed since the last notification was sent?
        if not self._can_notify(domain):
            seconds_since_last = round(
                time.monotonic() - self.notification_timers_cache[domain]
            )
            self.log.info(
                "[%s] Not sending notifications. Only %s of required "
                "%s seconds have passed since last notification.",
                domain,
                seconds_since_last,
                _SECONDS_BETWEEN_NOTIFICATIONS,
            )
            return

        # Build the notification message
        body_text = self._format_message(
            name, title, domain, tags, title_change=title_change
        )

        # Send notifications to all subscribed rooms in parallel
        successful, failed = await self._broadcast_to_rooms(domain, body_text)

        # Record that a notification was sent if at least one room received it.
        if successful > 0:
            self._record_notification(domain)

        # Log completion
        notification_type = "title change" if title_change else "going live"
        self.log.info(
            "[%s] Completed sending %s notifications! %s succeeded, %s failed.",
            domain,
            notification_type,
            successful,
            failed,
        )

        self.metrics.record_delivery(
            NotificationType.TITLE_CHANGE if title_change else NotificationType.LIVE,
            successful=successful,
            failed=failed,
        )

    async def send_cleanup_warning(self, domain: str) -> None:
        """Send cleanup warning notification to all subscribed rooms.

        :param domain: The stream domain.
        """
        remaining_days = _CLEANUP_DELETE_DAYS - _CLEANUP_WARNING_DAYS
        body_text = (
            "⚠️  Warning: Subscription Cleanup Scheduled\n\n"
            f"The Owncast instance at {domain} has been "
            f"unreachable for {_CLEANUP_WARNING_DAYS} days. If it remains "
            f"unreachable for {remaining_days} more days "
            f"({_CLEANUP_DELETE_DAYS} days total), this subscription "
            f"will be automatically removed."
        )

        successful, failed = await self._broadcast_to_rooms(domain, body_text)
        self.log.info(
            "[%s] Sent cleanup warning to %s rooms (%s failed).",
            domain,
            successful,
            failed,
        )

        self.metrics.record_delivery(
            NotificationType.CLEANUP_WARNING, successful=successful, failed=failed
        )

    async def send_cleanup_deletion(self, domain: str) -> None:
        """Send cleanup deletion notification to all subscribed rooms.

        :param domain: The stream domain.
        """
        body_text = (
            "🗑️  Subscription Automatically Removed\n\n"
            f"The Owncast instance at {domain} has been "
            f"unreachable for {_CLEANUP_DELETE_DAYS} days and has been "
            f"automatically removed from subscriptions in this "
            f"room.\n\n"
            f"If the instance comes online again and you want to "
            f"resubscribe, run `!subscribe {domain}`."
        )

        successful, failed = await self._broadcast_to_rooms(domain, body_text)
        self.log.info(
            "[%s] Sent cleanup deletion notice to %s rooms (%s failed).",
            domain,
            successful,
            failed,
        )

        self.metrics.record_delivery(
            NotificationType.CLEANUP_DELETION, successful=successful, failed=failed
        )

    def get_last_notification_time(self, domain: str) -> float:
        """Get the cached live/title-change notification timestamp for a domain.

        :param domain: The stream domain.
        :return: Monotonic timestamp of the last successful live/title-change
            notification, or 0 if none has been recorded.
        """
        return self.notification_timers_cache.get(domain, 0)

    def clear_notification_state(self, domain: str) -> None:
        """Clear cached notification state for a deleted domain.

        :param domain: The stream domain to remove from local caches.
        """
        self.notification_timers_cache.pop(domain, None)

    async def _send_notification(
        self, room_id: str, body_text: str, domain: str
    ) -> None:
        """Send a notification to a single room.

        :param room_id: The Matrix room ID to send to.
        :param body_text: The message body text.
        :param domain: The stream domain (for logging).
        Propagates send errors so broadcast delivery accounting can count failures.
        """
        try:
            content = TextMessageEventContent(msgtype=MessageType.TEXT, body=body_text)
            await self.client.send_message(room_id, content)
        except Exception as exception:
            self.log.warning(
                "[%s] Failed to send notification message to room [%s]: %s",
                domain,
                room_id,
                exception,
            )
            raise

    def _format_message(
        self,
        name: str,
        title: str,
        domain: str,
        tags: Sequence[str],
        *,
        title_change: bool,
    ) -> str:
        """Format the notification message body.

        :param name: The stream name.
        :param title: The stream title.
        :param domain: The stream domain.
        :param tags: List of stream tags.
        :param title_change: Whether this is a title change notification.
        :return: Formatted message body.
        """
        # Use name if available, fallback to domain
        stream_name = name or domain
        safe_stream_name = _sanitize_for_plain_text(stream_name)

        # Choose message based on notification type
        if title_change:
            parts = [f"📝 {safe_stream_name} has changed its stream title!"]
        else:
            parts = [f"🎥 {safe_stream_name} is now live!"]

        # Add title if present
        if title:
            safe_title = _sanitize_for_plain_text(title)
            parts.append(f"\nStream Title: {safe_title}")

        # Add stream URL
        parts.append(f"\n\nTo tune in, visit: https://{domain}/")

        # Add tags if present
        if tags:
            tag_text = " ".join(
                f"#{safe_tag}"
                for tag in tags
                if (safe_tag := _sanitize_for_plain_text(tag))
                and not safe_tag.startswith(".")
            )

            if tag_text:
                parts.append(f"\n\n{tag_text}")

        return "".join(parts)

    def _can_notify(self, domain: str) -> bool:
        """Check if enough time has passed to send another notification.

        :param domain: The stream domain.
        :return: True if notification can be sent, False otherwise.
        """
        last_notification_time = self.notification_timers_cache.get(domain)
        return (
            last_notification_time is None
            or time.monotonic() - last_notification_time
            >= _SECONDS_BETWEEN_NOTIFICATIONS
        )

    def _record_notification(self, domain: str) -> None:
        """Record that a notification was sent at the current time.

        :param domain: The stream domain.
        """
        self.notification_timers_cache[domain] = time.monotonic()

    async def _broadcast_to_rooms(self, domain: str, body_text: str) -> tuple[int, int]:
        """Send a message to all rooms subscribed to a domain.

        :param domain: The stream domain.
        :param body_text: The message body text.
        :return: Tuple of (successful, failed) counts.
        """
        room_ids = await self.subscription_repo.get_subscribed_rooms(domain)
        tasks = [
            self._send_notification(room_id, body_text, domain) for room_id in room_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed = sum(1 for r in results if isinstance(r, BaseException))
        successful = len(results) - failed
        return successful, failed
