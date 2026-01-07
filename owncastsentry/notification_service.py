# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import time
import asyncio
from typing import List

from mautrix.types import TextMessageEventContent, MessageType

from .database import SubscriptionRepository
from .utils import SECONDS_BETWEEN_NOTIFICATIONS, sanitize_for_markdown


class NotificationService:
    """Service for sending Matrix notifications about stream events."""

    def __init__(self, client, subscription_repo: SubscriptionRepository, logger):
        """
        Initialize the notification service.

        :param client: The Matrix client for sending messages
        :param subscription_repo: Repository for managing subscriptions
        :param logger: Logger instance
        """
        self.client = client
        self.subscription_repo = subscription_repo
        self.log = logger

        # Cache for tracking when notifications were last sent
        self.notification_timers_cache = {}

    async def notify_stream_live(
        self,
        domain: str,
        name: str,
        title: str,
        tags: List[str],
        title_change: bool = False,
    ) -> None:
        """
        Sends notifications to rooms with subscriptions to the provided stream domain.

        :param domain: The domain of the stream to send notifications for.
        :param name: The name of the stream to include in the message.
        :param title: The title of the stream to include in the message.
        :param tags: List of stream tags to include in the message.
        :param title_change: Whether or not this is for a stream changing its title rather than going live.
        :return: Nothing.
        """
        # Has enough time passed since the last notification was sent?
        if not self._can_notify(domain):
            seconds_since_last = round(
                time.time() - self.notification_timers_cache[domain]
            )
            self.log.info(
                f"[{domain}] Not sending notifications. Only {seconds_since_last} of required {SECONDS_BETWEEN_NOTIFICATIONS} seconds has passed since last notification."
            )
            return

        # Record that we're sending a notification now
        self._record_notification(domain)

        # Get a list of room IDs with active subscriptions to the stream domain
        room_ids = await self.subscription_repo.get_subscribed_rooms(domain)

        # Build the notification message
        body_text = self._format_message(name, title, domain, tags, title_change)

        # Set up counters for statistics
        successful_notifications = 0
        failed_notifications = 0

        # Send notifications to all subscribed rooms in parallel
        # IMPROVEMENT: Parallel notification delivery with asyncio.gather (was a TODO in original code)
        tasks = []
        for room_id in room_ids:
            tasks.append(self._send_notification(room_id, body_text, domain))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        for result in results:
            if isinstance(result, Exception):
                failed_notifications += 1
            else:
                successful_notifications += 1

        # Log completion
        notification_type = "title change" if title_change else "going live"
        self.log.info(
            f"[{domain}] Completed sending {notification_type} notifications! {successful_notifications} succeeded, {failed_notifications} failed."
        )

    async def _send_notification(
        self, room_id: str, body_text: str, domain: str
    ) -> None:
        """
        Send a notification to a single room.

        :param room_id: The Matrix room ID to send to
        :param body_text: The message body text
        :param domain: The stream domain (for logging)
        :return: Nothing
        :raises: Exception if sending fails
        """
        try:
            content = TextMessageEventContent(msgtype=MessageType.TEXT, body=body_text)
            await self.client.send_message(room_id, content)
        except Exception as exception:
            self.log.warning(
                f"[{domain}] Failed to send notification message to room [{room_id}]: {exception}"
            )
            raise

    def _format_message(
        self, name: str, title: str, domain: str, tags: List[str], title_change: bool
    ) -> str:
        """
        Format the notification message body.

        :param name: The stream name
        :param title: The stream title
        :param domain: The stream domain
        :param tags: List of stream tags
        :param title_change: Whether this is a title change notification
        :return: Formatted message body
        """
        # Use name if available, fallback to domain
        stream_name = name if name else domain
        safe_stream_name = sanitize_for_markdown(stream_name)

        # Choose message based on notification type
        if title_change:
            body_text = "📝 " + safe_stream_name + " has changed its stream title!"
        else:
            body_text = "🎥 " + safe_stream_name + " is now live!"

        # Add title if present
        if title != "":
            safe_title = sanitize_for_markdown(title)
            body_text += "\nStream Title: " + safe_title

        # Add stream URL
        body_text += "\n\nTo tune in, visit: https://" + domain + "/"

        # Add tags if present
        if tags and len(tags) > 0:
            safe_tags = []
            for tag in tags:
                safe_tag = sanitize_for_markdown(tag)
                if safe_tag and not safe_tag.startswith('.'):
                    safe_tags.append(safe_tag)

            if safe_tags:
                body_text += "\n\n"
                body_text += " ".join("#" + tag for tag in safe_tags)

        return body_text

    def _can_notify(self, domain: str) -> bool:
        """
        Check if enough time has passed to send another notification.

        :param domain: The stream domain
        :return: True if notification can be sent, False otherwise
        """
        if domain not in self.notification_timers_cache:
            return True

        seconds_since_last = round(time.time() - self.notification_timers_cache[domain])
        return seconds_since_last >= SECONDS_BETWEEN_NOTIFICATIONS

    def _record_notification(self, domain: str) -> None:
        """
        Record that a notification was sent at the current time.

        :param domain: The stream domain
        :return: Nothing
        """
        self.notification_timers_cache[domain] = time.time()

    async def send_cleanup_warning(self, domain: str) -> None:
        """
        Send 83-day warning notification to all subscribed rooms.

        :param domain: The stream domain
        :return: Nothing
        """
        # Get all subscribed rooms
        room_ids = await self.subscription_repo.get_subscribed_rooms(domain)

        # Build the warning message
        body_text = (
            f"⚠️  Warning: Subscription Cleanup Scheduled\n\n"
            f"The Owncast instance at {domain} has been unreachable for 83 days. "
            f"If it remains unreachable for 7 more days (90 days total), this "
            f"subscription will be automatically removed."
        )

        # Send to all rooms in parallel
        tasks = []
        for room_id in room_ids:
            tasks.append(self._send_notification(room_id, body_text, domain))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))

        self.log.info(
            f"[{domain}] Sent cleanup warning to {successful} rooms ({failed} failed)."
        )

    async def send_cleanup_deletion(self, domain: str) -> None:
        """
        Send 90-day deletion notification to all subscribed rooms.

        :param domain: The stream domain
        :return: Nothing
        """
        # Get all subscribed rooms
        room_ids = await self.subscription_repo.get_subscribed_rooms(domain)

        # Build the deletion message
        body_text = (
            f"🗑️  Subscription Automatically Removed\n\n"
            f"The Owncast instance at {domain} has been unreachable for 90 days "
            f"and has been automatically removed from subscriptions in this room.\n\n"
            f"If the instance comes online again and you want to resubscribe, "
            f"run `!subscribe {domain}`."
        )

        # Send to all rooms in parallel
        tasks = []
        for room_id in room_ids:
            tasks.append(self._send_notification(room_id, body_text, domain))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))

        self.log.info(
            f"[{domain}] Sent cleanup deletion notice to {successful} rooms ({failed} failed)."
        )
