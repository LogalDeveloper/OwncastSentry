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

"""Command handlers for OwncastSentry bot commands."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from .types import (
    AlreadySubscribedError,
    InvalidOwncastInstanceError,
    NotSubscribedError,
    StreamStatus,
)

if TYPE_CHECKING:
    from maubot import MessageEvent  # type: ignore[attr-defined]

    from .subscription_manager import SubscriptionManager


_MARKDOWN_ESCAPE_TABLE = str.maketrans({c: f"\\{c}" for c in r"\*_[]()~`#+-=|{}.!<>&"})
_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 60 * _SECONDS_PER_MINUTE
_SECONDS_PER_DAY = 24 * _SECONDS_PER_HOUR


def _sanitize_for_plain_text(text: str) -> str:
    """Sanitize text before Markdown escaping."""
    sanitized = text.replace("\n", " ").replace("\r", " ")
    return " ".join(sanitized.split())


def _escape_markdown(text: str) -> str:
    """Escape Markdown special characters in untrusted text."""
    if not text:
        return text

    return text.translate(_MARKDOWN_ESCAPE_TABLE)


def _sanitize_for_markdown(text: str) -> str:
    """Sanitize text for safe Markdown rendering."""
    if not text:
        return text

    return _escape_markdown(_sanitize_for_plain_text(text))


def _format_duration(timestamp_str: str, now: datetime) -> str:
    """Calculate and format the duration from a timestamp to now."""
    try:
        timestamp = datetime.fromisoformat(timestamp_str)
        delta = now - timestamp

        seconds = int(delta.total_seconds())
        if seconds < 0:
            return "unknown duration"
        if seconds < _SECONDS_PER_MINUTE:
            return f"{seconds} second{'s' if seconds != 1 else ''}"
        if seconds < _SECONDS_PER_HOUR:
            minutes = seconds // _SECONDS_PER_MINUTE
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
        if seconds < _SECONDS_PER_DAY:
            hours = seconds // _SECONDS_PER_HOUR
            return f"{hours} hour{'s' if hours != 1 else ''}"
    except TypeError, ValueError:
        return "unknown duration"
    else:
        days = seconds // _SECONDS_PER_DAY
        return f"{days} day{'s' if days != 1 else ''}"


class CommandHandler:
    """Handles bot commands for managing stream subscriptions."""

    def __init__(
        self,
        subscription_manager: SubscriptionManager,
    ) -> None:
        """Initialize the command handler.

        :param subscription_manager: Subscription domain workflow coordinator.
        """
        self.subscription_manager = subscription_manager

    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        """Subscribe a room to a stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: User-supplied Owncast URL, domain, or Fediverse-style address.
        """
        try:
            stream_domain = await self.subscription_manager.subscribe(evt.room_id, url)
        except InvalidOwncastInstanceError:
            await evt.reply(
                "The URL you supplied does not appear to "
                "be a valid Owncast instance. You may have "
                "specified an invalid domain, or the "
                "instance is offline.",
                markdown=False,
            )
            return
        except AlreadySubscribedError as e:
            await evt.reply(
                f"This room is already subscribed to notifications for {e.domain}.",
                markdown=False,
            )
            return

        await evt.reply(
            f"Subscription added! This room will receive "
            f"notifications when {stream_domain} goes live.",
            markdown=False,
        )

    async def unsubscribe(self, evt: MessageEvent, url: str) -> None:
        """Unsubscribe a room from a stream's notifications.

        :param evt: MessageEvent of the message calling the command.
        :param url: User-supplied Owncast URL, domain, or Fediverse-style address.
        """
        try:
            stream_domain = await self.subscription_manager.unsubscribe(
                evt.room_id, url
            )
        except NotSubscribedError as e:
            await evt.reply(
                f"This room is already not subscribed to notifications for {e.domain}.",
                markdown=False,
            )
            return

        await evt.reply(
            f"Subscription removed! This room will no "
            f"longer receive notifications for {stream_domain}.",
            markdown=False,
        )

    async def subscriptions(self, evt: MessageEvent) -> None:
        """List all stream subscriptions in the current room.

        :param evt: MessageEvent of the message calling the command.
        """
        subscriptions = await self.subscription_manager.list_room_subscriptions(
            evt.room_id
        )

        if not subscriptions:
            await evt.reply(
                "This room is not subscribed to any Owncast "
                "instances.\n\nTo subscribe to an Owncast "
                "instance, use `!subscribe <domain>`",
                markdown=True,
            )
            return

        # Build the response message body as Markdown
        count = len(subscriptions)
        parts = [f"**Subscriptions for this room ({count}):**\n\n"]
        now = datetime.now(UTC)

        for subscription in subscriptions:
            domain = subscription.domain
            stream_state = subscription.stream_state
            stream_name = stream_state.name or domain
            safe_stream_name = _sanitize_for_markdown(stream_name)

            # Start building this stream's entry with stream name as main bullet
            parts.append(f"- **{safe_stream_name}**  \n")

            # Add title if stream is online (as a sub-bullet)
            if stream_state.status == StreamStatus.ONLINE and stream_state.title:
                safe_title = _sanitize_for_markdown(stream_state.title)
                parts.append(f"  - Title: {safe_title}  \n")

            # Determine status and duration (as a sub-bullet)
            match stream_state.status:
                case StreamStatus.ONLINE:
                    duration = _format_duration(
                        cast("str", stream_state.status_since), now
                    )
                    parts.append(f"  - Status: Online for {duration}  \n")
                case StreamStatus.UNKNOWN:
                    parts.append("  - Status: Unknown (instance unreachable)  \n")
                case StreamStatus.OFFLINE if stream_state.status_since:
                    duration = _format_duration(stream_state.status_since, now)
                    parts.append(f"  - Status: Offline for {duration}  \n")
                case StreamStatus.OFFLINE:
                    parts.append("  - Status: Offline  \n")

            # Add stream link (as a sub-bullet)
            parts.append(f"  - Link: https://{domain}\n\n")

        # Add help text for unsubscribing
        parts.append(
            "\nTo unsubscribe from any of these Owncast "
            "instances, use `!unsubscribe <domain>`"
        )

        # Send the response as Markdown
        await evt.reply("".join(parts), markdown=True)

    async def live(self, evt: MessageEvent) -> None:
        """List currently live streams in the current room.

        :param evt: MessageEvent of the message calling the command.
        """
        live_streams = await self.subscription_manager.list_live_room_subscriptions(
            evt.room_id
        )

        if not live_streams:
            if not await self.subscription_manager.has_room_subscriptions(evt.room_id):
                await evt.reply(
                    "This room is not subscribed to any Owncast "
                    "instances.\n\nTo subscribe to an Owncast "
                    "instance, use `!subscribe <domain>`",
                    markdown=True,
                )
                return

            await evt.reply(
                "No subscribed Owncast instances are currently "
                "live.\n\nUse `!subscriptions` to list all "
                "subscriptions.",
                markdown=True,
            )
            return

        # Build the response message body as Markdown
        count = len(live_streams)
        parts = [f"**Live Owncast instances ({count}):**\n\n"]
        now = datetime.now(UTC)

        for subscription in live_streams:
            domain = subscription.domain
            stream_state = subscription.stream_state
            stream_name = stream_state.name or domain
            safe_stream_name = _sanitize_for_markdown(stream_name)

            # Start building this stream's entry with stream name as main bullet
            parts.append(f"- **{safe_stream_name}**  \n")

            # Add title when available
            if stream_state.title:
                safe_title = _sanitize_for_markdown(stream_state.title)
                parts.append(f"  - Title: {safe_title}  \n")

            # Add status with duration
            duration = _format_duration(cast("str", stream_state.status_since), now)
            parts.append(f"  - Online for {duration}  \n")

            # Add stream link
            parts.append(f"  - Link: https://{domain}\n\n")

        # Send the response as Markdown
        await evt.reply("".join(parts).rstrip(), markdown=True)
