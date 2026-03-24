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

"""Utility functions and constants for OwncastSentry."""

import re
from urllib.parse import urlparse

# Path to the GetStatus API call on Owncast instances
OWNCAST_STATUS_PATH = "/api/status"

# Path to GetWebConfig API call on Owncast instances
OWNCAST_CONFIG_PATH = "/api/config"

# Fields that must be present in an Owncast status API response
REQUIRED_STATUS_FIELDS = frozenset(
    {
        "lastConnectTime",
        "lastDisconnectTime",
        "streamTitle",
        "online",
    }
)


def user_agent(version: str) -> str:
    """Build the User-Agent header string for HTTP requests.

    :param version: The plugin version string.
    :return: A formatted User-Agent string.
    """
    return (
        f"OwncastSentry/{version}"
        " (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)"
    )


# Hard minimum amount of time between when notifications can be sent
# for a stream. Prevents spamming notifications for glitchy or
# malicious streams.
SECONDS_BETWEEN_NOTIFICATIONS = 20 * 60  # 20 minutes in seconds

# After a stream goes offline, a timer is started. Then, ...
# - If a stream comes back online with the same title within this
#   time, no notification is sent.
# - If a stream comes back online with a different title, a rename
#   notification is sent.
# - If this time period passes entirely and a stream comes back
#   online after, it's treated as regular going live.
TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN = 7 * 60  # 7 min in seconds

# Auto-cleanup timing (days of continuous unreachability)
CLEANUP_WARNING_DAYS = 83
CLEANUP_DELETE_DAYS = 90

# Counter thresholds derived from days (60-second polling intervals)
CLEANUP_WARNING_THRESHOLD = CLEANUP_WARNING_DAYS * 24 * 60
CLEANUP_DELETE_THRESHOLD = CLEANUP_DELETE_DAYS * 24 * 60

# Failure counter threshold for treating stream status as "unknown"
UNKNOWN_STATUS_THRESHOLD = 15

# Maximum field lengths based on Owncast's configuration
# Source: https://github.com/owncast/owncast/blob/master/
#         web/utils/config-constants.tsx
MAX_INSTANCE_TITLE_LENGTH = 255  # Server Name (line 81)
MAX_STREAM_TITLE_LENGTH = 100  # Stream Title (line 91)
MAX_TAG_LENGTH = 24  # Per tag (line 208)


def should_query_stream(failure_counter: int) -> bool:
    """Determine if a stream should be queried based on failure count.

    Implements progressive backoff with increasing intervals:
    - Counters 0-4:   every 60s  (first 5 minutes)
    - Counters 5-9:   every 2min (next 5 minutes)
    - Counters 10-14: every 3min (next 5 minutes)
    - Counters 15-29: every 5min (next 15 minutes)
    - Counters 30+:   every 15min

    :param failure_counter: The current failure counter value.
    :return: True if the stream should be queried this cycle.
    """
    if failure_counter <= 4:
        # Query every cycle for first 5 minutes (counters 0-4)
        return True
    if failure_counter <= 9:
        # Query every 2nd cycle for next 5 minutes (counters 5-9)
        return failure_counter % 2 == 0
    if failure_counter <= 14:
        # Query every 3rd cycle for next 5 minutes (counters 10-14)
        return failure_counter % 3 == 0
    if failure_counter <= 29:
        # Query every 5th cycle for next 15 minutes (counters 15-29)
        return failure_counter % 5 == 0
    # Query every 15th cycle after 30 minutes (counter 30+)
    return failure_counter % 15 == 0


def domainify(url: str) -> str:
    """Extract and sanitize a domain from user input.

    Handles URLs, bare domains, and email-style input (user@domain).
    Only allows valid domain characters (alphanumeric, hyphens, periods).

    :param url: URL, domain, or email-style string
    :return: Sanitized domain
    """
    # Handle email-style format first (e.g., "notify@stream.logal.dev")
    if "@" in url:
        url = url.split("@")[-1]

    # Prepend // if no scheme so urlparse treats input as netloc
    if not url.startswith(("http://", "https://", "//")):
        url = f"//{url}"

    parsed = urlparse(url)
    domain = (parsed.netloc or parsed.path).lower()

    # Strip port and path
    domain = domain.split(":")[0].split("/")[0]

    # Allow only valid domain characters
    return re.sub(r"[^a-z0-9.-]", "", domain).strip(".-")


def truncate(text: str, max_length: int) -> str:
    """Truncate text to a maximum length.

    :param text: The text to truncate
    :param max_length: Maximum allowed length
    :return: Truncated text, or original if within limit
    """
    if not text or len(text) <= max_length:
        return text
    return text[:max_length]


_MARKDOWN_ESCAPE_TABLE = str.maketrans({c: f"\\{c}" for c in r"\*_[]()~`#+-=|{}.!<>&"})


def escape_markdown(text: str) -> str:
    """Escape Markdown special characters to prevent injection attacks.

    This function sanitizes untrusted external input (like stream names and titles)
    before embedding them in Markdown-formatted messages. It prevents malicious
    actors from injecting arbitrary Markdown/HTML content.

    :param text: The text to escape
    :return: The escaped text safe for Markdown rendering
    """
    if not text:
        return text

    return text.translate(_MARKDOWN_ESCAPE_TABLE)


def sanitize_for_plain_text(text: str) -> str:
    """Sanitize text for plain text rendering.

    Remove newlines and normalize whitespace without escaping
    special characters. Use this for plain text notifications where
    escaping would show literal backslashes.

    :param text: The text to sanitize
    :return: Sanitized text
    """
    if not text:
        return text

    # Remove newlines and carriage returns to prevent multi-line injection
    sanitized = text.replace("\n", " ").replace("\r", " ")

    # Collapse multiple spaces into single space
    return " ".join(sanitized.split())


def sanitize_for_markdown(text: str) -> str:
    """Sanitize text for safe Markdown rendering.

    Remove newlines, normalize whitespace, and escape Markdown special
    characters. Use this for any untrusted external content before
    embedding in Markdown messages.

    Note: This function does not truncate. Size limits should be
    enforced at the model layer (e.g., in from_api_response methods).

    :param text: The text to sanitize
    :return: Sanitized and escaped text safe for Markdown rendering
    """
    if not text:
        return text

    return escape_markdown(sanitize_for_plain_text(text))
