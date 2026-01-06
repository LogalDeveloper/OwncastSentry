# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

from urllib.parse import urlparse

# Path to the GetStatus API call on Owncast instances
OWNCAST_STATUS_PATH = "/api/status"

# Path to GetWebConfig API call on Owncast instances
OWNCAST_CONFIG_PATH = "/api/config"

# User agent to send with all HTTP requests.
USER_AGENT = (
    "OwncastSentry/1.1.0 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)"
)

# Hard minimum amount of time between when notifications can be sent for a stream. Prevents spamming notifications for glitchy or malicious streams.
SECONDS_BETWEEN_NOTIFICATIONS = 20 * 60  # 20 minutes in seconds

# I'm not sure the best way to name or explain this variable, so let's just say what uses it:
#
# After a stream goes offline, a timer is started. Then, ...
# - If a stream comes back online with the same title within this time, no notification is sent.
# - If a stream comes back online with a different title, a rename notification is sent.
# - If this time period passes entirely and a stream comes back online after, it's treated as regular going live.
TEMPORARY_OFFLINE_NOTIFICATION_COOLDOWN = 7 * 60  # 7 minutes in seconds

# Counter thresholds for auto-cleanup (based on 60-second polling intervals)
CLEANUP_WARNING_THRESHOLD = 83 * 24 * 60  # 119,520 cycles = 83 days
CLEANUP_DELETE_THRESHOLD = 90 * 24 * 60  # 129,600 cycles = 90 days


def should_query_stream(failure_counter: int) -> bool:
    """
    Determine if a stream should be queried based on its failure counter.
    Implements progressive backoff: 60s (5min) -> 2min (5min) -> 3min (5min) -> 5min (15min) -> 15min.

    :param failure_counter: The current failure counter value
    :return: True if the stream should be queried this cycle, False otherwise
    """
    if failure_counter <= 4:
        # Query every 60s for first 5 minutes (counters 0-4)
        return True
    elif failure_counter <= 9:
        # Query every 2 minutes for next 5 minutes (counters 5-9)
        return (failure_counter * 60) % 120 == 0
    elif failure_counter <= 14:
        # Query every 3 minutes for next 5 minutes (counters 10-14)
        return (failure_counter * 60) % 180 == 0
    elif failure_counter <= 29:
        # Query every 5 minutes for next 15 minutes (counters 15-29)
        return (failure_counter * 60) % 300 == 0
    else:
        # Query every 15 minutes after 30 minutes (counter 30+)
        return (failure_counter * 60) % 900 == 0


def domainify(url: str) -> str:
    """
    Take a given URL and convert it to just the domain.

    :param url: URL or domain string
    :return: Domain extracted from the URL
    """
    # Take whatever input the user provided and try to turn it into just the domain.
    # Examples:
    # "stream.logal.dev" -> "stream.logal.dev"
    # "https://stream.logal.dev" -> "stream.logal.dev"
    # "stream.logal.dev/embed/chat/readwrite" -> "stream.logal.dev"
    # "https://stream.logal.dev/abcdefghijklmno/123456789" -> "stream.logal.dev"
    # "notify@stream.logal.dev" -> "stream.logal.dev"

    parsed_url = urlparse(url)
    domain = (parsed_url.netloc or parsed_url.path).lower()

    if "@" in domain:
        return domain.split("@")[-1]

    return domain
