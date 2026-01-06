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
