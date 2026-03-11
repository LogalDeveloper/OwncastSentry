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

"""HTTP client for querying Owncast instance APIs."""

import json
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    import logging

from .models import StreamConfig, StreamState
from .utils import OWNCAST_CONFIG_PATH, OWNCAST_STATUS_PATH, user_agent


class OwncastClient:
    """HTTP client for communicating with Owncast instances."""

    def __init__(self, logger: logging.Logger, version: str) -> None:
        """Initialize the Owncast client with an HTTP session.

        :param logger: Logger instance for debugging
        :param version: Plugin version string for the User-Agent header
        """
        self.log = logger

        # Set up HTTP session configuration
        headers = {"User-Agent": user_agent(version)}
        cookie_jar = aiohttp.DummyCookieJar()
        connector = aiohttp.TCPConnector(
            use_dns_cache=False,
            limit=1000,
            limit_per_host=1,
            keepalive_timeout=120,
        )
        timeout = aiohttp.ClientTimeout(sock_connect=5, sock_read=5)

        self.session = aiohttp.ClientSession(
            headers=headers,
            cookie_jar=cookie_jar,
            timeout=timeout,
            connector=connector,
        )

    async def get_stream_state(self, domain: str) -> StreamState | None:
        """Get the current stream state for a given domain.

        HTTPS on port 443 is assumed, no other protocols or ports
        are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A StreamState if available, None on error.
        """
        self.log.debug(f"[{domain}] Fetching current stream state...")
        status_url = "https://" + domain + OWNCAST_STATUS_PATH

        # Make a request to the endpoint
        try:
            response = await self.session.request(
                "GET", status_url, allow_redirects=False
            )
        except Exception as e:
            self.log.warning(
                f"[{domain}] Error making GET request to {OWNCAST_STATUS_PATH}: {e}"
            )
            return None

        # Check the response code is success
        if response.status != 200:
            self.log.warning(
                f"[{domain}] Response to request on "
                f"{OWNCAST_STATUS_PATH} was not 200, "
                f"got {response.status} instead."
            )
            return None

        # Try to interpret the response as JSON
        try:
            new_state = json.loads(await response.read())
        except Exception as e:
            self.log.warning(
                f"[{domain}] Rejecting response to request on "
                f"{OWNCAST_STATUS_PATH} as could not be "
                f"interpreted as JSON: {e}"
            )
            return None

        # Validate the response contains all basic info needed
        required_fields = [
            "lastConnectTime",
            "lastDisconnectTime",
            "streamTitle",
            "online",
        ]
        for field in required_fields:
            if field not in new_state:
                self.log.warning(
                    f"[{domain}] Rejecting response to request "
                    f"on {OWNCAST_STATUS_PATH} as it does not "
                    f"have {field} field."
                )
                return None

        return StreamState.from_api_response(new_state, domain)

    async def get_stream_config(self, domain: str) -> StreamConfig | None:
        """Get the current stream config for a given domain.

        HTTPS on port 443 is assumed, no other protocols or ports
        are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A StreamConfig, or None if fetch failed.
        """
        self.log.debug(f"[{domain}] Fetching current stream config...")
        config_url = "https://" + domain + OWNCAST_CONFIG_PATH

        # Make a request to the endpoint
        try:
            response = await self.session.request(
                "GET", config_url, allow_redirects=False
            )
        except Exception as e:
            self.log.warning(
                f"[{domain}] Error making GET request to {OWNCAST_CONFIG_PATH}: {e}"
            )
            return None

        # Check the response code is success
        if response.status != 200:
            self.log.warning(
                f"[{domain}] Response to request on "
                f"{OWNCAST_CONFIG_PATH} was not 200, "
                f"got {response.status} instead."
            )
            return None

        # Try to interpret the response as JSON
        try:
            config = json.loads(await response.read())
        except Exception as e:
            self.log.warning(
                f"[{domain}] Rejecting response to request on "
                f"{OWNCAST_CONFIG_PATH} as could not be "
                f"interpreted as JSON: {e}"
            )
            return None

        # Create StreamConfig from response (fields are truncated to max lengths)
        return StreamConfig.from_api_response(config)

    async def validate_instance(self, domain: str) -> bool:
        """Validate that a domain is a valid Owncast instance.

        :param domain: The domain to validate
        :return: True if valid Owncast instance, False otherwise
        """
        state = await self.get_stream_state(domain)
        return state is not None

    async def close(self) -> None:
        """Close the HTTP session."""
        await self.session.close()
