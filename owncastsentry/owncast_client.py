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

from typing import TYPE_CHECKING, Any

import aiohttp

from .models import StreamConfig, StreamState
from .utils import (
    OWNCAST_CONFIG_PATH,
    OWNCAST_STATUS_PATH,
    REQUIRED_STATUS_FIELDS,
    user_agent,
)

if TYPE_CHECKING:
    import logging

    from .metrics import MetricsService


class OwncastClient:
    """HTTP client for communicating with Owncast instances."""

    def __init__(
        self,
        logger: logging.Logger,
        version: str,
        metrics: MetricsService,
    ) -> None:
        """Initialize the Owncast client with an HTTP session.

        :param logger: Logger instance for debugging
        :param version: Plugin version string for the User-Agent header
        :param metrics: Metrics service for recording response times.
        """
        self.log = logger
        self.metrics = metrics

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

    async def _fetch_json(self, domain: str, path: str) -> dict[str, Any] | None:
        """Fetch and parse JSON from an Owncast API endpoint.

        :param domain: The domain to query.
        :param path: The API path to request.
        :return: Parsed JSON response, or None on error.
        """
        url = f"https://{domain}{path}"
        try:
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status != 200:
                    self.log.warning(
                        f"[{domain}] Response to request on "
                        f"{path} was not 200, "
                        f"got {response.status} instead."
                    )
                    return None
                try:
                    result: dict[str, Any] = await response.json()
                    return result
                except (ValueError, aiohttp.ContentTypeError) as e:
                    self.log.warning(
                        f"[{domain}] Rejecting response to request on "
                        f"{path} as could not be "
                        f"interpreted as JSON: {e}"
                    )
                    return None
        except (aiohttp.ClientError, TimeoutError, OSError) as e:
            self.log.warning(f"[{domain}] Error making GET request to {path}: {e}")
            return None

    async def get_stream_state(self, domain: str) -> StreamState | None:
        """Get the current stream state for a given domain.

        HTTPS on port 443 is assumed, no other protocols or ports
        are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A StreamState if available, None on error.
        """
        self.log.debug(f"[{domain}] Fetching current stream state...")
        with self.metrics.response_timer(domain) as timer:
            new_state = await self._fetch_json(domain, OWNCAST_STATUS_PATH)

            if new_state is None:
                return None

            # Validate the response contains all basic info needed
            missing = REQUIRED_STATUS_FIELDS - new_state.keys()
            if missing:
                self.log.warning(
                    f"[{domain}] Rejecting response to request on "
                    f"{OWNCAST_STATUS_PATH} as it is missing "
                    f"fields: {', '.join(sorted(missing))}"
                )
                return None

            timer.success()
            return StreamState.from_api_response(new_state, domain)

    async def get_stream_config(self, domain: str) -> StreamConfig | None:
        """Get the current stream config for a given domain.

        HTTPS on port 443 is assumed, no other protocols or ports
        are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A StreamConfig, or None if fetch failed.
        """
        self.log.debug(f"[{domain}] Fetching current stream config...")
        with self.metrics.response_timer(domain) as timer:
            config = await self._fetch_json(domain, OWNCAST_CONFIG_PATH)
            if config is None:
                return None

            timer.success()
            return StreamConfig.from_api_response(config)

    async def validate_instance(self, domain: str) -> bool:
        """Validate that a domain is a valid Owncast instance.

        :param domain: The domain to validate
        :return: True if valid Owncast instance, False otherwise
        """
        state = await self.get_stream_state(domain)
        return state is not None

    @property
    def open_connection_count(self) -> int:
        """Return the total number of open HTTP connections."""
        connector = self.session.connector
        if connector is None:
            return 0
        idle = sum(len(conns) for conns in connector._conns.values())
        active = len(connector._acquired)
        return idle + active

    async def close(self) -> None:
        """Close the HTTP session."""
        await self.session.close()
