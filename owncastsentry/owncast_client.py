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
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp

from .types import InvalidApiResponseError, StreamConfig, StreamState

if TYPE_CHECKING:
    import logging

    from .metrics import MetricsService


_OWNCAST_STATUS_PATH = "/api/status"
_OWNCAST_CONFIG_PATH = "/api/config"
_MAX_JSON_RESPONSE_BYTES = 1024 * 1024
_JSON_READ_CHUNK_BYTES = 64 * 1024
_HTTP_CONNECTION_LIMIT = 1000
_HTTP_CONNECTION_LIMIT_PER_HOST = 1
_HTTP_KEEPALIVE_TIMEOUT_SECONDS = 120
_HTTP_CONNECT_TIMEOUT_SECONDS = 5
_HTTP_READ_TIMEOUT_SECONDS = 5


def _user_agent(version: str) -> str:
    """Build the User-Agent header string for HTTP requests."""
    return (
        f"OwncastSentry/{version}"
        " (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)"
    )


async def _read_limited_response_body(
    response: aiohttp.ClientResponse,
) -> bytearray | None:
    """Read a response body while enforcing the maximum JSON response size."""
    # Check Content-Length first when the server provides it so clearly
    # oversized responses can be rejected before buffering any body bytes.
    if (
        response.content_length is not None
        and response.content_length > _MAX_JSON_RESPONSE_BYTES
    ):
        return None

    body = bytearray()
    # Read until EOF instead of using one read(n) call. aiohttp's read(n)
    # may return a partial body as soon as data is available.
    async for chunk in response.content.iter_chunked(_JSON_READ_CHUNK_BYTES):
        body.extend(chunk)
        if len(body) > _MAX_JSON_RESPONSE_BYTES:
            return None

    return body


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
        headers = {"User-Agent": _user_agent(version)}
        cookie_jar = aiohttp.DummyCookieJar()
        connector = aiohttp.TCPConnector(
            use_dns_cache=False,
            limit=_HTTP_CONNECTION_LIMIT,
            limit_per_host=_HTTP_CONNECTION_LIMIT_PER_HOST,
            keepalive_timeout=_HTTP_KEEPALIVE_TIMEOUT_SECONDS,
        )
        timeout = aiohttp.ClientTimeout(
            sock_connect=_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=_HTTP_READ_TIMEOUT_SECONDS,
        )

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
        self.log.debug("[%s] Fetching current stream state...", domain)
        with self.metrics.response_timer(domain) as timer:
            new_state = await self._fetch_json(domain, _OWNCAST_STATUS_PATH)

            if new_state is None:
                return None

            observed_at = datetime.now(UTC)
            try:
                stream_state = StreamState.from_api_response(
                    new_state, domain, observed_at
                )
            except InvalidApiResponseError as e:
                self.log.warning(
                    "[%s] Rejecting response to request on %s as response "
                    "shape is invalid: %s",
                    domain,
                    _OWNCAST_STATUS_PATH,
                    e,
                )
                return None

            timer.success()
            return stream_state

    async def get_stream_config(self, domain: str) -> StreamConfig | None:
        """Get the current stream config for a given domain.

        HTTPS on port 443 is assumed, no other protocols or ports
        are supported.

        :param domain: The domain (not URL) where the stream is hosted.
        :return: A StreamConfig, or None if fetch failed.
        """
        self.log.debug("[%s] Fetching current stream config...", domain)
        with self.metrics.response_timer(domain) as timer:
            config = await self._fetch_json(domain, _OWNCAST_CONFIG_PATH)
            if config is None:
                return None

            try:
                stream_config = StreamConfig.from_api_response(config)
            except InvalidApiResponseError as e:
                self.log.warning(
                    "[%s] Rejecting response to request on %s as response "
                    "shape is invalid: %s",
                    domain,
                    _OWNCAST_CONFIG_PATH,
                    e,
                )
                return None

            timer.success()
            return stream_config

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

        # aiohttp does not expose public live pool counts; this metric intentionally
        # mirrors its internal idle and active connection pools.
        idle = sum(len(conns) for conns in connector._conns.values())  # noqa: SLF001
        active = len(connector._acquired)  # noqa: SLF001
        return idle + active

    async def close(self) -> None:
        """Close the HTTP session."""
        await self.session.close()

    async def _fetch_json(self, domain: str, path: str) -> dict[str, Any] | None:
        """Fetch and parse JSON from an Owncast API endpoint.

        :param domain: The domain to query.
        :param path: The API path to request.
        :return: Parsed JSON response, or None on error.
        """
        url = f"https://{domain}{path}"
        try:
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status != HTTPStatus.OK:
                    self.log.warning(
                        "[%s] Response to request on %s was not 200, got %s instead.",
                        domain,
                        path,
                        response.status,
                    )
                    return None
                try:
                    body = await _read_limited_response_body(response)
                    if body is None:
                        self.log.warning(
                            "[%s] Rejecting response to request on %s as it "
                            "was larger than %s bytes.",
                            domain,
                            path,
                            _MAX_JSON_RESPONSE_BYTES,
                        )
                        return None

                    result = json.loads(body)
                    if not isinstance(result, dict):
                        self.log.warning(
                            "[%s] Rejecting response to request on %s as JSON "
                            "was not an object.",
                            domain,
                            path,
                        )
                        return None
                except ValueError as e:
                    self.log.warning(
                        "[%s] Rejecting response to request on %s as could not "
                        "be interpreted as JSON: %s",
                        domain,
                        path,
                        e,
                    )
                    return None
                else:
                    return result
        except (aiohttp.ClientError, TimeoutError, OSError) as e:
            self.log.warning("[%s] Error making GET request to %s: %s", domain, path, e)
            return None
