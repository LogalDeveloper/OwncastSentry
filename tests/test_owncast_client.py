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

"""Tests for the Owncast HTTP client."""

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
import time_machine
from aioresponses import aioresponses

from owncastsentry.metrics import MetricsService
from owncastsentry.owncast_client import (
    _MAX_JSON_RESPONSE_BYTES,
    OwncastClient,
    _read_limited_response_body,
    _user_agent,
)
from tests.conftest import (
    VALID_CONFIG_RESPONSE,
    VALID_STATUS_RESPONSE,
    generate_metrics_output,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _ChunkedContent:
    """Fake aiohttp response content that yields predefined chunks."""

    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        """Store chunks to return from iter_chunked."""
        self._chunks = chunks

    async def iter_chunked(self, _size: int) -> AsyncIterator[bytes]:
        """Yield chunks using the interface aiohttp exposes."""
        for chunk in self._chunks:
            yield chunk


class _ChunkedResponse:
    """Fake aiohttp response with chunked content."""

    def __init__(
        self,
        chunks: tuple[bytes, ...],
        content_length: int | None = None,
    ) -> None:
        """Store the content stream and optional Content-Length value."""
        self.content = _ChunkedContent(chunks)
        self.content_length = content_length


@pytest.fixture
async def owncast_client() -> AsyncIterator[OwncastClient]:
    """Create an OwncastClient and close it after the test."""
    client = OwncastClient(
        logger=logging.getLogger("test"),
        version="0.0.0",
        metrics=MetricsService(),
    )
    yield client
    await client.close()


class TestUserAgent:
    """User-Agent header construction."""

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            pytest.param(
                "1.2.3",
                "OwncastSentry/1.2.3 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="semver",
            ),
            pytest.param(
                "0.0.0",
                "OwncastSentry/0.0.0 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="zeroed",
            ),
            pytest.param(
                "1.1.1.dev10+gf0146d061.d20260313",
                "OwncastSentry/1.1.1.dev10+gf0146d061.d20260313 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="dev-version",
            ),
        ],
    )
    def test_user_agent(self, version: str, expected: str) -> None:
        """Build a correctly formatted User-Agent header."""
        assert _user_agent(version) == expected


class TestReadLimitedResponseBody:
    """Bounded response body reading."""

    async def test_reads_all_chunks_before_returning(self) -> None:
        """Return the full body when JSON arrives in multiple chunks."""
        response = _ChunkedResponse(
            (
                b'{"streamTitle":',
                b'"hello","online":true}',
            )
        )

        result = await _read_limited_response_body(response)

        assert result == bytearray(b'{"streamTitle":"hello","online":true}')

    async def test_returns_none_when_content_length_is_too_large(self) -> None:
        """Return None when Content-Length is already over the limit."""
        response = _ChunkedResponse(
            (),
            content_length=_MAX_JSON_RESPONSE_BYTES + 1,
        )

        result = await _read_limited_response_body(response)

        assert result is None

    async def test_returns_none_when_streamed_body_is_too_large(self) -> None:
        """Return None when chunked content grows past the limit."""
        response = _ChunkedResponse(
            (
                b"x" * _MAX_JSON_RESPONSE_BYTES,
                b"x",
            )
        )

        result = await _read_limited_response_body(response)

        assert result is None


class TestGetStreamState:
    """Stream observation retrieval from the status API."""

    @time_machine.travel(datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC))
    async def test_returns_state_on_success(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return a StreamStateObservation with correct fields on a valid response."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is not None
        assert result.domain == "stream.logal.dev"
        assert (
            result.title
            == "I think I can do this... Let's start a nuclear reaction - Playing Nucleares!"  # noqa: E501
        )
        assert result.online is False
        assert result.observed_at == "2026-03-13T12:00:00+00:00"

    async def test_returns_none_on_missing_field(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response is missing required fields."""
        incomplete = {"streamTitle": "Test Stream"}
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=json.dumps(incomplete).encode(),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_invalid_field_type(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response has malformed field types."""
        malformed = {
            **VALID_STATUS_RESPONSE,
            "streamTitle": 123,
        }
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=json.dumps(malformed).encode(),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_invalid_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response body is not valid JSON."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=b"not json",
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_non_object_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response JSON is not an object."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=json.dumps([]).encode(),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_oversized_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response body is too large."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=b" " * (_MAX_JSON_RESPONSE_BYTES + 1),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_non_200(self, owncast_client: OwncastClient) -> None:
        """Return None when the response status is not 200."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                status=404,
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_connection_error(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when a connection error occurs."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                exception=ConnectionError(),
            )
            result = await owncast_client.get_stream_state("stream.logal.dev")

        assert result is None


class TestGetStreamConfig:
    """Stream configuration retrieval from the config API."""

    async def test_returns_config_on_success(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return a StreamConfigObservation with fields from a valid response."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=json.dumps(VALID_CONFIG_RESPONSE).encode(),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is not None
        assert result.name == "LogalDeveloper's Live Stream"
        assert result.tags == (
            "video games",
            "chatting",
            "casual",
            "english",
            "streaming",
            "owncast",
            "variety",
        )

    async def test_returns_none_on_invalid_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response body is not valid JSON."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=b"not json",
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_non_object_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response JSON is not an object."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=json.dumps([]).encode(),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_invalid_field_type(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response has malformed field types."""
        malformed = {
            **VALID_CONFIG_RESPONSE,
            "tags": "gaming",
        }
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=json.dumps(malformed).encode(),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_oversized_json(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response body is too large."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=b" " * (_MAX_JSON_RESPONSE_BYTES + 1),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_non_200(self, owncast_client: OwncastClient) -> None:
        """Return None when the response status is not 200."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                status=500,
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None

    async def test_returns_none_on_connection_error(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when a connection error occurs."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                exception=ConnectionError(),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is None


class TestValidateInstance:
    """Owncast instance validation via the status API."""

    async def test_returns_true_for_valid_instance(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return True when the status endpoint returns a valid response."""
        with aioresponses() as mocked:
            mocked.get(
                "https://valid.com/api/status",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            result = await owncast_client.validate_instance("valid.com")

        assert result is True

    async def test_returns_false_for_invalid_instance(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return False when the status endpoint returns a non-200 response."""
        with aioresponses() as mocked:
            mocked.get(
                "https://invalid.com/api/status",
                status=404,
            )
            result = await owncast_client.validate_instance("invalid.com")

        assert result is False


class TestResponseTimeMetrics:
    """Response time gauge recording."""

    async def test_records_on_success(self) -> None:
        """Record response time on a successful request."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        try:
            with aioresponses() as mocked:
                mocked.get(
                    "https://example.com/api/status",
                    body=json.dumps(VALID_STATUS_RESPONSE).encode(),
                )
                await client.get_stream_state("example.com")
            output = generate_metrics_output(metrics)
            assert 'owncastsentry_api_response_seconds{domain="example.com"}' in output
        finally:
            await client.close()

    async def test_no_observation_on_failure(self) -> None:
        """Do not record response time when request fails."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        try:
            with aioresponses() as mocked:
                mocked.get(
                    "https://example.com/api/status",
                    status=500,
                )
                await client.get_stream_state("example.com")
            output = generate_metrics_output(metrics)
            assert (
                'owncastsentry_api_response_seconds{domain="example.com"}' not in output
            )
        finally:
            await client.close()

    async def test_no_observation_on_connection_error(self) -> None:
        """Do not record response time on connection error."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        try:
            with aioresponses() as mocked:
                mocked.get(
                    "https://example.com/api/status",
                    exception=ConnectionError(),
                )
                await client.get_stream_state("example.com")
            output = generate_metrics_output(metrics)
            assert (
                'owncastsentry_api_response_seconds{domain="example.com"}' not in output
            )
        finally:
            await client.close()


class TestOpenConnectionCount:
    """Open connection count."""

    async def test_zero_with_no_requests(self, owncast_client: OwncastClient) -> None:
        """Return zero when no requests have been made."""
        assert owncast_client.open_connection_count == 0

    async def test_zero_after_close(self, owncast_client: OwncastClient) -> None:
        """Return zero after the HTTP session has been closed."""
        await owncast_client.close()

        assert owncast_client.open_connection_count == 0
