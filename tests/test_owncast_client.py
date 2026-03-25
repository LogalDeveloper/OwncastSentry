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
from typing import TYPE_CHECKING

import pytest
from aioresponses import aioresponses

from owncastsentry.metrics import MetricsService
from owncastsentry.owncast_client import OwncastClient
from tests.conftest import (
    VALID_CONFIG_RESPONSE,
    VALID_STATUS_RESPONSE,
    generate_metrics_output,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


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


class TestGetStreamState:
    """Stream state retrieval from the status API."""

    async def test_returns_state_on_success(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return a StreamState with correct fields on a valid 200 response."""
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
        assert result.last_connect_time is None
        assert result.last_disconnect_time == "2026-03-04T21:05:32-05:00"

    async def test_returns_none_on_missing_field(
        self, owncast_client: OwncastClient
    ) -> None:
        """Return None when the response is missing required fields."""
        incomplete = {"streamTitle": "Test Stream", "online": True}
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/status",
                body=json.dumps(incomplete).encode(),
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
        """Return a StreamConfig with correct fields on a valid 200 response."""
        with aioresponses() as mocked:
            mocked.get(
                "https://stream.logal.dev/api/config",
                body=json.dumps(VALID_CONFIG_RESPONSE).encode(),
            )
            result = await owncast_client.get_stream_config("stream.logal.dev")

        assert result is not None
        assert result.name == "LogalDeveloper's Live Stream"
        assert result.tags == [
            "video games",
            "chatting",
            "casual",
            "english",
            "streaming",
            "owncast",
            "variety",
        ]

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
    """Response time histogram recording."""

    async def test_records_on_success(self) -> None:
        """Record response time on a successful request."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        with aioresponses() as mocked:
            mocked.get(
                "https://example.com/api/status",
                body=json.dumps(VALID_STATUS_RESPONSE).encode(),
            )
            await client.get_stream_state("example.com")
        output = generate_metrics_output(metrics)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' in output
        await client.close()

    async def test_no_observation_on_failure(self) -> None:
        """Do not record response time when request fails."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        with aioresponses() as mocked:
            mocked.get(
                "https://example.com/api/status",
                status=500,
            )
            await client.get_stream_state("example.com")
        output = generate_metrics_output(metrics)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' not in output
        await client.close()

    async def test_no_observation_on_connection_error(self) -> None:
        """Do not record response time on connection error."""
        metrics = MetricsService()
        client = OwncastClient(
            logger=logging.getLogger("test"),
            version="0.0.0",
            metrics=metrics,
        )
        with aioresponses() as mocked:
            mocked.get(
                "https://example.com/api/status",
                exception=ConnectionError(),
            )
            await client.get_stream_state("example.com")
        output = generate_metrics_output(metrics)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' not in output
        await client.close()


class TestOpenConnectionCount:
    """Open connection count."""

    async def test_zero_with_no_requests(self, owncast_client: OwncastClient) -> None:
        """Return zero when no requests have been made."""
        assert owncast_client.open_connection_count == 0
