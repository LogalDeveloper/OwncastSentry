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

"""Tests for data models."""

from __future__ import annotations

import pytest

from owncastsentry.models import StreamConfig, StreamState, StreamStatus
from owncastsentry.utils import (
    MAX_INSTANCE_TITLE_LENGTH,
    MAX_STREAM_TITLE_LENGTH,
    MAX_TAG_LENGTH,
    UNKNOWN_STATUS_THRESHOLD,
)


class TestStreamStateStatus:
    """Stream status derivation from state fields."""

    @pytest.mark.parametrize(
        ("failure_counter", "last_connect_time", "expected"),
        [
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD + 1,
                None,
                StreamStatus.UNKNOWN,
                id="above-threshold-offline-returns-unknown",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD + 1,
                "2026-01-01T00:00:00Z",
                StreamStatus.UNKNOWN,
                id="above-threshold-online-returns-unknown",
            ),
            pytest.param(
                0,
                "2026-01-01T00:00:00Z",
                StreamStatus.ONLINE,
                id="zero-failures-with-connect-time-returns-online",
            ),
            pytest.param(
                0,
                None,
                StreamStatus.OFFLINE,
                id="zero-failures-no-connect-time-returns-offline",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD,
                "2026-01-01T00:00:00Z",
                StreamStatus.ONLINE,
                id="at-threshold-with-connect-time-returns-online",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD,
                None,
                StreamStatus.OFFLINE,
                id="at-threshold-no-connect-time-returns-offline",
            ),
        ],
    )
    def test_status(
        self,
        failure_counter: int,
        last_connect_time: str | None,
        expected: StreamStatus,
    ) -> None:
        """Return the correct status based on failure counter and connect time."""
        state = StreamState(
            domain="example.com",
            failure_counter=failure_counter,
            last_connect_time=last_connect_time,
        )
        assert state.status is expected


class TestStreamStateFromApiResponse:
    """StreamState construction from an API response dictionary."""

    def test_typical_response(self) -> None:
        """Populate all fields from a complete API response."""
        response = {
            "streamTitle": "My Stream",
            "lastConnectTime": "2026-01-01T00:00:00Z",
            "lastDisconnectTime": "2025-12-31T23:00:00Z",
        }
        state = StreamState.from_api_response(response, "example.com")
        assert state.domain == "example.com"
        assert state.title == "My Stream"
        assert state.last_connect_time == "2026-01-01T00:00:00Z"
        assert state.last_disconnect_time == "2025-12-31T23:00:00Z"
        assert state.name is None
        assert state.failure_counter == 0

    def test_empty_response_defaults(self) -> None:
        """Use defaults when optional fields are missing."""
        state = StreamState.from_api_response({}, "bare.example.com")
        assert state.domain == "bare.example.com"
        assert state.title == ""
        assert state.last_connect_time is None
        assert state.last_disconnect_time is None

    def test_title_truncation(self) -> None:
        """Truncate the stream title to MAX_STREAM_TITLE_LENGTH."""
        long_title = "A" * (MAX_STREAM_TITLE_LENGTH + 50)
        response = {"streamTitle": long_title}
        state = StreamState.from_api_response(response, "example.com")
        assert len(state.title) == MAX_STREAM_TITLE_LENGTH
        assert state.title == "A" * MAX_STREAM_TITLE_LENGTH


class TestStreamStateFromDbRow:
    """StreamState construction from a database row dictionary."""

    def test_typical_row(self) -> None:
        """Populate all fields from a complete database row."""
        row = {
            "domain": "example.com",
            "name": "Test Instance",
            "title": "Live Now",
            "last_connect_time": "2026-01-01T00:00:00Z",
            "last_disconnect_time": "2025-12-31T23:00:00Z",
            "failure_counter": 3,
        }
        state = StreamState.from_db_row(row)
        assert state.domain == "example.com"
        assert state.name == "Test Instance"
        assert state.title == "Live Now"
        assert state.last_connect_time == "2026-01-01T00:00:00Z"
        assert state.last_disconnect_time == "2025-12-31T23:00:00Z"
        assert state.failure_counter == 3

    def test_row_with_none_optional_fields(self) -> None:
        """Accept None for optional fields in a database row."""
        row = {
            "domain": "example.com",
            "name": None,
            "title": None,
            "last_connect_time": None,
            "last_disconnect_time": None,
            "failure_counter": 0,
        }
        state = StreamState.from_db_row(row)
        assert state.domain == "example.com"
        assert state.name is None
        assert state.title is None
        assert state.last_connect_time is None
        assert state.last_disconnect_time is None
        assert state.failure_counter == 0


class TestStreamConfigFromApiResponse:
    """StreamConfig construction from an API response dictionary."""

    def test_typical_response(self) -> None:
        """Populate name and tags from a complete API response."""
        response = {"name": "My Instance", "tags": ["gaming", "music"]}
        config = StreamConfig.from_api_response(response)
        assert config.name == "My Instance"
        assert config.tags == ["gaming", "music"]

    def test_missing_keys_defaults(self) -> None:
        """Use defaults when name and tags keys are missing."""
        config = StreamConfig.from_api_response({})
        assert config.name == ""
        assert config.tags == []

    def test_name_truncation(self) -> None:
        """Truncate the instance name to MAX_INSTANCE_TITLE_LENGTH."""
        long_name = "B" * (MAX_INSTANCE_TITLE_LENGTH + 50)
        response = {"name": long_name, "tags": []}
        config = StreamConfig.from_api_response(response)
        assert len(config.name) == MAX_INSTANCE_TITLE_LENGTH
        assert config.name == "B" * MAX_INSTANCE_TITLE_LENGTH

    def test_tag_truncation(self) -> None:
        """Truncate each tag to MAX_TAG_LENGTH."""
        long_tag = "C" * (MAX_TAG_LENGTH + 10)
        response = {"name": "", "tags": [long_tag, "short"]}
        config = StreamConfig.from_api_response(response)
        assert len(config.tags[0]) == MAX_TAG_LENGTH
        assert config.tags[0] == "C" * MAX_TAG_LENGTH
        assert config.tags[1] == "short"
