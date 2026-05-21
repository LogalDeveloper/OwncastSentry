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

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest
import time_machine

from owncastsentry.types import (
    _MAX_INSTANCE_TITLE_LENGTH,
    _MAX_STREAM_TITLE_LENGTH,
    _MAX_TAG_LENGTH,
    UNKNOWN_STATUS_THRESHOLD,
    AlreadySubscribedError,
    InvalidApiResponseError,
    InvalidOwncastInstanceError,
    NotSubscribedError,
    RoomSubscription,
    StreamConfigObservation,
    StreamState,
    StreamStateObservation,
    StreamStatus,
    SubscriptionError,
    UpdateResult,
    _truncate,
    format_status_since,
)


class TestTruncate:
    """Text truncation to a maximum length."""

    @pytest.mark.parametrize(
        ("text", "max_length", "expected"),
        [
            pytest.param("hello", 10, "hello", id="under-limit"),
            pytest.param("hello", 5, "hello", id="exact-limit"),
            pytest.param("hello world", 5, "hello", id="over-limit"),
            pytest.param("", 5, "", id="empty-string"),
        ],
    )
    def test_truncates(self, text: str, max_length: int, expected: str) -> None:
        """Return text unchanged within the limit and truncate over-limit text."""
        assert _truncate(text, max_length) == expected


class TestStreamStateStatus:
    """Stream status derivation from state fields."""

    @pytest.mark.parametrize(
        ("failure_counter", "online", "expected"),
        [
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD + 1,
                False,
                StreamStatus.UNKNOWN,
                id="above-threshold-offline-returns-unknown",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD + 1,
                True,
                StreamStatus.UNKNOWN,
                id="above-threshold-online-returns-unknown",
            ),
            pytest.param(
                0,
                True,
                StreamStatus.ONLINE,
                id="zero-failures-online-returns-online",
            ),
            pytest.param(
                0,
                False,
                StreamStatus.OFFLINE,
                id="zero-failures-offline-returns-offline",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD,
                True,
                StreamStatus.ONLINE,
                id="at-threshold-online-returns-online",
            ),
            pytest.param(
                UNKNOWN_STATUS_THRESHOLD,
                False,
                StreamStatus.OFFLINE,
                id="at-threshold-offline-returns-offline",
            ),
        ],
    )
    def test_status(
        self,
        failure_counter: int,
        online: object,
        expected: StreamStatus,
    ) -> None:
        """Return the correct status based on failure counter and online state."""
        state = StreamState(
            domain="example.com",
            failure_counter=failure_counter,
            online=online is True,
        )
        assert state.status is expected


class TestFormatStatusSince:
    """Status timestamp formatting."""

    def test_formats_as_utc_iso_seconds(self) -> None:
        """Normalize aware datetimes to UTC with second precision."""
        timestamp = datetime(
            2026,
            1,
            1,
            12,
            34,
            56,
            123456,
            tzinfo=timezone(timedelta(hours=-5)),
        )

        assert format_status_since(timestamp) == "2026-01-01T17:34:56+00:00"

    def test_treats_naive_as_utc(self) -> None:
        """Format naive datetimes as UTC."""
        timestamp = datetime.fromisoformat("2026-01-01T12:34:56.123456")

        assert format_status_since(timestamp) == "2026-01-01T12:34:56+00:00"


class TestStreamStateObservationFromApiResponse:
    """StreamStateObservation construction from an API response dictionary."""

    def test_typical_response(self) -> None:
        """Populate API-derived fields from a complete status response."""
        observed_at = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        response = {
            "streamTitle": "My Stream",
            "online": True,
        }
        with time_machine.travel(observed_at, tick=False):
            observation = StreamStateObservation.from_api_response(
                response, "example.com"
            )
        assert observation.domain == "example.com"
        assert observation.title == "My Stream"
        assert observation.online is True
        assert observation.observed_at == "2026-01-01T00:00:01+00:00"

    def test_missing_required_field_raises(self) -> None:
        """Reject API responses without required stream state fields."""
        with pytest.raises(InvalidApiResponseError):
            StreamStateObservation.from_api_response({}, "bare.example.com")

    def test_offline_response(self) -> None:
        """Populate offline state from a status response."""
        observed_at = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        response = {
            "streamTitle": "Offline Stream",
            "online": False,
        }
        with time_machine.travel(observed_at, tick=False):
            observation = StreamStateObservation.from_api_response(
                response, "example.com"
            )
        assert observation.online is False
        assert observation.observed_at == "2026-01-01T00:00:01+00:00"

    def test_title_truncation(self) -> None:
        """Truncate the stream title to _MAX_STREAM_TITLE_LENGTH."""
        observed_at = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        long_title = "A" * (_MAX_STREAM_TITLE_LENGTH + 50)
        response = {
            "streamTitle": long_title,
            "online": True,
        }
        with time_machine.travel(observed_at, tick=False):
            observation = StreamStateObservation.from_api_response(
                response, "example.com"
            )
        assert len(observation.title) == _MAX_STREAM_TITLE_LENGTH
        assert observation.title == "A" * _MAX_STREAM_TITLE_LENGTH

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("streamTitle", 123, id="title-not-string"),
            pytest.param("online", "true", id="online-not-bool"),
        ],
    )
    def test_invalid_field_type_raises(self, field: str, value: object) -> None:
        """Reject stream state responses with malformed field types."""
        response: dict[str, object] = {
            "streamTitle": "My Stream",
            "online": True,
        }
        response[field] = value

        with pytest.raises(InvalidApiResponseError):
            StreamStateObservation.from_api_response(response, "example.com")


class TestStreamStateFromDbRow:
    """StreamState construction from a database row dictionary."""

    def test_typical_row(self) -> None:
        """Populate all fields from a complete database row."""
        row = {
            "domain": "example.com",
            "name": "Test Instance",
            "title": "Live Now",
            "online": True,
            "status_since": "2026-01-01T00:00:00+00:00",
            "failure_counter": 3,
        }
        state = StreamState.from_db_row(row)
        assert state.domain == "example.com"
        assert state.name == "Test Instance"
        assert state.title == "Live Now"
        assert state.online is True
        assert state.status_since == "2026-01-01T00:00:00+00:00"
        assert state.failure_counter == 3

    def test_row_with_none_optional_fields(self) -> None:
        """Accept None for optional fields in a database row."""
        row = {
            "domain": "example.com",
            "name": None,
            "title": None,
            "online": False,
            "status_since": None,
            "failure_counter": 0,
        }
        state = StreamState.from_db_row(row)
        assert state.domain == "example.com"
        assert state.name is None
        assert state.title is None
        assert state.online is False
        assert state.status_since is None
        assert state.failure_counter == 0


class TestStreamConfigObservationFromApiResponse:
    """StreamConfigObservation construction from an API response dictionary."""

    def test_typical_response(self) -> None:
        """Populate name and tags from a complete API response."""
        response = {"name": "My Instance", "tags": ["gaming", "music"]}
        observed_at = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        with time_machine.travel(observed_at, tick=False):
            config = StreamConfigObservation.from_api_response(response)
        assert config.name == "My Instance"
        assert config.tags == ("gaming", "music")
        assert config.observed_at == "2026-01-01T00:00:01+00:00"

    def test_missing_keys_defaults(self) -> None:
        """Use defaults when name and tags keys are missing."""
        config = StreamConfigObservation.from_api_response({})
        assert config.name == ""
        assert config.tags == ()

    def test_name_truncation(self) -> None:
        """Truncate the instance name to _MAX_INSTANCE_TITLE_LENGTH."""
        long_name = "B" * (_MAX_INSTANCE_TITLE_LENGTH + 50)
        response = {"name": long_name, "tags": []}
        config = StreamConfigObservation.from_api_response(response)
        assert len(config.name) == _MAX_INSTANCE_TITLE_LENGTH
        assert config.name == "B" * _MAX_INSTANCE_TITLE_LENGTH

    def test_tag_truncation(self) -> None:
        """Truncate each tag to _MAX_TAG_LENGTH."""
        long_tag = "C" * (_MAX_TAG_LENGTH + 10)
        response = {"name": "", "tags": [long_tag, "short"]}
        config = StreamConfigObservation.from_api_response(response)
        assert len(config.tags[0]) == _MAX_TAG_LENGTH
        assert config.tags[0] == "C" * _MAX_TAG_LENGTH
        assert config.tags[1] == "short"

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("name", None, id="name-not-string"),
            pytest.param("tags", "gaming", id="tags-not-list"),
            pytest.param("tags", ["gaming", 123], id="tag-not-string"),
        ],
    )
    def test_invalid_field_type_raises(self, field: str, value: object) -> None:
        """Reject stream config responses with malformed field types."""
        response: dict[str, object] = {"name": "My Instance", "tags": ["gaming"]}
        response[field] = value

        with pytest.raises(InvalidApiResponseError):
            StreamConfigObservation.from_api_response(response)


class TestValueTypeImmutability:
    """Dataclass value containers are immutable snapshots."""

    def test_stream_state_is_immutable(self) -> None:
        """StreamState cannot be mutated in place."""
        state = StreamState(domain="stream.example")
        with pytest.raises(FrozenInstanceError):
            state.title = "Changed"  # type: ignore[misc]

    def test_stream_observation_is_immutable(self) -> None:
        """StreamStateObservation cannot be mutated in place."""
        observation = StreamStateObservation(
            domain="stream.example",
        )
        with pytest.raises(FrozenInstanceError):
            observation.title = "Changed"  # type: ignore[misc]

    def test_stream_config_is_immutable(self) -> None:
        """StreamConfigObservation cannot be mutated in place."""
        config = StreamConfigObservation(name="Stream")
        with pytest.raises(FrozenInstanceError):
            config.name = "Changed"  # type: ignore[misc]

    def test_stream_config_tags_are_immutable(self) -> None:
        """StreamConfigObservation tags are stored in an immutable tuple."""
        config = StreamConfigObservation(name="Stream", tags=("gaming",))
        assert config.tags == ("gaming",)

    def test_update_result_is_immutable(self) -> None:
        """UpdateResult cannot be mutated in place."""
        result = UpdateResult(total_streams=1, successful_checks=1, failed_checks=0)
        with pytest.raises(FrozenInstanceError):
            result.failed_checks = 1  # type: ignore[misc]


class TestSubscriptionTypes:
    """Subscription display containers and domain error hierarchy."""

    @pytest.mark.parametrize(
        "error_cls",
        [
            pytest.param(InvalidOwncastInstanceError, id="invalid-instance"),
            pytest.param(AlreadySubscribedError, id="already-subscribed"),
            pytest.param(NotSubscribedError, id="not-subscribed"),
        ],
    )
    def test_errors_subclass_subscription_error(
        self, error_cls: type[Exception]
    ) -> None:
        """Every subscription domain error subclasses SubscriptionError."""
        assert issubclass(error_cls, SubscriptionError)

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                InvalidOwncastInstanceError("bad.example"),
                id="invalid-instance",
            ),
            pytest.param(
                AlreadySubscribedError("dupe.example"),
                id="already-subscribed",
            ),
            pytest.param(
                NotSubscribedError("missing.example"),
                id="not-subscribed",
            ),
        ],
    )
    def test_errors_store_domain(self, error: SubscriptionError) -> None:
        """Subscription domain errors expose the stream domain that failed."""
        assert error.domain in str(error)

    def test_room_subscription_is_immutable(self) -> None:
        """RoomSubscription is an immutable stream display snapshot."""
        state = StreamState(domain="stream.example")
        subscription = RoomSubscription(domain="stream.example", stream_state=state)
        with pytest.raises(FrozenInstanceError):
            subscription.domain = "other.example"  # type: ignore[misc]
