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

import pytest

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
    StreamConfig,
    StreamState,
    StreamStatus,
    SubscriptionError,
    UpdateResult,
    _truncate,
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
        """Truncate text that exceeds the maximum length."""
        assert _truncate(text, max_length) == expected


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
            "online": True,
        }
        state = StreamState.from_api_response(response, "example.com")
        assert state.domain == "example.com"
        assert state.title == "My Stream"
        assert state.last_connect_time == "2026-01-01T00:00:00Z"
        assert state.last_disconnect_time == "2025-12-31T23:00:00Z"
        assert state.name is None
        assert state.failure_counter == 0

    def test_missing_required_field_raises(self) -> None:
        """Reject API responses without required stream state fields."""
        with pytest.raises(InvalidApiResponseError):
            StreamState.from_api_response({}, "bare.example.com")

    def test_nullable_timestamp_fields(self) -> None:
        """Accept null values for Owncast timestamp fields."""
        response = {
            "streamTitle": "Offline Stream",
            "lastConnectTime": None,
            "lastDisconnectTime": None,
            "online": False,
        }
        state = StreamState.from_api_response(response, "example.com")
        assert state.last_connect_time is None
        assert state.last_disconnect_time is None

    def test_title_truncation(self) -> None:
        """Truncate the stream title to _MAX_STREAM_TITLE_LENGTH."""
        long_title = "A" * (_MAX_STREAM_TITLE_LENGTH + 50)
        response = {
            "streamTitle": long_title,
            "lastConnectTime": None,
            "lastDisconnectTime": None,
            "online": True,
        }
        state = StreamState.from_api_response(response, "example.com")
        assert len(state.title) == _MAX_STREAM_TITLE_LENGTH
        assert state.title == "A" * _MAX_STREAM_TITLE_LENGTH

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("streamTitle", 123, id="title-not-string"),
            pytest.param("lastConnectTime", [], id="connect-time-not-string-or-null"),
            pytest.param(
                "lastDisconnectTime",
                {},
                id="disconnect-time-not-string-or-null",
            ),
            pytest.param("online", "true", id="online-not-bool"),
        ],
    )
    def test_invalid_field_type_raises(self, field: str, value: object) -> None:
        """Reject stream state responses with malformed field types."""
        response: dict[str, object] = {
            "streamTitle": "My Stream",
            "lastConnectTime": None,
            "lastDisconnectTime": None,
            "online": True,
        }
        response[field] = value

        with pytest.raises(InvalidApiResponseError):
            StreamState.from_api_response(response, "example.com")


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
        assert config.tags == ("gaming", "music")

    def test_missing_keys_defaults(self) -> None:
        """Use defaults when name and tags keys are missing."""
        config = StreamConfig.from_api_response({})
        assert config.name == ""
        assert config.tags == ()

    def test_name_truncation(self) -> None:
        """Truncate the instance name to _MAX_INSTANCE_TITLE_LENGTH."""
        long_name = "B" * (_MAX_INSTANCE_TITLE_LENGTH + 50)
        response = {"name": long_name, "tags": []}
        config = StreamConfig.from_api_response(response)
        assert len(config.name) == _MAX_INSTANCE_TITLE_LENGTH
        assert config.name == "B" * _MAX_INSTANCE_TITLE_LENGTH

    def test_tag_truncation(self) -> None:
        """Truncate each tag to _MAX_TAG_LENGTH."""
        long_tag = "C" * (_MAX_TAG_LENGTH + 10)
        response = {"name": "", "tags": [long_tag, "short"]}
        config = StreamConfig.from_api_response(response)
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
            StreamConfig.from_api_response(response)


class TestValueTypeImmutability:
    """Dataclass value containers are immutable snapshots."""

    def test_stream_state_is_immutable(self) -> None:
        """StreamState cannot be mutated in place."""
        state = StreamState(domain="stream.example")
        with pytest.raises(FrozenInstanceError):
            state.title = "Changed"  # type: ignore[misc]

    def test_stream_config_is_immutable(self) -> None:
        """StreamConfig cannot be mutated in place."""
        config = StreamConfig(name="Stream")
        with pytest.raises(FrozenInstanceError):
            config.name = "Changed"  # type: ignore[misc]

    def test_stream_config_tags_are_immutable(self) -> None:
        """StreamConfig tags are stored in an immutable tuple."""
        config = StreamConfig(name="Stream", tags=("gaming",))
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
