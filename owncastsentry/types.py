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

"""Data containers and domain errors for OwncastSentry."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

UNKNOWN_STATUS_THRESHOLD = 15

# Maximum field lengths based on Owncast's admin config constants.
# Source: https://github.com/owncast/owncast/blob/master/web/utils/config-constants.tsx
_MAX_INSTANCE_TITLE_LENGTH = 255  # TEXTFIELD_PROPS_SERVER_NAME maxLength
_MAX_STREAM_TITLE_LENGTH = 100  # TEXTFIELD_PROPS_STREAM_TITLE maxLength
_MAX_TAG_LENGTH = 24  # tag maxLength


class InvalidApiResponseError(ValueError):
    """The Owncast API response did not match the expected shape."""


def _require_field(response: dict[str, Any], field: str) -> Any:
    """Return a required API response field or raise if missing."""
    try:
        return response[field]
    except KeyError as e:
        raise InvalidApiResponseError(f"missing field: {field}") from e


def _require_str(response: dict[str, Any], field: str) -> str:
    """Return a required string API response field."""
    value = _require_field(response, field)
    if not isinstance(value, str):
        raise InvalidApiResponseError(f"{field} must be a string")
    return value


def _require_nullable_str(response: dict[str, Any], field: str) -> str | None:
    """Return a required nullable string API response field."""
    value = _require_field(response, field)
    if value is not None and not isinstance(value, str):
        raise InvalidApiResponseError(f"{field} must be a string or null")
    return value


def _optional_config_str(response: dict[str, Any], field: str) -> str:
    """Return an optional config string, defaulting to empty when absent."""
    value = response.get(field, "")
    if not isinstance(value, str):
        raise InvalidApiResponseError(f"{field} must be a string")
    return value


def _optional_tag_list(response: dict[str, Any]) -> list[str]:
    """Return optional config tags, defaulting to an empty list when absent."""
    value = response.get("tags", [])
    if not isinstance(value, list):
        raise InvalidApiResponseError("tags must be a list")
    if not all(isinstance(tag, str) for tag in value):
        raise InvalidApiResponseError("tags must contain only strings")
    return value


def _truncate(text: str, max_length: int) -> str:
    """Truncate text to a maximum length."""
    if len(text) <= max_length:
        return text
    return text[:max_length]


class StreamStatus(Enum):
    """Represents the status of a stream."""

    ONLINE = "online"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class StreamState:
    """Represents the state of an Owncast stream."""

    domain: str
    name: str | None = None
    title: str | None = None
    last_connect_time: str | None = None
    last_disconnect_time: str | None = None
    failure_counter: int = 0

    @property
    def status(self) -> StreamStatus:
        """Derive stream status from failure count and last connect time.

        Returns UNKNOWN if failures exceed the threshold, ONLINE if a
        last connect time is present, or OFFLINE otherwise.
        """
        if self.failure_counter > UNKNOWN_STATUS_THRESHOLD:
            return StreamStatus.UNKNOWN
        if self.last_connect_time is not None:
            return StreamStatus.ONLINE
        return StreamStatus.OFFLINE

    @classmethod
    def from_api_response(cls, response: dict[str, Any], domain: str) -> StreamState:
        """Create a StreamState from an API response.

        :param response: API response as a dictionary (camelCase keys).
        :param domain: The stream domain.
        :return: StreamState instance.
        :raises InvalidApiResponseError: If the response shape is invalid.
        """
        stream_title = _require_str(response, "streamTitle")
        last_connect_time = _require_nullable_str(response, "lastConnectTime")
        last_disconnect_time = _require_nullable_str(response, "lastDisconnectTime")
        online = _require_field(response, "online")
        if not isinstance(online, bool):
            raise InvalidApiResponseError("online must be a boolean")

        return cls(
            domain=domain,
            title=_truncate(stream_title, _MAX_STREAM_TITLE_LENGTH),
            last_connect_time=last_connect_time,
            last_disconnect_time=last_disconnect_time,
        )

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> StreamState:
        """Create a StreamState from a database row.

        :param row: Database row as a dictionary.
        :return: StreamState instance.
        """
        return cls(
            domain=row["domain"],
            name=row["name"],
            title=row["title"],
            last_connect_time=row["last_connect_time"],
            last_disconnect_time=row["last_disconnect_time"],
            failure_counter=row["failure_counter"],
        )


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Represents the configuration of an Owncast stream."""

    name: str = ""
    tags: tuple[str, ...] = ()

    @classmethod
    def from_api_response(cls, response: dict[str, Any]) -> StreamConfig:
        """Create a StreamConfig from an API response.

        :param response: API response as a dictionary.
        :return: StreamConfig instance.
        :raises InvalidApiResponseError: If the response shape is invalid.
        """
        # Apply Owncast's instance name limit.
        name = _truncate(
            _optional_config_str(response, "name"), _MAX_INSTANCE_TITLE_LENGTH
        )

        # Apply Owncast's per-tag limit.
        raw_tags = _optional_tag_list(response)
        tags = tuple([_truncate(tag, _MAX_TAG_LENGTH) for tag in raw_tags])

        return cls(name=name, tags=tags)


@dataclass(frozen=True, slots=True)
class UpdateResult:
    """Result of a stream update cycle."""

    total_streams: int
    successful_checks: int
    failed_checks: int


class SubscriptionError(Exception):
    """Base class for subscription domain errors."""


class InvalidOwncastInstanceError(SubscriptionError):
    """The requested domain is not a reachable Owncast instance."""

    def __init__(self, domain: str) -> None:
        """Initialize with the rejected stream domain."""
        self.domain = domain
        super().__init__(f"invalid Owncast instance: {domain}")


class AlreadySubscribedError(SubscriptionError):
    """The room is already subscribed to the stream."""

    def __init__(self, domain: str) -> None:
        """Initialize with the duplicate stream domain."""
        self.domain = domain
        super().__init__(f"already subscribed: {domain}")


class NotSubscribedError(SubscriptionError):
    """The room is not subscribed to the stream."""

    def __init__(self, domain: str) -> None:
        """Initialize with the missing stream domain."""
        self.domain = domain
        super().__init__(f"not subscribed: {domain}")


@dataclass(frozen=True, slots=True)
class RoomSubscription:
    """A stream subscription resolved with the stream state used for display."""

    domain: str
    stream_state: StreamState
