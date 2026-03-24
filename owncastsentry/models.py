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

"""Data models for OwncastSentry."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .utils import (
    MAX_INSTANCE_TITLE_LENGTH,
    MAX_STREAM_TITLE_LENGTH,
    MAX_TAG_LENGTH,
    UNKNOWN_STATUS_THRESHOLD,
    truncate,
)


class StreamStatus(Enum):
    """Represents the status of a stream."""

    ONLINE = "online"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


@dataclass
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
        """Derive stream status from failure count and connect times.

        Returns UNKNOWN if failures exceed the threshold, ONLINE if a
        connect time is present, or OFFLINE otherwise.
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
        """
        return cls(
            domain=domain,
            title=truncate(response.get("streamTitle", ""), MAX_STREAM_TITLE_LENGTH),
            last_connect_time=response.get("lastConnectTime"),
            last_disconnect_time=response.get("lastDisconnectTime"),
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


@dataclass
class UpdateResult:
    """Result of a stream update cycle."""

    total_streams: int
    successful_checks: int
    failed_checks: int

    @property
    def http_healthy(self) -> bool:
        """Determine HTTP health based on update results.

        HTTP is considered healthy if:
        - No streams are subscribed (nothing to check), OR
        - At least one stream check succeeded

        :return: True if HTTP is considered healthy.
        """
        if self.total_streams == 0:
            return True
        return self.successful_checks > 0


@dataclass
class StreamConfig:
    """Represents the configuration of an Owncast stream."""

    name: str = ""
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_api_response(cls, response: dict[str, Any]) -> StreamConfig:
        """Create a StreamConfig from an API response.

        :param response: API response as a dictionary.
        :return: StreamConfig instance.
        """
        # Truncate instance name to max length
        name = truncate(response.get("name", ""), MAX_INSTANCE_TITLE_LENGTH)

        # Truncate each tag to max length
        raw_tags = response.get("tags", [])
        tags = [truncate(tag, MAX_TAG_LENGTH) for tag in raw_tags]

        return cls(name=name, tags=tags)
