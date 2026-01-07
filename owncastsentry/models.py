# Copyright 2026 Logan Fick
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

from dataclasses import dataclass
from typing import Optional, List


@dataclass
class StreamState:
    """Represents the state of an Owncast stream."""

    domain: str
    name: Optional[str] = None
    title: Optional[str] = None
    last_connect_time: Optional[str] = None
    last_disconnect_time: Optional[str] = None
    failure_counter: int = 0

    @property
    def online(self) -> bool:
        """Returns True if the stream is currently online."""
        return self.last_connect_time is not None

    @classmethod
    def from_api_response(cls, response: dict, domain: str) -> "StreamState":
        """
        Creates a StreamState from an API response.

        :param response: API response as a dictionary (camelCase keys)
        :param domain: The stream domain
        :return: StreamState instance
        """
        from .utils import truncate, MAX_STREAM_TITLE_LENGTH

        return cls(
            domain=domain,
            title=truncate(response.get("streamTitle", ""), MAX_STREAM_TITLE_LENGTH),
            last_connect_time=response.get("lastConnectTime"),
            last_disconnect_time=response.get("lastDisconnectTime"),
        )

    @classmethod
    def from_db_row(cls, row: dict) -> "StreamState":
        """
        Creates a StreamState from a database row.

        :param row: Database row as a dictionary
        :return: StreamState instance
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
class StreamConfig:
    """Represents the configuration of an Owncast stream."""

    name: str = ""
    tags: List[str] = None

    def __post_init__(self):
        """Initialize default values after dataclass initialization."""
        if self.tags is None:
            self.tags = []

    @classmethod
    def from_api_response(cls, response: dict) -> "StreamConfig":
        """
        Creates a StreamConfig from an API response.

        :param response: API response as a dictionary
        :return: StreamConfig instance
        """
        from .utils import truncate, MAX_INSTANCE_TITLE_LENGTH, MAX_TAG_LENGTH

        # Truncate instance name to max length
        name = truncate(response.get("name", ""), MAX_INSTANCE_TITLE_LENGTH)

        # Truncate each tag to max length
        raw_tags = response.get("tags", [])
        tags = [truncate(tag, MAX_TAG_LENGTH) for tag in raw_tags]

        return cls(name=name, tags=tags)
