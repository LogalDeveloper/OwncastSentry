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

"""Shared test fixtures and stubs for OwncastSentry tests."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest
from mautrix.util.async_db import Database
from prometheus_client import generate_latest

from owncastsentry import OwncastSentry
from owncastsentry.config import Config
from owncastsentry.repository import (
    StreamRepository,
    SubscriptionRepository,
    get_upgrade_table,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from owncastsentry.metrics import MetricsService
    from owncastsentry.types import StreamConfig, StreamState


def generate_metrics_output(metrics: MetricsService) -> str:
    """Generate Prometheus text format output from a MetricsService registry."""
    return generate_latest(metrics.registry).decode("utf-8")


@pytest.fixture
async def database(tmp_path: Path) -> AsyncIterator[Database]:
    """Yield a real SQLite-backed mautrix Database with migrations applied."""
    db_path = tmp_path / "test.db"
    db = Database.create(
        f"sqlite:///{db_path}",
        upgrade_table=get_upgrade_table(),
    )
    await db.start()
    yield db
    await db.stop()


@pytest.fixture
def stream_repo(database: Database) -> StreamRepository:
    """Return a StreamRepository backed by the test database."""
    return StreamRepository(database)


@pytest.fixture
def subscription_repo(database: Database) -> SubscriptionRepository:
    """Return a SubscriptionRepository backed by the test database."""
    return SubscriptionRepository(database)


# These fixtures use names expected by the maubot.testing framework.
# They look unused but are discovered automatically by maubot's pytest plugin.


@pytest.fixture
def maubot_plugin_class():
    """Use OwncastSentry as the plugin class for maubot integration tests."""
    return OwncastSentry


@pytest.fixture
def maubot_plugin_config_class():
    """Use our Config class for maubot integration tests."""
    return Config


@pytest.fixture
def maubot_upgrade_table():
    """Provide the database upgrade table for maubot integration tests."""
    return get_upgrade_table()


VALID_CONFIG_RESPONSE: dict[str, object] = {
    "appearanceVariables": {},
    "name": "LogalDeveloper's Live Stream",
    "customStyles": "",
    "streamTitle": "I think I can do this... Let's start a nuclear reaction - Playing Nucleares!",  # noqa: E501
    "offlineMessage": "<p>HTTP/1.1 204 No Content</p>\n<p>You've reached the right place, but I'm not live right now.</p>",  # noqa: E501
    "logo": "/logo",
    "version": "Owncast v0.2.4-linux-64bit (8e89391309dd3aa4d0db4361a2ba1e144f42f8c8)",
    "extraPageContent": (
        "<h1>About</h1>\n<p>Hi there! I'm Logan, a cybersecurity professional "
        "with a background in Linux systems and network administration. This is my "
        "little corner of the internet where I exclusively run my live streams. While "
        "gaming is my most common stream topic, I occasionally stream other things "
        "which interest me, such as cybersecurity challenges. If you are looking for "
        "my main website, where my blog and project directory is hosted, please visit: "
        '<a href="https://logal.dev/">https://logal.dev/</a></p>\n<p>Any views and '
        "opinions expressed in my live streams and videos are my own and do not "
        "necessarily reflect those of my employer or any affiliated organizations."
        "</p>\n<h1>Following</h1>\n<p>I don\u2019t stick to a strict streaming "
        "schedule, but there are a few ways to stay updated so you know when I go "
        "live:</p>\n<ul>\n<li>\u2b50 <strong>Matrix</strong>: Send the message "
        "<code>!subscribe stream.logal.dev</code> to "
        '<a href="https://matrix.to/#/@owncastsentry:logal.dev">'
        "@owncastsentry:logal.dev</a>.</li>\n<li><strong>Fediverse</strong>: Follow "
        "<code>@notify@stream.logal.dev</code>.</li>\n</ul>\n<h1>Chat Commands</h1>"
        "\n<p>To make things more interactive, there are several commands you can "
        "send in chat to play sound effects live on stream:</p>\n<ul>\n"
        "<li>!boom</li>\n<li>!bluetooth</li>\n<li>!bruh</li>\n<li>!creeper</li>\n"
        "<li>!directed</li>\n<li>!fart</li>\n<li>!icetea</li>\n<li>!oof</li>\n"
        "<li>!perfect</li>\n<li>!spare</li>\n<li>!thatsit</li>\n<li>!usb</li>\n"
        "<li>!whocares</li>\n<li>!yoda</li>\n</ul>"
    ),
    "summary": "Video games, cybersecurity, and more!",
    "tags": [
        "video games",
        "chatting",
        "casual",
        "english",
        "streaming",
        "owncast",
        "variety",
    ],
    "socialHandles": None,
    "externalActions": [
        {
            "url": "https://ko-fi.com/logaldeveloper",
            "html": "",
            "title": "Tip",
            "description": "",
            "icon": "https://stream.logal.dev/img/platformlogos/ko-fi.svg",
            "color": "",
            "openExternally": True,
        },
        {
            "url": "https://tubefree.org/c/logaldeveloper_stream_archive",
            "html": "",
            "title": "Previous Live Stream Recordings",
            "description": "",
            "icon": "https://stream.logal.dev/img/platformlogos/fediverse.svg",
            "color": "",
            "openExternally": True,
        },
    ],
    "notifications": {
        "browser": {
            "publicKey": "BI9BhIY6c7nfP6ZSIu7T53Lta5sGYqDqwSpiCabZ0XQxPQOmnUHRSjInaa3HX9XmYE-bV8SFmbFk4stZy2jnJ3M",  # noqa: E501
            "enabled": True,
        },
    },
    "federation": {
        "account": "notify@stream.logal.dev",
        "followerCount": 74,
        "enabled": True,
    },
    "maxSocketPayloadSize": 2048,
    "hideViewerCount": False,
    "chatDisabled": False,
    "chatSpamProtectionDisabled": False,
    "nsfw": False,
    "authentication": {"indieAuthEnabled": True},
}

VALID_STATUS_RESPONSE: dict[str, object] = {
    "serverTime": "2026-03-13T15:16:14.354962696-04:00",
    "lastConnectTime": None,
    "lastDisconnectTime": "2026-03-04T21:05:32-05:00",
    "versionNumber": "0.2.4",
    "streamTitle": "I think I can do this... Let's start a nuclear reaction - Playing Nucleares!",  # noqa: E501
    "online": False,
}


@dataclass
class _SentMessage:
    """A message recorded by _StubMatrixClient."""

    room_id: str
    content: Any


class _StubMatrixClient:
    """Recording stub for the Matrix client used in notification tests."""

    def __init__(self) -> None:
        self.sent_messages: list[_SentMessage] = []
        self.should_fail_for_rooms: set[str] = set()

    async def send_message(self, room_id: str, content: Any) -> None:
        """Record a sent message, or raise if room is in the fail set."""
        if room_id in self.should_fail_for_rooms:
            msg = f"Stubbed failure for room {room_id}"
            raise RuntimeError(msg)
        self.sent_messages.append(_SentMessage(room_id=room_id, content=content))


@dataclass
class _StubOwncastClient:
    """Configurable stub for the Owncast HTTP client."""

    stream_state: StreamState | None = None
    stream_config: StreamConfig | None = None
    state_call_count: int = field(default=0, init=False)
    config_call_count: int = field(default=0, init=False)
    queried_domains: list[str] = field(default_factory=list, init=False)

    async def get_stream_state(self, domain: str) -> StreamState | None:
        """Return the configured stream state."""
        self.state_call_count += 1
        self.queried_domains.append(domain)
        return self.stream_state

    async def get_stream_config(self, domain: str) -> StreamConfig | None:
        """Return the configured stream config."""
        self.config_call_count += 1
        return self.stream_config
