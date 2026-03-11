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

"""Health checking service for OwncastSentry."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging

    from mautrix.util.async_db import Database

    from .owncast_client import OwncastClient


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
class HealthStatus:
    """Represents the health status of the plugin."""

    database_healthy: bool
    http_healthy: bool

    @property
    def is_healthy(self) -> bool:
        """Check if all health components are healthy.

        :return: True if all checks pass.
        """
        return self.database_healthy and self.http_healthy


class HealthChecker:
    """Service for performing health checks on the plugin."""

    def __init__(
        self,
        database: Database,
        owncast_client: OwncastClient,
        logger: logging.Logger,
    ):
        """Initialize the health checker.

        :param database: The maubot database instance.
        :param owncast_client: Client for making HTTP requests.
        :param logger: Logger instance for debugging.
        """
        self.db = database
        self.owncast_client = owncast_client
        self.log = logger

    async def check_database(self) -> bool:
        """Check if the database is functioning by executing a simple query.

        :return: True if database is healthy, False otherwise.
        """
        try:
            async with self.db.acquire() as conn:  # type: ignore[var-annotated]
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            self.log.warning(f"Database health check failed: {e}")
            return False

    async def perform_health_check(
        self,
        update_result: UpdateResult,
        endpoint: str,
    ) -> None:
        """Perform health check and report to configured endpoint if all healthy.

        :param update_result: Result of the stream update cycle.
        :param endpoint: Health check endpoint URL (empty string to skip reporting).
        """
        # Check database health
        database_healthy = await self.check_database()

        # Evaluate HTTP health from update results
        http_healthy = update_result.http_healthy

        # Create health status
        status = HealthStatus(
            database_healthy=database_healthy,
            http_healthy=http_healthy,
        )

        self.log.debug(
            f"Health check: database={database_healthy}, http={http_healthy}, "
            f"streams={update_result.total_streams}, "
            f"succeeded={update_result.successful_checks}, "
            f"failed={update_result.failed_checks}"
        )

        # Skip endpoint notification if not configured
        if not endpoint or not endpoint.strip():
            self.log.debug("Health check endpoint not configured, skipping report.")
            return

        # Only send to endpoint if ALL checks pass
        if not status.is_healthy:
            self.log.warning(
                f"Health check failed, not reporting to endpoint. "
                f"database={database_healthy}, http={http_healthy}"
            )
            return

        # Send GET request to health endpoint
        await self._send_health_report(endpoint)

    async def _send_health_report(self, endpoint: str) -> None:
        """Send a GET request to the health check endpoint.

        :param endpoint: The endpoint URL.
        """
        try:
            async with self.owncast_client.session.get(
                endpoint, allow_redirects=True
            ) as response:
                if 200 <= response.status < 300:
                    self.log.debug(
                        f"Health check reported successfully (status={response.status})"
                    )
                else:
                    self.log.warning(
                        "Health check endpoint returned "
                        f"non-success status: {response.status}"
                    )
        except Exception as e:
            self.log.warning(f"Failed to report health check to endpoint: {e}")
