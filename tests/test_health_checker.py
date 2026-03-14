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

"""Tests for the health checking service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from aioresponses import aioresponses

from owncastsentry.health_checker import HealthChecker, HealthStatus, UpdateResult
from owncastsentry.owncast_client import OwncastClient

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from mautrix.util.async_db import Database


@pytest.fixture
async def health_checker(database: Database) -> AsyncIterator[HealthChecker]:
    """Yield a HealthChecker backed by a real OwncastClient."""
    client = OwncastClient(logger=logging.getLogger("test"), version="0.0.0")
    yield HealthChecker(database, client, logging.getLogger("test"))
    await client.close()


class TestUpdateResultHttpHealthy:
    """HTTP health derivation from update results."""

    @pytest.mark.parametrize(
        ("total", "successful", "failed", "expected"),
        [
            pytest.param(0, 0, 0, True, id="no-streams-is-healthy"),
            pytest.param(3, 2, 1, True, id="some-successes-is-healthy"),
            pytest.param(3, 0, 3, False, id="all-failures-is-unhealthy"),
        ],
    )
    def test_http_healthy(
        self, total: int, successful: int, failed: int, expected: bool
    ) -> None:
        """Derive HTTP health from stream check results."""
        result = UpdateResult(
            total_streams=total,
            successful_checks=successful,
            failed_checks=failed,
        )
        assert result.http_healthy is expected


class TestHealthStatusIsHealthy:
    """Overall health status derivation."""

    @pytest.mark.parametrize(
        ("db_healthy", "http_healthy", "expected"),
        [
            pytest.param(True, True, True, id="all-healthy"),
            pytest.param(False, True, False, id="db-unhealthy"),
            pytest.param(True, False, False, id="http-unhealthy"),
            pytest.param(False, False, False, id="both-unhealthy"),
        ],
    )
    def test_is_healthy(
        self, db_healthy: bool, http_healthy: bool, expected: bool
    ) -> None:
        """Derive overall health from component health."""
        status = HealthStatus(database_healthy=db_healthy, http_healthy=http_healthy)
        assert status.is_healthy is expected


class TestCheckDatabase:
    """Database health check."""

    async def test_returns_true_for_healthy_db(
        self, health_checker: HealthChecker
    ) -> None:
        """Return True when the database responds to queries."""
        assert await health_checker.check_database() is True

    async def test_returns_false_for_stopped_db(
        self, health_checker: HealthChecker, database: Database
    ) -> None:
        """Return False when the database connection is closed."""
        await database.stop()
        assert await health_checker.check_database() is False


class TestPerformHealthCheck:
    """Health check orchestration and endpoint reporting."""

    async def test_skips_report_when_no_endpoint(
        self, health_checker: HealthChecker
    ) -> None:
        """Skip reporting when endpoint is empty."""
        result = UpdateResult(total_streams=0, successful_checks=0, failed_checks=0)
        with aioresponses():
            await health_checker.perform_health_check(result, "")

    async def test_skips_report_when_unhealthy(
        self, health_checker: HealthChecker
    ) -> None:
        """Skip health report when all stream checks failed."""
        result = UpdateResult(total_streams=3, successful_checks=0, failed_checks=3)
        with aioresponses():
            await health_checker.perform_health_check(
                result, "https://health.example.com/ping"
            )

    async def test_sends_report_when_healthy(
        self, health_checker: HealthChecker
    ) -> None:
        """Send GET to endpoint when all checks pass."""
        result = UpdateResult(total_streams=1, successful_checks=1, failed_checks=0)
        with aioresponses() as mocked:
            mocked.get("https://health.example.com/ping", status=200)
            await health_checker.perform_health_check(
                result, "https://health.example.com/ping"
            )

    async def test_skips_report_for_whitespace_endpoint(
        self, health_checker: HealthChecker
    ) -> None:
        """Skip reporting when endpoint is whitespace."""
        result = UpdateResult(total_streams=0, successful_checks=0, failed_checks=0)
        with aioresponses():
            await health_checker.perform_health_check(result, "   ")


class TestSendHealthReport:
    """Health report HTTP delivery."""

    async def test_handles_success_response(
        self, health_checker: HealthChecker
    ) -> None:
        """Complete without error on a 2xx response."""
        with aioresponses() as mocked:
            mocked.get("https://health.example.com/ping", status=200)
            await health_checker._send_health_report("https://health.example.com/ping")

    async def test_handles_non_success_response(
        self, health_checker: HealthChecker
    ) -> None:
        """Complete without error on a non-2xx response."""
        with aioresponses() as mocked:
            mocked.get("https://health.example.com/ping", status=500)
            await health_checker._send_health_report("https://health.example.com/ping")

    async def test_handles_connection_error(
        self, health_checker: HealthChecker
    ) -> None:
        """Complete without error on a connection failure."""
        with aioresponses() as mocked:
            mocked.get(
                "https://health.example.com/ping",
                exception=ConnectionError(),
            )
            await health_checker._send_health_report("https://health.example.com/ping")
