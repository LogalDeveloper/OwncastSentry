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

"""Prometheus metrics service for OwncastSentry."""

import time
from contextlib import contextmanager, suppress
from enum import StrEnum
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Info

from .types import StreamStatus


class NotificationType(StrEnum):
    """Notification type labels for the delivery counter."""

    LIVE = "live"
    TITLE_CHANGE = "title_change"
    CLEANUP_WARNING = "cleanup_warning"
    CLEANUP_DELETION = "cleanup_deletion"


class ErrorSource(StrEnum):
    """Error source labels for the error counter."""

    SCHEDULER_LOOP = "scheduler_loop"
    COMMAND = "command"


if TYPE_CHECKING:
    from collections.abc import Callable, Generator

# Mapping from StreamStatus enum to numeric gauge values
_STATUS_VALUES: dict[StreamStatus, float] = {
    StreamStatus.ONLINE: 1.0,
    StreamStatus.OFFLINE: 0.0,
    StreamStatus.UNKNOWN: -1.0,
}


class _ResponseTimer:
    """Timer that only records an observation when explicitly marked successful."""

    __slots__ = ("_domain", "_gauge", "_should_observe", "_start")

    def __init__(self, gauge: Gauge, domain: str) -> None:
        self._gauge = gauge
        self._domain = domain
        self._start = time.monotonic()
        self._should_observe = False

    def success(self) -> None:
        """Mark the request as successful so the duration is recorded."""
        self._should_observe = True

    def _finalize(self) -> None:
        if self._should_observe:
            self._gauge.labels(domain=self._domain).set(
                max(time.monotonic() - self._start, 0)
            )
        else:
            with suppress(KeyError):
                self._gauge.remove(self._domain)


class MetricsService:
    """Manages Prometheus metrics with an isolated registry."""

    def __init__(self) -> None:
        """Initialize metrics with a custom collector registry."""
        self.registry = CollectorRegistry()

        self.notification_delivery_total = Counter(
            "owncastsentry_notification_delivery_total",
            "Total notification delivery attempts to individual rooms",
            ["type", "result"],
            registry=self.registry,
        )
        self.stream_status = Gauge(
            "owncastsentry_stream_status",
            "Current stream status (1=online, 0=offline, -1=unknown)",
            ["domain"],
            registry=self.registry,
        )
        self.stream_subscriptions = Gauge(
            "owncastsentry_stream_subscriptions",
            "Number of room subscriptions per stream domain",
            ["domain"],
            registry=self.registry,
        )
        self.check_failures = Gauge(
            "owncastsentry_check_failures",
            "Consecutive check failure count per stream domain",
            ["domain"],
            registry=self.registry,
        )
        self.api_response_seconds = Gauge(
            "owncastsentry_api_response_seconds",
            "Last successful HTTP response time in seconds per stream domain",
            ["domain"],
            registry=self.registry,
        )
        self.build_info = Info(
            "owncastsentry",
            "OwncastSentry build information",
            registry=self.registry,
        )
        self.open_connections = Gauge(
            "owncastsentry_http_connections_open",
            "Total number of open HTTP connections (idle and active)",
            registry=self.registry,
        )
        self.errors_total = Counter(
            "owncastsentry_errors_total",
            "Internal errors by source",
            ["source"],
            registry=self.registry,
        )

        # Initialize all known label combinations so they start at 0
        for notification_type in NotificationType:
            for result in ("success", "failure"):
                self.notification_delivery_total.labels(
                    type=notification_type, result=result
                )
        for source in ErrorSource:
            self.errors_total.labels(source=source)

    def record_delivery(
        self,
        notification_type: NotificationType,
        *,
        successful: int = 0,
        failed: int = 0,
    ) -> None:
        """Record notification delivery results.

        :param notification_type: The type of notification delivered.
        :param successful: Number of successful room deliveries.
        :param failed: Number of failed room deliveries.
        """
        self.notification_delivery_total.labels(
            type=notification_type, result="success"
        ).inc(successful)
        self.notification_delivery_total.labels(
            type=notification_type, result="failure"
        ).inc(failed)

    def set_stream_status(self, domain: str, status: StreamStatus) -> None:
        """Set the status gauge for a stream domain.

        :param domain: The stream domain.
        :param status: The current stream status.
        """
        self.stream_status.labels(domain=domain).set(_STATUS_VALUES[status])

    def set_check_failures(self, domain: str, count: int) -> None:
        """Set the consecutive failure count for a stream domain.

        :param domain: The stream domain.
        :param count: The current failure counter value.
        """
        self.check_failures.labels(domain=domain).set(count)

    def set_subscription_count(self, domain: str, count: int) -> None:
        """Set the subscription count for a stream domain.

        :param domain: The stream domain.
        :param count: The number of room subscriptions.
        """
        self.stream_subscriptions.labels(domain=domain).set(count)

    @contextmanager
    def response_timer(self, domain: str) -> Generator[_ResponseTimer]:
        """Return a context manager that times an HTTP request.

        Call ``timer.success()`` inside the block to record the duration.
        If ``success()`` is never called, nothing is recorded.

        :param domain: The stream domain being queried.
        """
        timer = _ResponseTimer(self.api_response_seconds, domain)
        try:
            yield timer
        finally:
            timer._finalize()

    def set_build_info(self, version: str) -> None:
        """Set the build version info metric.

        :param version: The plugin version string.
        """
        self.build_info.info({"version": version})

    def register_open_connections_gauge(self, callback: Callable[[], float]) -> None:
        """Register a gauge that reads open connection count on scrape.

        :param callback: Function returning the current open count.
        """
        self.open_connections.set_function(callback)

    def record_error(self, source: ErrorSource) -> None:
        """Increment the internal error counter.

        :param source: The source of the error.
        """
        self.errors_total.labels(source=source).inc()

    def remove_stream(self, domain: str) -> None:
        """Remove a stream's gauge labels after cleanup deletion.

        :param domain: The stream domain to remove.
        """
        with suppress(KeyError):
            self.stream_status.remove(domain)
        with suppress(KeyError):
            self.check_failures.remove(domain)
        with suppress(KeyError):
            self.stream_subscriptions.remove(domain)
        with suppress(KeyError):
            self.api_response_seconds.remove(domain)
