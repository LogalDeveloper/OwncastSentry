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

"""Tests for the Prometheus metrics service."""

import pytest

from owncastsentry.metrics import ErrorSource, MetricsService, NotificationType
from owncastsentry.types import StreamStatus
from tests.conftest import generate_metrics_output


class TestRecordDelivery:
    """Notification delivery counter with type and result labels."""

    @pytest.mark.parametrize(
        ("notification_type", "successful", "failed", "expected_fragments"),
        [
            pytest.param(
                NotificationType.LIVE,
                3,
                0,
                ['result="success",type="live"} 3.0'],
                id="live-success",
            ),
            pytest.param(
                NotificationType.LIVE,
                0,
                2,
                ['result="failure",type="live"} 2.0'],
                id="live-failure",
            ),
            pytest.param(
                NotificationType.TITLE_CHANGE,
                1,
                0,
                ['result="success",type="title_change"} 1.0'],
                id="title-change-success",
            ),
            pytest.param(
                NotificationType.CLEANUP_WARNING,
                2,
                0,
                ['result="success",type="cleanup_warning"} 2.0'],
                id="cleanup-warning-success",
            ),
            pytest.param(
                NotificationType.CLEANUP_DELETION,
                1,
                1,
                [
                    'result="success",type="cleanup_deletion"} 1.0',
                    'result="failure",type="cleanup_deletion"} 1.0',
                ],
                id="cleanup-deletion-mixed",
            ),
        ],
    )
    def test_records_delivery(
        self,
        notification_type: NotificationType,
        successful: int,
        failed: int,
        expected_fragments: list[str],
    ) -> None:
        """Record delivery results with correct type and result labels."""
        service = MetricsService()
        service.record_delivery(notification_type, successful=successful, failed=failed)
        output = generate_metrics_output(service)
        for fragment in expected_fragments:
            assert fragment in output

    @pytest.mark.parametrize(
        ("notification_type", "result"),
        [
            pytest.param(t, r, id=f"{t}-{r}")
            for t in NotificationType
            for r in ("success", "failure")
        ],
    )
    def test_all_combinations_initialized(
        self, notification_type: NotificationType, result: str
    ) -> None:
        """All type/result label combinations exist at zero on init."""
        service = MetricsService()
        output = generate_metrics_output(service)
        expected = (
            f"owncastsentry_notification_delivery_total"
            f'{{result="{result}",type="{notification_type}"}} 0.0'
        )
        assert expected in output


class TestSetStreamStatus:
    """Per-stream status gauge."""

    @pytest.mark.parametrize(
        ("status", "expected_value"),
        [
            pytest.param(StreamStatus.ONLINE, 1.0, id="online"),
            pytest.param(StreamStatus.OFFLINE, 0.0, id="offline"),
            pytest.param(StreamStatus.UNKNOWN, -1.0, id="unknown"),
        ],
    )
    def test_sets_status(self, status: StreamStatus, expected_value: float) -> None:
        """Set gauge to the correct value for each stream status."""
        service = MetricsService()
        service.set_stream_status("test.com", status)
        output = generate_metrics_output(service)
        expected = f'owncastsentry_stream_status{{domain="test.com"}} {expected_value}'
        assert expected in output


class TestSetSubscriptionCount:
    """Per-stream subscription count gauge."""

    def test_sets_count(self) -> None:
        """Set the subscription count for a domain."""
        service = MetricsService()
        service.set_subscription_count("test.com", 5)
        output = generate_metrics_output(service)
        assert 'owncastsentry_stream_subscriptions{domain="test.com"} 5.0' in output

    def test_updates_count(self) -> None:
        """Update the subscription count for a domain."""
        service = MetricsService()
        service.set_subscription_count("test.com", 5)
        service.set_subscription_count("test.com", 3)
        output = generate_metrics_output(service)
        assert 'owncastsentry_stream_subscriptions{domain="test.com"} 3.0' in output


class TestSetCheckFailures:
    """Check failure counter gauge per domain."""

    def test_sets_count(self) -> None:
        """Set the failure count for a domain."""
        service = MetricsService()
        service.set_check_failures("fail.com", 3)
        output = generate_metrics_output(service)
        assert 'owncastsentry_check_failures{domain="fail.com"} 3.0' in output

    def test_resets_to_zero(self) -> None:
        """Reset the failure count to zero."""
        service = MetricsService()
        service.set_check_failures("fail.com", 5)
        service.set_check_failures("fail.com", 0)
        output = generate_metrics_output(service)
        assert 'owncastsentry_check_failures{domain="fail.com"} 0.0' in output


class TestResponseTimer:
    """Response time gauge via context manager."""

    def test_records_on_success(self) -> None:
        """Record a response time when success() is called."""
        service = MetricsService()
        with service.response_timer("example.com") as timer:
            timer.success()
        output = generate_metrics_output(service)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' in output

    def test_does_not_record_without_success(self) -> None:
        """Do not record when success() is never called."""
        service = MetricsService()
        with service.response_timer("example.com"):
            pass
        output = generate_metrics_output(service)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' not in output

    def test_overwrites_previous_value(self) -> None:
        """Overwrite previous value with the latest response time."""
        service = MetricsService()
        with service.response_timer("example.com") as timer:
            timer.success()
        with service.response_timer("example.com") as timer:
            timer.success()
        output = generate_metrics_output(service)
        # Gauge should have exactly one line for this domain, not accumulated
        matches = [
            line
            for line in output.splitlines()
            if line.startswith("owncastsentry_api_response_seconds{")
        ]
        assert len(matches) == 1

    def test_does_not_record_on_exception(self) -> None:
        """Do not record when the block raises an exception."""
        service = MetricsService()
        with (
            pytest.raises(ValueError, match="boom"),
            service.response_timer("example.com"),
        ):
            raise ValueError("boom")
        output = generate_metrics_output(service)
        assert 'owncastsentry_api_response_seconds{domain="example.com"}' not in output


class TestRemoveStream:
    """Stale stream label cleanup."""

    def test_removes_stream_label(self) -> None:
        """Remove a stream's gauge labels after cleanup deletion."""
        service = MetricsService()
        service.set_stream_status("gone.com", StreamStatus.OFFLINE)
        service.set_subscription_count("gone.com", 2)
        assert 'domain="gone.com"' in generate_metrics_output(service)
        service.remove_stream("gone.com")
        assert 'domain="gone.com"' not in generate_metrics_output(service)

    def test_remove_nonexistent_is_noop(self) -> None:
        """Removing a nonexistent stream does not raise."""
        service = MetricsService()
        service.remove_stream("never.com")


class TestRegisterOpenConnectionsGauge:
    """Callback-based open connection gauge."""

    def test_reads_value_from_callback(self) -> None:
        """Read the open connection count from the callback at scrape time."""
        service = MetricsService()
        counter = [3]
        service.register_open_connections_gauge(lambda: counter[0])
        output = generate_metrics_output(service)
        assert "owncastsentry_http_connections_open 3.0" in output

    def test_reflects_updated_value(self) -> None:
        """Reflect changes in the callback value on subsequent scrapes."""
        service = MetricsService()
        counter = [1]
        service.register_open_connections_gauge(lambda: counter[0])
        counter[0] = 5
        output = generate_metrics_output(service)
        assert "owncastsentry_http_connections_open 5.0" in output


class TestSetBuildInfo:
    """Build version info metric."""

    def test_sets_version(self) -> None:
        """Set the build version info."""
        service = MetricsService()
        service.set_build_info("1.2.3")
        output = generate_metrics_output(service)
        assert 'owncastsentry_info{version="1.2.3"} 1.0' in output


class TestRecordError:
    """Internal error counter."""

    def test_increments_counter(self) -> None:
        """Increment the error counter for a source."""
        service = MetricsService()
        service.record_error(ErrorSource.SCHEDULER_LOOP)
        output = generate_metrics_output(service)
        assert 'owncastsentry_errors_total{source="scheduler_loop"} 1.0' in output

    def test_increments_multiple_sources(self) -> None:
        """Increment error counters for different sources independently."""
        service = MetricsService()
        service.record_error(ErrorSource.SCHEDULER_LOOP)
        service.record_error(ErrorSource.COMMAND)
        service.record_error(ErrorSource.COMMAND)
        output = generate_metrics_output(service)
        assert 'owncastsentry_errors_total{source="scheduler_loop"} 1.0' in output
        assert 'owncastsentry_errors_total{source="command"} 2.0' in output

    @pytest.mark.parametrize(
        "source",
        [pytest.param(s, id=s) for s in ErrorSource],
    )
    def test_all_sources_initialized(self, source: ErrorSource) -> None:
        """All known source labels exist at zero on init."""
        service = MetricsService()
        output = generate_metrics_output(service)
        expected = f'owncastsentry_errors_total{{source="{source}"}} 0.0'
        assert expected in output


class TestRegistryOutput:
    """Prometheus registry output."""

    def test_returns_string(self) -> None:
        """Return a string (not bytes)."""
        service = MetricsService()
        output = generate_metrics_output(service)
        assert isinstance(output, str)

    def test_contains_help_lines(self) -> None:
        """Include HELP lines for registered metrics."""
        service = MetricsService()
        output = generate_metrics_output(service)
        assert "# HELP owncastsentry_notification_delivery_total" in output
        assert "# HELP owncastsentry_errors_total" in output
        assert "# HELP owncastsentry_info" in output

    def test_uses_isolated_registry(self) -> None:
        """Use a custom registry, not the global default."""
        service = MetricsService()
        output = generate_metrics_output(service)
        assert "python_gc" not in output
        assert "process_" not in output
