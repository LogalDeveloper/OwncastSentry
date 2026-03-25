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

"""Tests for utility functions and constants."""

import pytest

from owncastsentry.utils import (
    domainify,
    escape_markdown,
    sanitize_for_markdown,
    sanitize_for_plain_text,
    should_query_stream,
    truncate,
    user_agent,
)


class TestUserAgent:
    """User-Agent header construction."""

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            pytest.param(
                "1.2.3",
                "OwncastSentry/1.2.3 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="semver",
            ),
            pytest.param(
                "0.0.0",
                "OwncastSentry/0.0.0 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="zeroed",
            ),
            pytest.param(
                "1.1.1.dev10+gf0146d061.d20260313",
                "OwncastSentry/1.1.1.dev10+gf0146d061.d20260313 (bot; +https://git.logal.dev/LogalDeveloper/OwncastSentry)",
                id="dev-version",
            ),
        ],
    )
    def test_user_agent(self, version: str, expected: str) -> None:
        """Build a correctly formatted User-Agent header."""
        assert user_agent(version) == expected


class TestShouldQueryStream:
    """Progressive backoff logic for stream polling."""

    @pytest.mark.parametrize(
        ("counter", "expected"),
        [
            pytest.param(0, True, id="counter-0-always-query"),
            pytest.param(1, True, id="counter-1-always-query"),
            pytest.param(4, True, id="counter-4-always-query"),
            pytest.param(5, False, id="counter-5-skip-odd"),
            pytest.param(6, True, id="counter-6-query-even"),
            pytest.param(9, False, id="counter-9-skip-odd"),
            pytest.param(10, False, id="counter-10-skip-not-mod-3"),
            pytest.param(12, True, id="counter-12-query-mod-3"),
            pytest.param(14, False, id="counter-14-skip-not-mod-3"),
            pytest.param(15, True, id="counter-15-query-mod-5"),
            pytest.param(16, False, id="counter-16-skip-not-mod-5"),
            pytest.param(20, True, id="counter-20-query-mod-5"),
            pytest.param(29, False, id="counter-29-skip-not-mod-5"),
            pytest.param(30, True, id="counter-30-query-mod-15"),
            pytest.param(31, False, id="counter-31-skip-not-mod-15"),
            pytest.param(45, True, id="counter-45-query-mod-15"),
            pytest.param(100, False, id="counter-100-skip-not-mod-15"),
            pytest.param(105, True, id="counter-105-query-mod-15"),
        ],
    )
    def test_backoff_tiers(self, counter: int, expected: bool) -> None:
        """Return the expected query decision for each backoff tier."""
        assert should_query_stream(counter) == expected


class TestDomainify:
    """Domain extraction and sanitization from user input."""

    @pytest.mark.parametrize(
        ("input_url", "expected"),
        [
            pytest.param("example.com", "example.com", id="bare-domain"),
            pytest.param("https://example.com", "example.com", id="https-url"),
            pytest.param("http://example.com", "example.com", id="http-url"),
            pytest.param("https://example.com:8080", "example.com", id="url-with-port"),
            pytest.param(
                "https://example.com/path/to/page",
                "example.com",
                id="url-with-path",
            ),
            pytest.param(
                "user@stream.logal.dev",
                "stream.logal.dev",
                id="email-style",
            ),
            pytest.param("EXAMPLE.COM", "example.com", id="uppercase"),
            pytest.param("exam!ple.com", "example.com", id="special-chars-stripped"),
            pytest.param(".example.com.", "example.com", id="leading-trailing-dots"),
            pytest.param("-example.com-", "example.com", id="leading-trailing-hyphens"),
            pytest.param(
                "sub.domain.example.com",
                "sub.domain.example.com",
                id="subdomain",
            ),
        ],
    )
    def test_extracts_domain(self, input_url: str, expected: str) -> None:
        """Extract and sanitize the domain from various input formats."""
        assert domainify(input_url) == expected


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
        assert truncate(text, max_length) == expected


class TestEscapeMarkdown:
    """Markdown special character escaping."""

    @pytest.mark.parametrize(
        ("input_text", "expected"),
        [
            pytest.param("hello", "hello", id="plain-text-unchanged"),
            pytest.param("*bold*", "\\*bold\\*", id="asterisks"),
            pytest.param("_italic_", "\\_italic\\_", id="underscores"),
            pytest.param("[link](url)", "\\[link\\]\\(url\\)", id="link-syntax"),
            pytest.param("`code`", "\\`code\\`", id="backticks"),
            pytest.param("# heading", "\\# heading", id="heading"),
            pytest.param("> quote", "\\> quote", id="blockquote"),
            pytest.param("<html>", "\\<html\\>", id="angle-brackets"),
            pytest.param("a & b", "a \\& b", id="ampersand"),
            pytest.param("a\\b", "a\\\\b", id="backslash"),
            pytest.param("", "", id="empty-string"),
        ],
    )
    def test_escapes_special_chars(self, input_text: str, expected: str) -> None:
        """Escape the given Markdown special character."""
        assert escape_markdown(input_text) == expected


class TestSanitizeForPlainText:
    """Plain text sanitization for notifications."""

    @pytest.mark.parametrize(
        ("input_text", "expected"),
        [
            pytest.param("hello world", "hello world", id="plain-text"),
            pytest.param("line1\nline2", "line1 line2", id="newline-removed"),
            pytest.param("line1\rline2", "line1 line2", id="carriage-return"),
            pytest.param("line1\r\nline2", "line1 line2", id="crlf-removed"),
            pytest.param(
                "too   many   spaces", "too many spaces", id="spaces-collapsed"
            ),
            pytest.param("", "", id="empty-string"),
        ],
    )
    def test_sanitizes(self, input_text: str, expected: str) -> None:
        """Sanitize the text for safe plain-text rendering."""
        assert sanitize_for_plain_text(input_text) == expected


class TestSanitizeForMarkdown:
    """Markdown sanitization combining newline removal and escaping."""

    def test_removes_newlines_and_escapes(self) -> None:
        """Remove newlines and escape Markdown special characters."""
        result = sanitize_for_markdown("*bold*\nnew line")
        assert result == "\\*bold\\* new line"

    def test_empty_string(self) -> None:
        """Return empty string unchanged."""
        assert sanitize_for_markdown("") == ""
