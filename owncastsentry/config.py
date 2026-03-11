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

"""Configuration module for the OwncastSentry plugin."""

from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper


class Config(BaseProxyConfig):
    """Configuration class for OwncastSentry plugin."""

    def do_update(self, helper: ConfigUpdateHelper) -> None:
        """Update configuration with user-provided values.

        :param helper: ConfigUpdateHelper for copying values.
        """
        helper.copy("health_check_endpoint")

    @property
    def health_check_endpoint(self) -> str:
        """Return the configured health check endpoint URL."""
        return self["health_check_endpoint"]  # type: ignore[no-any-return]
