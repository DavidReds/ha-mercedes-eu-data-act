"""Config flow for Mercedes-Benz EU Data Act integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_CONSUMER_GROUP,
    CONF_KAFKA_BOOTSTRAP,
    CONF_KAFKA_TOPIC,
    CONF_OAUTH_SCOPE,
    CONF_TOKEN_URL,
    DEFAULT_KAFKA_BOOTSTRAP,
    DEFAULT_OAUTH_SCOPE,
    DEFAULT_TOKEN_URL,
    DOMAIN,
)
from .oauth_kafka import MBOAuthTokenProvider

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_CLIENT_ID): str,
        vol.Required(CONF_CLIENT_SECRET): str,
        vol.Required(CONF_KAFKA_TOPIC): str,
        vol.Required(CONF_CONSUMER_GROUP): str,
        vol.Optional(CONF_OAUTH_SCOPE, default=DEFAULT_OAUTH_SCOPE): str,
        vol.Optional(CONF_TOKEN_URL, default=DEFAULT_TOKEN_URL): str,
        vol.Optional(CONF_KAFKA_BOOTSTRAP, default=DEFAULT_KAFKA_BOOTSTRAP): str,
    }
)


class MercedesEUDataActConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Mercedes-Benz EU Data Act."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial configuration step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            session = async_get_clientsession(self.hass)
            provider = MBOAuthTokenProvider(
                session=session,
                token_url=user_input[CONF_TOKEN_URL],
                client_id=user_input[CONF_CLIENT_ID],
                client_secret=user_input[CONF_CLIENT_SECRET],
                scope=user_input.get(CONF_OAUTH_SCOPE, DEFAULT_OAUTH_SCOPE),
            )
            try:
                await provider.async_token()
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.error("Failed to authenticate with MB OAuth2: %s", err)
                errors["base"] = "invalid_auth"
            else:
                await self.async_set_unique_id(user_input[CONF_KAFKA_TOPIC])
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"Mercedes-Benz ({user_input[CONF_KAFKA_TOPIC]})",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )
