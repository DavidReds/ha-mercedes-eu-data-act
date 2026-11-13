"""The Mercedes-Benz EU Data Act integration."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send

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
    SIGNAL_NEW_TELEMETRY,
)
from .oauth_kafka import MBOAuthTokenProvider, MercedesDataActStreamClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.DEVICE_TRACKER,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Mercedes-Benz EU Data Act from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    session = async_get_clientsession(hass)
    data = entry.data

    token_provider = MBOAuthTokenProvider(
        session=session,
        token_url=data.get(CONF_TOKEN_URL, DEFAULT_TOKEN_URL),
        client_id=data[CONF_CLIENT_ID],
        client_secret=data[CONF_CLIENT_SECRET],
        scope=data.get(CONF_OAUTH_SCOPE, DEFAULT_OAUTH_SCOPE),
    )

    def _on_telemetry(device_id: str, signals: dict[str, Any]) -> None:
        """Dispatch signal updates to Home Assistant platform listeners."""
        async_dispatcher_send(
            hass,
            f"{SIGNAL_NEW_TELEMETRY}_{entry.entry_id}",
            device_id,
            signals,
        )

    stream_client = MercedesDataActStreamClient(
        session=session,
        bootstrap_servers=data.get(CONF_KAFKA_BOOTSTRAP, DEFAULT_KAFKA_BOOTSTRAP),
        topic=data[CONF_KAFKA_TOPIC],
        consumer_group=data[CONF_CONSUMER_GROUP],
        token_provider=token_provider,
        on_message_callback=_on_telemetry,
    )

    hass.data[DOMAIN][entry.entry_id] = stream_client

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await stream_client.start()

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    stream_client: MercedesDataActStreamClient = hass.data[DOMAIN].pop(entry.entry_id)
    await stream_client.stop()
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
