"""OAuth2 Token Provider and Async Kafka Consumer for Mercedes-Benz EU Data Act."""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from typing import Any, Callable

import aiohttp
from aiokafka import AIOKafkaConsumer
from aiokafka.abc import AbstractTokenProvider
from aiokafka.errors import KafkaError

_LOGGER = logging.getLogger(__name__)


class MBOAuthTokenProvider(AbstractTokenProvider):
    """OAuth2 SASL_OAUTHBEARER token provider for Mercedes-Benz Kafka Push API."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str,
    ) -> None:
        """Initialize the token provider."""
        self._session = session
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._access_token: str | None = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        """Return a valid OAuth2 bearer token, refreshing if necessary."""
        async with self._lock:
            now = time.monotonic()
            if self._access_token and now < (self._expires_at - 60):
                return self._access_token

            _LOGGER.debug("Requesting new OAuth2 token from %s", self._token_url)
            data = {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
            if self._scope:
                data["scope"] = self._scope

            async with self._session.post(
                self._token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"OAuth2 token request failed ({resp.status}): {body}"
                    )
                payload = await resp.json()
                self._access_token = payload["access_token"]
                expires_in = int(payload.get("expires_in", 3600))
                self._expires_at = now + expires_in
                _LOGGER.debug("Obtained OAuth2 token valid for %s seconds", expires_in)
                return self._access_token


class MercedesDataActStreamClient:
    """Manages the persistent Kafka consumer stream and signal state cache."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        token_provider: MBOAuthTokenProvider,
        on_message_callback: Callable[[str, dict[str, Any]], None],
    ) -> None:
        """Initialize the stream client."""
        self._session = session
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._consumer_group = consumer_group
        self._token_provider = token_provider
        self._on_message_callback = on_message_callback
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._running = False

        # In-memory state store: { device_id (VIN/WallboxID): { signal_name: value_dict } }
        self.devices: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        """Start the background Kafka consumer loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        """Continuously consume messages from the Mercedes-Benz Kafka topic."""
        ssl_context = ssl.create_default_context()

        while self._running:
            try:
                self._consumer = AIOKafkaConsumer(
                    self._topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._consumer_group,
                    security_protocol="SASL_SSL",
                    sasl_mechanism="OAUTHBEARER",
                    sasl_oauth_token_provider=self._token_provider,
                    ssl_context=ssl_context,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                )
                await self._consumer.start()
                _LOGGER.info(
                    "Connected to Mercedes-Benz EU Data Act Kafka topic '%s'",
                    self._topic,
                )

                async for msg in self._consumer:
                    if not self._running:
                        break
                    await self._handle_raw_message(msg.value)

            except asyncio.CancelledError:
                break
            except KafkaError as err:
                _LOGGER.error("Kafka stream error: %s. Reconnecting in 15s...", err)
                await asyncio.sleep(15)
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.exception(
                    "Unexpected error in Mercedes-Benz stream: %s. Reconnecting in 30s...",
                    err,
                )
                await asyncio.sleep(30)
            finally:
                if self._consumer:
                    try:
                        await self._consumer.stop()
                    except Exception:  # pylint: disable=broad-except
                        pass
                    self._consumer = None

    async def _handle_raw_message(self, raw_bytes: bytes | None) -> None:
        """Parse incoming JSON telemetry payload from Kafka Push API."""
        if not raw_bytes:
            return
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            _LOGGER.warning("Failed to decode Kafka payload: %s", err)
            return

        # Extract device identifier (VIN for vehicles, deviceId/serial for wallboxes)
        device_id = (
            payload.get("vin")
            or payload.get("deviceId")
            or payload.get("header", {}).get("vin")
            or payload.get("header", {}).get("deviceId")
        )
        if not device_id:
            _LOGGER.debug("Received payload without device identifier: %s", payload)
            return

        device_signals = self.devices.setdefault(device_id, {})

        # Normalize signal structures (supports both flat signal maps and signal arrays)
        signals = payload.get("signals") or payload.get("data") or payload
        if isinstance(signals, list):
            for item in signals:
                name = item.get("name") or item.get("signalName")
                if name:
                    device_signals[name] = {
                        "value": item.get("value"),
                        "unit": item.get("unit"),
                        "timestamp": item.get("timestamp") or payload.get("timestamp"),
                    }
        elif isinstance(signals, dict):
            for key, val in signals.items():
                if key in ("vin", "deviceId", "header", "timestamp", "messageType"):
                    continue
                if isinstance(val, dict) and "value" in val:
                    device_signals[key] = val
                else:
                    device_signals[key] = {
                        "value": val,
                        "unit": None,
                        "timestamp": payload.get("timestamp"),
                    }

        # Notify Home Assistant entities
        self._on_message_callback(device_id, device_signals)
