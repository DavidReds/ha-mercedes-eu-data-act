from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time
from typing import Any, Callable

import aiohttp
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider

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
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Initialize the token provider."""
        self._session = session
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._loop = loop
        self._access_token: str | None = None
        self._expires_at: float = 0.0
        self._async_lock = asyncio.Lock()

    async def async_token(self) -> str:
        """Asynchronously fetch or return a valid OAuth2 bearer token."""
        async with self._async_lock:
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

    def token(self) -> str:
        """Synchronous token method called by kafka-python-ng thread."""
        if self._access_token and time.monotonic() < (self._expires_at - 60):
            return self._access_token
        if self._loop is None:
            raise RuntimeError("Event loop not configured for token provider")
        future = asyncio.run_coroutine_threadsafe(self.async_token(), self._loop)
        return future.result(timeout=25)


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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # In-memory state store: { device_id (VIN/WallboxID): { signal_name: value_dict } }
        self.devices: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        """Start the background Kafka consumer thread."""
        if self._thread and self._thread.is_alive():
            return
        self._loop = asyncio.get_running_loop()
        self._token_provider._loop = self._loop
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._consume_loop,
            name="MercedesEUDataActKafka",
            daemon=True,
        )
        self._thread.start()

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            await asyncio.to_thread(self._thread.join, 5.0)
        self._thread = None

    def _consume_loop(self) -> None:
        """Continuously consume messages from the Mercedes-Benz Kafka topic."""
        ssl_context = ssl.create_default_context()

        while not self._stop_event.is_set():
            consumer: KafkaConsumer | None = None
            try:
                consumer = KafkaConsumer(
                    self._topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._consumer_group,
                    security_protocol="SASL_SSL",
                    sasl_mechanism="OAUTHBEARER",
                    sasl_oauth_token_provider=self._token_provider,
                    ssl_context=ssl_context,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                )
                _LOGGER.info(
                    "Connected to Mercedes-Benz EU Data Act Kafka topic '%s'",
                    self._topic,
                )

                while not self._stop_event.is_set():
                    for msg in consumer:
                        if self._stop_event.is_set():
                            break
                        self._handle_raw_message(msg.value)

            except KafkaError as err:
                _LOGGER.error("Kafka stream error: %s. Reconnecting in 15s...", err)
                self._stop_event.wait(15)
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.exception(
                    "Unexpected error in Mercedes-Benz stream: %s. Reconnecting in 30s...",
                    err,
                )
                self._stop_event.wait(30)
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:  # pylint: disable=broad-except
                        pass

    def _handle_raw_message(self, raw_bytes: bytes | None) -> None:
        """Parse incoming JSON telemetry payload from Kafka Push API."""
        if not raw_bytes:
            return
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            _LOGGER.warning("Failed to decode Kafka payload: %s", err)
            return

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

        if self._loop:
            self._loop.call_soon_threadsafe(
                self._on_message_callback, device_id, device_signals
            )