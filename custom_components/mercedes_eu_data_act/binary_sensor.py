"""Dynamic Binary Sensor platform for boolean Mercedes-Benz EU Data Act signals."""
from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_NEW_TELEMETRY


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up dynamic binary sensors for boolean signals (doors, windows, ignition)."""
    known_entities: set[str] = set()

    @callback
    def _handle_telemetry(device_id: str, signals: dict[str, Any]) -> None:
        new_entities: list[MercedesDataActBinarySensor] = []
        for signal_name, signal_data in signals.items():
            val = signal_data.get("value")
            if not isinstance(val, bool):
                continue

            unique_key = f"{device_id}_{signal_name}"
            if unique_key not in known_entities:
                known_entities.add(unique_key)
                new_entities.append(
                    MercedesDataActBinarySensor(
                        entry_id=entry.entry_id,
                        device_id=device_id,
                        signal_name=signal_name,
                        initial_data=signal_data,
                    )
                )
        if new_entities:
            async_add_entities(new_entities)

    entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{SIGNAL_NEW_TELEMETRY}_{entry.entry_id}",
            _handle_telemetry,
        )
    )


class MercedesDataActBinarySensor(BinarySensorEntity):
    """Representation of a boolean Mercedes-Benz EU Data Act signal."""

    _attr_has_entity_name = True

    def __init__(
        self,
        entry_id: str,
        device_id: str,
        signal_name: str,
        initial_data: dict[str, Any],
    ) -> None:
        """Initialize the binary sensor."""
        self._entry_id = entry_id
        self._device_id = device_id
        self._signal_name = signal_name
        self._attr_unique_id = f"{DOMAIN}_{device_id}_{signal_name}"
        self._attr_name = signal_name.replace("_", " ").title()
        self._attr_is_on = bool(initial_data.get("value"))

        lower_name = signal_name.lower()
        if "door" in lower_name or "trunk" in lower_name or "hood" in lower_name:
            self._attr_device_class = BinarySensorDeviceClass.DOOR
        elif "window" in lower_name or "sunroof" in lower_name:
            self._attr_device_class = BinarySensorDeviceClass.WINDOW
        elif "lock" in lower_name:
            self._attr_device_class = BinarySensorDeviceClass.LOCK
        elif "charg" in lower_name:
            self._attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device_id)},
            name=f"Mercedes-Benz {device_id}",
            manufacturer="Mercedes-Benz",
            model="EU Data Act Connected Device",
        )

    async def async_added_to_hass(self) -> None:
        """Register dispatcher callback."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                f"{SIGNAL_NEW_TELEMETRY}_{self._entry_id}",
                self._async_update_signal,
            )
        )

    @callback
    def _async_update_signal(self, device_id: str, signals: dict[str, Any]) -> None:
        """Update binary state."""
        if device_id != self._device_id or self._signal_name not in signals:
            return
        self._attr_is_on = bool(signals[self._signal_name].get("value"))
        self.async_write_ha_state()
