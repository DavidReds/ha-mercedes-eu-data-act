"""Dynamic Sensor platform for Mercedes-Benz EU Data Act signals."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    UnitOfLength,
    UnitOfPressure,
    UnitOfSpeed,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_NEW_TELEMETRY

_LOGGER = logging.getLogger(__name__)

# Map common EU Data Act signal names / units to HA DeviceClasses
UNIT_MAP: dict[str, tuple[SensorDeviceClass | None, str | None, SensorStateClass | None]] = {
    "%": (SensorDeviceClass.BATTERY, PERCENTAGE, SensorStateClass.MEASUREMENT),
    "km": (SensorDeviceClass.DISTANCE, UnitOfLength.KILOMETERS, SensorStateClass.TOTAL_INCREASING),
    "km/h": (SensorDeviceClass.SPEED, UnitOfSpeed.KILOMETERS_PER_HOUR, SensorStateClass.MEASUREMENT),
    "kPa": (SensorDeviceClass.PRESSURE, UnitOfPressure.KPA, SensorStateClass.MEASUREMENT),
    "bar": (SensorDeviceClass.PRESSURE, UnitOfPressure.BAR, SensorStateClass.MEASUREMENT),
    "°C": (SensorDeviceClass.TEMPERATURE, UnitOfTemperature.CELSIUS, SensorStateClass.MEASUREMENT),
    "C": (SensorDeviceClass.TEMPERATURE, UnitOfTemperature.CELSIUS, SensorStateClass.MEASUREMENT),
}

# Signals handled by other platforms (device_tracker, binary_sensor)
EXCLUDED_SIGNALS = {
    "latitude",
    "longitude",
    "positionLat",
    "positionLong",
    "geoposition",
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up dynamic sensors as signals arrive from the Kafka Push API."""
    known_entities: set[str] = set()

    @callback
    def _handle_telemetry(device_id: str, signals: dict[str, Any]) -> None:
        new_entities: list[MercedesDataActSensor] = []
        for signal_name, signal_data in signals.items():
            if signal_name in EXCLUDED_SIGNALS:
                continue
            val = signal_data.get("value")
            # Skip booleans (handled by binary_sensor) and nested dicts
            if isinstance(val, (bool, dict, list)):
                continue

            unique_key = f"{device_id}_{signal_name}"
            if unique_key not in known_entities:
                known_entities.add(unique_key)
                new_entities.append(
                    MercedesDataActSensor(
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


class MercedesDataActSensor(SensorEntity):
    """Representation of a dynamic Mercedes-Benz EU Data Act telemetry sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        entry_id: str,
        device_id: str,
        signal_name: str,
        initial_data: dict[str, Any],
    ) -> None:
        """Initialize the sensor."""
        self._entry_id = entry_id
        self._device_id = device_id
        self._signal_name = signal_name
        self._attr_unique_id = f"{DOMAIN}_{device_id}_{signal_name}"
        self._attr_name = signal_name.replace("_", " ").title()
        self._attr_native_value = initial_data.get("value")
        self._last_timestamp = initial_data.get("timestamp")

        raw_unit = initial_data.get("unit")
        if raw_unit in UNIT_MAP:
            dev_class, unit, state_class = UNIT_MAP[raw_unit]
            if raw_unit == "%" and not any(
                k in signal_name.lower() for k in ("battery", "soc", "state_of_charge")
            ):
                dev_class = None
            self._attr_device_class = dev_class
            self._attr_native_unit_of_measurement = unit
            self._attr_state_class = state_class
        elif raw_unit:
            self._attr_native_unit_of_measurement = raw_unit

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device_id)},
            name=f"Mercedes-Benz {device_id}",
            manufacturer="Mercedes-Benz",
            model="EU Data Act Connected Device",
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional signal metadata."""
        return {
            "raw_signal": self._signal_name,
            "last_signal_timestamp": self._last_timestamp,
        }

    async def async_added_to_hass(self) -> None:
        """Subscribe to real-time updates when added to Home Assistant."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                f"{SIGNAL_NEW_TELEMETRY}_{self._entry_id}",
                self._async_update_signal,
            )
        )

    @callback
    def _async_update_signal(self, device_id: str, signals: dict[str, Any]) -> None:
        """Update sensor value when matching device signal arrives."""
        if device_id != self._device_id or self._signal_name not in signals:
            return
        data = signals[self._signal_name]
        self._attr_native_value = data.get("value")
        self._last_timestamp = data.get("timestamp")
        self.async_write_ha_state()
