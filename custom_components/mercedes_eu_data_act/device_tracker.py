"""Device Tracker platform for Package 6005 (Navigation & Positioning Data)."""
from __future__ import annotations

from typing import Any

try:
    from homeassistant.components.device_tracker import SourceType, TrackerEntity
except ImportError:
    from homeassistant.components.device_tracker import SourceType
    from homeassistant.components.device_tracker.config_entry import TrackerEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_NEW_TELEMETRY

LAT_KEYS = ("latitude", "positionLat")
LON_KEYS = ("longitude", "positionLong")


def _extract_coords(signals: dict[str, Any]) -> tuple[float | None, float | None]:
    """Extract latitude and longitude from EU Data Act package 6005 signals."""
    lat = None
    lon = None
    for key in LAT_KEYS:
        if key in signals and signals[key].get("value") is not None:
            lat = float(signals[key]["value"])
            break
    for key in LON_KEYS:
        if key in signals and signals[key].get("value") is not None:
            lon = float(signals[key]["value"])
            break
    if (lat is None or lon is None) and "geoposition" in signals:
        geo = signals["geoposition"].get("value")
        if isinstance(geo, dict):
            lat = geo.get("latitude") or geo.get("lat")
            lon = geo.get("longitude") or geo.get("lon")
    return lat, lon


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up device tracker entities when GPS telemetry arrives."""
    known_trackers: set[str] = set()

    @callback
    def _handle_telemetry(device_id: str, signals: dict[str, Any]) -> None:
        if device_id in known_trackers:
            return
        lat, lon = _extract_coords(signals)
        if lat is not None and lon is not None:
            known_trackers.add(device_id)
            async_add_entities(
                [MercedesDataActTracker(entry.entry_id, device_id, lat, lon)]
            )

    entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{SIGNAL_NEW_TELEMETRY}_{entry.entry_id}",
            _handle_telemetry,
        )
    )


class MercedesDataActTracker(TrackerEntity):
    """GPS Tracker for a Mercedes-Benz vehicle."""

    _attr_has_entity_name = True
    _attr_name = "Location"

    def __init__(
        self, entry_id: str, device_id: str, lat: float, lon: float
    ) -> None:
        """Initialize the tracker."""
        self._entry_id = entry_id
        self._device_id = device_id
        self._attr_unique_id = f"{DOMAIN}_{device_id}_location"
        self._latitude = lat
        self._longitude = lon
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device_id)},
            name=f"Mercedes-Benz {device_id}",
            manufacturer="Mercedes-Benz",
            model="EU Data Act Connected Device",
        )

    @property
    def source_type(self) -> SourceType:
        """Return GPS source type."""
        return SourceType.GPS

    @property
    def latitude(self) -> float | None:
        """Return latitude value."""
        return self._latitude

    @property
    def longitude(self) -> float | None:
        """Return longitude value."""
        return self._longitude

    async def async_added_to_hass(self) -> None:
        """Subscribe to updates."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                f"{SIGNAL_NEW_TELEMETRY}_{self._entry_id}",
                self._async_update_location,
            )
        )

    @callback
    def _async_update_location(self, device_id: str, signals: dict[str, Any]) -> None:
        """Update GPS coordinates."""
        if device_id != self._device_id:
            return
        lat, lon = _extract_coords(signals)
        if lat is not None and lon is not None:
            self._latitude = lat
            self._longitude = lon
            self.async_write_ha_state()
