"""Utilities for Flume."""

from collections.abc import Iterable

from pyflumeng import Device

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from .const import CONF_TOKEN


def get_valid_flume_devices(flume_devices: Iterable[Device]) -> list[Device]:
    """Return a list of Flume devices that have a valid location."""
    return [
        device
        for device in flume_devices
        if device.id is not None
        and device.location is not None
        and device.location.name
    ]


@callback
def async_persist_auth_token(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Persist a refreshed PyFlumeNG OAuth token in the config entry."""
    auth = entry.runtime_data.auth
    token = auth.token
    if token is None or entry.data.get(CONF_TOKEN) == token:
        return
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_TOKEN: dict(token)},
    )
