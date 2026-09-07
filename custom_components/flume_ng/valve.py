"""Flume shutoff valve controls."""

from collections.abc import Mapping
from typing import Any, override

from pyflumeng import Device, Integration

from homeassistant.components.valve import (
    ValveDeviceClass,
    ValveEntity,
    ValveEntityDescription,
    ValveEntityFeature,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import FLUME_TYPE_SENSOR
from .coordinator import (
    FlumeConfigEntry,
    FlumePortalDeviceDataUpdateCoordinator,
    get_portal_device_coordinator,
)
from .entity import FlumeEntity
from .util import get_valid_flume_devices

SHUTOFF_VALVE_DESCRIPTION = ValveEntityDescription(
    key="shutoff_valve",
    translation_key="shutoff_valve",
    device_class=ValveDeviceClass.WATER,
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up connected shutoff valves."""
    runtime_data = config_entry.runtime_data
    if "portal_writes" not in runtime_data.auth.capabilities:
        return

    entities: list[FlumeShutoffValve] = []
    for device in get_valid_flume_devices(runtime_data.devices):
        if device.type != FLUME_TYPE_SENSOR or device.id is None:
            continue
        coordinator = get_portal_device_coordinator(hass, config_entry, str(device.id))
        entities.extend(
            FlumeShutoffValve(coordinator, device, integration_id)
            for integration_id in coordinator.data.integrations
        )

    async_add_entities(entities)


class FlumeShutoffValve(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], ValveEntity
):
    """Control a shutoff valve connected through Flume."""

    _attr_reports_position = False
    _attr_supported_features = ValveEntityFeature.OPEN | ValveEntityFeature.CLOSE

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        integration_id: str,
    ) -> None:
        """Initialize a Flume shutoff valve."""
        super().__init__(coordinator, SHUTOFF_VALVE_DESCRIPTION, device)
        self.integration_id = integration_id
        self._attr_unique_id = f"shutoff_valve_{self.device_id}_{integration_id}"
        self._attr_name = "Shutoff valve"

    @property
    def integration(self) -> Integration:
        """Return the latest integration data."""
        return self.coordinator.data.integrations[self.integration_id]

    @property
    @override
    def available(self) -> bool:
        """Return whether the integration still exists."""
        return (
            super().available
            and self.integration_id in self.coordinator.data.integrations
        )

    def _state(self) -> str | None:
        """Extract the valve state from known Flume integration shapes."""
        integration = self.integration
        info = integration.get("info")
        state: object = info.get("state") if isinstance(info, Mapping) else None
        if state is None:
            state = integration.state
        return state.lower() if isinstance(state, str) else None

    @property
    @override
    def is_closed(self) -> bool | None:
        """Return whether the valve reports a closed state."""
        state = self._state()
        if state == "open":
            return False
        if state in ("close", "closed"):
            return True
        return None

    async def _async_command(self, state: str) -> None:
        """Send a valve command and refresh its cloud state."""
        await self.hass.async_add_executor_job(
            self.coordinator.client.command_integration,
            self.device_id,
            self.integration_id,
            state,
        )
        await self.hass.async_add_executor_job(
            self.coordinator.client.refresh_integration,
            self.device_id,
            self.integration_id,
        )
        await self.coordinator.async_refresh()

    @override
    async def async_open_valve(self, **kwargs: Any) -> None:
        """Open the shutoff valve."""
        await self._async_command("open")

    @override
    async def async_close_valve(self, **kwargs: Any) -> None:
        """Close the shutoff valve."""
        await self._async_command("close")
