"""Platform for shared base classes for sensors."""

from typing import override

from pyflumeng import Device

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import EntityDescription
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import (
    FlumeCurrentFlowDataUpdateCoordinator,
    FlumeDeviceConnectionUpdateCoordinator,
    FlumeDeviceDataUpdateCoordinator,
    FlumeNotificationDataUpdateCoordinator,
    FlumePortalDeviceDataUpdateCoordinator,
    FlumeUsageBreakdownDataUpdateCoordinator,
)


class FlumeEntity[
    _FlumeCoordinatorT: FlumeDeviceDataUpdateCoordinator
    | FlumeCurrentFlowDataUpdateCoordinator
    | FlumeDeviceConnectionUpdateCoordinator
    | FlumeNotificationDataUpdateCoordinator
    | FlumePortalDeviceDataUpdateCoordinator
    | FlumeUsageBreakdownDataUpdateCoordinator
](CoordinatorEntity[_FlumeCoordinatorT]):
    """Base entity class."""

    _attr_attribution = "Data provided by Flume API"
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: _FlumeCoordinatorT,
        description: EntityDescription,
        device: Device,
        is_bridge: bool = False,
    ) -> None:
        """Class initializer."""
        super().__init__(coordinator)
        assert device.id is not None
        assert device.location is not None
        self.entity_description = description
        self.device_id = str(device.id)

        if is_bridge:
            name = "Flume Bridge"
        else:
            name = "Flume Sensor"

        self._attr_unique_id = f"{description.key}_{self.device_id}"

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, self.device_id)},
            manufacturer="Flume, Inc.",
            model="Flume Smart Water Monitor",
            name=f"{name} {device.location.name}",
            configuration_url="https://portal.flumewater.com",
        )

    @override
    async def async_added_to_hass(self) -> None:
        """Request an update when added."""
        await super().async_added_to_hass()
        # We do not ask for an update with async_add_entities()
        # because it will update disabled entities
        await self.coordinator.async_request_refresh()
