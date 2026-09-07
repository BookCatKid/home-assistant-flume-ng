"""Event entities for Flume notifications."""

from typing import override

from pyflumeng import Device

from homeassistant.components.event import EventEntity, EventEntityDescription
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import FLUME_TYPE_SENSOR
from .coordinator import FlumeConfigEntry, FlumeNotificationDataUpdateCoordinator
from .entity import FlumeEntity
from .util import get_valid_flume_devices

NOTIFICATION_EVENT_DESCRIPTION = EventEntityDescription(
    key="notification_event",
    translation_key="notification_event",
    event_types=["notification"],
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up one notification event entity per Flume water sensor."""
    coordinator = config_entry.runtime_data.notifications_coordinator
    async_add_entities(
        FlumeNotificationEvent(coordinator, device)
        for device in get_valid_flume_devices(config_entry.runtime_data.devices)
        if device.type == FLUME_TYPE_SENSOR
    )


class FlumeNotificationEvent(
    FlumeEntity[FlumeNotificationDataUpdateCoordinator], EventEntity
):
    """Surface newly observed Flume notifications as Home Assistant events."""

    def __init__(
        self,
        coordinator: FlumeNotificationDataUpdateCoordinator,
        device: Device,
    ) -> None:
        """Initialize the event entity."""
        super().__init__(coordinator, NOTIFICATION_EVENT_DESCRIPTION, device)

    @callback
    @override
    def _handle_coordinator_update(self) -> None:
        """Trigger an event for each newly observed notification for this sensor."""
        for payload in self.coordinator.new_notification_payloads:
            if str(payload.get("device_id")) == self.device_id:
                self._trigger_event("notification", payload)
        super()._handle_coordinator_update()
