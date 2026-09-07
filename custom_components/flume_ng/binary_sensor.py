"""Flume binary sensors."""

from dataclasses import dataclass
from typing import override

from pyflumeng import Device, DoNotAlertSchedule

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import (
    FLUME_TYPE_BRIDGE,
    FLUME_TYPE_SENSOR,
    NOTIFICATION_HIGH_FLOW,
    NOTIFICATION_LEAK_DETECTED,
)
from .coordinator import (
    FlumeConfigEntry,
    FlumeDeviceConnectionUpdateCoordinator,
    FlumeNotificationDataUpdateCoordinator,
    FlumePortalDeviceDataUpdateCoordinator,
    get_portal_device_coordinator,
)
from .entity import FlumeEntity
from .util import get_valid_flume_devices

BINARY_SENSOR_DESCRIPTION_CONNECTED = BinarySensorEntityDescription(
    key="connected", device_class=BinarySensorDeviceClass.CONNECTIVITY
)

BINARY_SENSOR_DESCRIPTION_LOW_BATTERY = BinarySensorEntityDescription(
    key="low_battery",
    entity_category=EntityCategory.DIAGNOSTIC,
    device_class=BinarySensorDeviceClass.BATTERY,
)

SCHEDULE_ACTIVE_DESCRIPTION = BinarySensorEntityDescription(
    key="do_not_alert_schedule_active",
    translation_key="do_not_alert_schedule_active",
    icon="mdi:calendar-clock",
)

# Levels reported by the devices endpoint. Unlisted values map to unknown
# rather than to a healthy battery.
BATTERY_LEVEL_IS_LOW = {"low": True, "medium": False, "high": False}


@dataclass(frozen=True, kw_only=True)
class FlumeBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes a binary sensor entity."""

    event_rule: str


FLUME_BINARY_NOTIFICATION_SENSORS: tuple[FlumeBinarySensorEntityDescription, ...] = (
    FlumeBinarySensorEntityDescription(
        key="leak",
        translation_key="leak",
        entity_category=EntityCategory.DIAGNOSTIC,
        event_rule=NOTIFICATION_LEAK_DETECTED,
    ),
    FlumeBinarySensorEntityDescription(
        key="flow",
        translation_key="flow",
        entity_category=EntityCategory.DIAGNOSTIC,
        event_rule=NOTIFICATION_HIGH_FLOW,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up a Flume binary sensor.."""
    flume_domain_data = config_entry.runtime_data
    flume_devices = flume_domain_data.devices

    flume_entity_list: list[
        FlumeNotificationBinarySensor
        | FlumeConnectionBinarySensor
        | FlumeLowBatteryBinarySensor
        | FlumeDoNotAlertScheduleBinarySensor
    ] = []

    connection_coordinator = FlumeDeviceConnectionUpdateCoordinator(
        hass=hass, config_entry=config_entry, client=flume_domain_data.client
    )
    notification_coordinator = flume_domain_data.notifications_coordinator
    flume_devices = get_valid_flume_devices(flume_devices)
    for device in flume_devices:
        connection_sensor = FlumeConnectionBinarySensor(
            coordinator=connection_coordinator,
            description=BINARY_SENSOR_DESCRIPTION_CONNECTED,
            device=device,
            is_bridge=(device.type == FLUME_TYPE_BRIDGE),
        )

        flume_entity_list.append(connection_sensor)

        if device.type != FLUME_TYPE_SENSOR:
            continue

        flume_entity_list.append(
            FlumeLowBatteryBinarySensor(
                coordinator=connection_coordinator,
                description=BINARY_SENSOR_DESCRIPTION_LOW_BATTERY,
                device=device,
            )
        )

        # Build notification sensors
        flume_entity_list.extend(
            [
                FlumeNotificationBinarySensor(
                    coordinator=notification_coordinator,
                    description=description,
                    device=device,
                )
                for description in FLUME_BINARY_NOTIFICATION_SENSORS
            ]
        )

        if "portal_api" in flume_domain_data.auth.capabilities:
            assert device.id is not None
            portal_coordinator = get_portal_device_coordinator(
                hass, config_entry, str(device.id)
            )
            known_schedule_ids = set(portal_coordinator.data.schedules)
            flume_entity_list.extend(
                FlumeDoNotAlertScheduleBinarySensor(
                    portal_coordinator,
                    device,
                    schedule_id,
                )
                for schedule_id in portal_coordinator.data.schedules
            )

            @callback
            def async_add_new_schedule_entities(
                coordinator: FlumePortalDeviceDataUpdateCoordinator = portal_coordinator,
                flume_device: Device = device,
                known_ids: set[str] = known_schedule_ids,
            ) -> None:
                """Add binary sensors for newly created do-not-alert schedules."""
                new_schedule_ids = coordinator.data.schedules.keys() - known_ids
                if not new_schedule_ids:
                    return
                known_ids.update(new_schedule_ids)
                async_add_entities(
                    FlumeDoNotAlertScheduleBinarySensor(
                        coordinator, flume_device, schedule_id
                    )
                    for schedule_id in new_schedule_ids
                )

            config_entry.async_on_unload(
                portal_coordinator.async_add_listener(async_add_new_schedule_entities)
            )

    async_add_entities(flume_entity_list)


class FlumeNotificationBinarySensor(
    FlumeEntity[FlumeNotificationDataUpdateCoordinator], BinarySensorEntity
):
    """Binary sensor class."""

    entity_description: FlumeBinarySensorEntityDescription

    @property
    @override
    def is_on(self) -> bool:
        """Return on state."""
        return bool(
            (
                notifications := self.coordinator.active_notifications_by_device.get(
                    self.device_id
                )
            )
            and self.entity_description.event_rule in notifications
        )


class FlumeLowBatteryBinarySensor(
    FlumeEntity[FlumeDeviceConnectionUpdateCoordinator], BinarySensorEntity
):
    """Binary sensor class for the sensor battery level."""

    @property
    @override
    def is_on(self) -> bool | None:
        """Return True when the device reports a low battery."""
        return BATTERY_LEVEL_IS_LOW.get(
            self.coordinator.battery_level.get(self.device_id, "")
        )


class FlumeConnectionBinarySensor(
    FlumeEntity[FlumeDeviceConnectionUpdateCoordinator], BinarySensorEntity
):
    """Binary Sensor class for WIFI Connection status."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY

    @property
    @override
    def is_on(self) -> bool:
        """Return connection status."""
        return bool(
            (connected := self.coordinator.connected) and connected[self.device_id]
        )


class FlumeDoNotAlertScheduleBinarySensor(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], BinarySensorEntity
):
    """Show whether a do-not-alert schedule is active right now."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        schedule_id: str,
    ) -> None:
        """Initialize a schedule status sensor."""
        super().__init__(coordinator, SCHEDULE_ACTIVE_DESCRIPTION, device)
        self.schedule_id = schedule_id
        self._attr_unique_id = (
            f"do_not_alert_schedule_active_{self.device_id}_{schedule_id}"
        )
        self._attr_name = self.schedule.name or f"Schedule {schedule_id}"

    @property
    def schedule(self) -> DoNotAlertSchedule:
        """Return the latest schedule data."""
        return self.coordinator.data.schedules[self.schedule_id]

    @property
    @override
    def available(self) -> bool:
        """Return whether the schedule still exists."""
        return super().available and self.schedule_id in self.coordinator.data.schedules

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether the schedule is active now."""
        if not self.available:
            return None
        return self.schedule.currently_active

    @property
    @override
    def extra_state_attributes(self) -> dict[str, str | list[str]]:
        """Expose the schedule timing and recurrence details."""
        schedule = self.schedule
        return {
            "description": schedule.description,
            "start_time": schedule.start_time,
            "end_time": schedule.end_time,
            "last_start": schedule.last_start,
            "last_end": schedule.last_end,
            "next_start": schedule.next_start,
            "next_end": schedule.next_end,
            "span_types": schedule.span_types,
            "recurrence": schedule.rrule_str,
        }
