"""Flume water usage sensors."""

from typing import override

from pyflumeng import (
    Budget,
    Device,
    FlumeAuth,
    FlumeData,
    FlumePortalAuth,
    UsageBreakdownCategory,
)
from requests import Session

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    PERCENTAGE,
    EntityCategory,
    UnitOfVolume,
    UnitOfVolumeFlowRate,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.typing import StateType

from .const import DEVICE_SCAN_INTERVAL, FLUME_TYPE_SENSOR
from .coordinator import (
    FlumeConfigEntry,
    FlumeCurrentFlowDataUpdateCoordinator,
    FlumeDeviceDataUpdateCoordinator,
    FlumeNotificationDataUpdateCoordinator,
    FlumePortalDeviceDataUpdateCoordinator,
    FlumeUsageBreakdownDataUpdateCoordinator,
    get_portal_device_coordinator,
    get_usage_breakdown_coordinator,
    portal_fast_polling_enabled,
)
from .entity import FlumeEntity
from .util import get_valid_flume_devices

FLUME_QUERIES_SENSOR: tuple[SensorEntityDescription, ...] = (
    SensorEntityDescription(
        key="current_interval",
        translation_key="current_interval",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_MINUTE,
        device_class=SensorDeviceClass.VOLUME_FLOW_RATE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="month_to_date",
        translation_key="month_to_date",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="week_to_date",
        translation_key="week_to_date",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="today",
        translation_key="today",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="last_60_min",
        translation_key="last_60_min",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_HOUR,
        device_class=SensorDeviceClass.VOLUME_FLOW_RATE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="last_24_hrs",
        translation_key="last_24_hrs",
        suggested_display_precision=2,
        native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_DAY,
        device_class=SensorDeviceClass.VOLUME_FLOW_RATE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="last_30_days",
        translation_key="last_30_days",
        suggested_display_precision=2,
        native_unit_of_measurement=f"{UnitOfVolume.GALLONS}/mo",
        state_class=SensorStateClass.MEASUREMENT,
    ),
)

CURRENT_FLOW_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="live_current_flow",
    translation_key="live_current_flow",
    suggested_display_precision=2,
    native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_MINUTE,
    device_class=SensorDeviceClass.VOLUME_FLOW_RATE,
    state_class=SensorStateClass.MEASUREMENT,
)

UNREAD_NOTIFICATIONS_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="unread_notifications",
    translation_key="unread_notifications",
    icon="mdi:bell-badge",
    state_class=SensorStateClass.MEASUREMENT,
)

BUDGET_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="budget_usage",
    translation_key="budget_usage",
    native_unit_of_measurement=UnitOfVolume.GALLONS,
    device_class=SensorDeviceClass.WATER,
    state_class=SensorStateClass.TOTAL,
    suggested_display_precision=1,
)

BUDGET_TARGET_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="budget_target",
    translation_key="budget_target",
    native_unit_of_measurement=UnitOfVolume.GALLONS,
    device_class=SensorDeviceClass.WATER,
    state_class=SensorStateClass.TOTAL,
    suggested_display_precision=1,
)

BUDGET_REMAINING_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="budget_remaining",
    translation_key="budget_remaining",
    native_unit_of_measurement=UnitOfVolume.GALLONS,
    device_class=SensorDeviceClass.WATER,
    state_class=SensorStateClass.TOTAL,
    suggested_display_precision=1,
)

BUDGET_PERCENTAGE_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="budget_percentage",
    translation_key="budget_percentage",
    native_unit_of_measurement=PERCENTAGE,
    state_class=SensorStateClass.MEASUREMENT,
    suggested_display_precision=1,
)

BUDGET_SENSOR_DESCRIPTIONS = {
    "used": BUDGET_SENSOR_DESCRIPTION,
    "target": BUDGET_TARGET_SENSOR_DESCRIPTION,
    "remaining": BUDGET_REMAINING_SENSOR_DESCRIPTION,
    "percentage": BUDGET_PERCENTAGE_SENSOR_DESCRIPTION,
}

RATE_LIMIT_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="rate_limit_remaining",
    translation_key="rate_limit_remaining",
    entity_category=EntityCategory.DIAGNOSTIC,
    icon="mdi:api",
    state_class=SensorStateClass.MEASUREMENT,
)

USAGE_BREAKDOWN_TOTAL_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="usage_breakdown_total",
    translation_key="usage_breakdown_total",
    native_unit_of_measurement=UnitOfVolume.GALLONS,
    device_class=SensorDeviceClass.WATER,
    state_class=SensorStateClass.TOTAL,
    suggested_display_precision=2,
)

USAGE_BREAKDOWN_CATEGORY_SENSOR_DESCRIPTION = SensorEntityDescription(
    key="usage_breakdown_category",
    native_unit_of_measurement=UnitOfVolume.GALLONS,
    device_class=SensorDeviceClass.WATER,
    state_class=SensorStateClass.TOTAL,
    suggested_display_precision=2,
)


def make_flume_datas(
    http_session: Session,
    flume_auth: FlumeAuth | FlumePortalAuth,
    flume_devices: list[Device],
) -> dict[str, FlumeData]:
    """Create FlumeData objects for each device."""
    flume_datas: dict[str, FlumeData] = {}
    for device in flume_devices:
        assert device.id is not None
        assert device.location is not None
        device_id = str(device.id)
        device_timezone = device.location.tz or "UTC"
        flume_data = FlumeData(
            flume_auth,
            device_id,
            device_timezone,
            scan_interval=DEVICE_SCAN_INTERVAL,
            update_on_init=False,
            http_session=http_session,
        )
        flume_datas[device_id] = flume_data
    return flume_datas


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the Flume sensor."""

    flume_domain_data = config_entry.runtime_data
    flume_devices = flume_domain_data.devices
    flume_auth = flume_domain_data.auth
    http_session = flume_domain_data.http_session
    flume_devices = [
        device
        for device in get_valid_flume_devices(flume_devices)
        if device.type == FLUME_TYPE_SENSOR
    ]
    flume_entity_list: list[
        FlumeSensor
        | FlumeCurrentFlowSensor
        | FlumeUnreadNotificationsSensor
        | FlumeBudgetSensor
        | FlumeRateLimitSensor
        | FlumeUsageBreakdownTotalSensor
        | FlumeUsageBreakdownCategorySensor
    ] = []
    flume_datas = await hass.async_add_executor_job(
        make_flume_datas, http_session, flume_auth, flume_devices
    )

    for device in flume_devices:
        assert device.id is not None
        device_id = str(device.id)
        flume_device = flume_datas[device_id]

        coordinator = FlumeDeviceDataUpdateCoordinator(
            hass=hass, config_entry=config_entry, flume_device=flume_device
        )
        current_flow_coordinator = FlumeCurrentFlowDataUpdateCoordinator(
            hass=hass,
            config_entry=config_entry,
            client=flume_domain_data.client,
            device_id=device_id,
        )

        flume_entity_list.extend(
            [
                FlumeSensor(
                    coordinator=coordinator,
                    description=description,
                    device=device,
                )
                for description in FLUME_QUERIES_SENSOR
            ]
        )
        flume_entity_list.append(
            FlumeCurrentFlowSensor(
                coordinator=current_flow_coordinator,
                description=CURRENT_FLOW_SENSOR_DESCRIPTION,
                device=device,
            )
        )
        flume_entity_list.append(
            FlumeUnreadNotificationsSensor(
                coordinator=flume_domain_data.notifications_coordinator,
                description=UNREAD_NOTIFICATIONS_SENSOR_DESCRIPTION,
                device=device,
            )
        )

        if "portal_api" in flume_auth.capabilities:
            portal_coordinator = get_portal_device_coordinator(
                hass, config_entry, device_id
            )
            known_budget_entities: set[tuple[str, str]] = set()

            @callback
            def async_add_new_budget_entities(
                coordinator: FlumePortalDeviceDataUpdateCoordinator = portal_coordinator,
                flume_device: Device = device,
                known_entities: set[tuple[str, str]] = known_budget_entities,
            ) -> None:
                """Add entities for budgets discovered after initial setup."""
                new_entities: list[FlumeBudgetSensor] = []
                for budget_id in coordinator.data.budgets:
                    for metric in BUDGET_SENSOR_DESCRIPTIONS:
                        key = (budget_id, metric)
                        if key in known_entities:
                            continue
                        known_entities.add(key)
                        new_entities.append(
                            FlumeBudgetSensor(
                                coordinator, flume_device, budget_id, metric
                            )
                        )
                if new_entities:
                    async_add_entities(new_entities)

            for budget_id in portal_coordinator.data.budgets:
                for metric in BUDGET_SENSOR_DESCRIPTIONS:
                    known_budget_entities.add((budget_id, metric))
                    flume_entity_list.append(
                        FlumeBudgetSensor(portal_coordinator, device, budget_id, metric)
                    )
            config_entry.async_on_unload(
                portal_coordinator.async_add_listener(async_add_new_budget_entities)
            )

            breakdown_coordinator = get_usage_breakdown_coordinator(
                hass, config_entry, device
            )
            await breakdown_coordinator.async_refresh()
            flume_entity_list.append(
                FlumeUsageBreakdownTotalSensor(breakdown_coordinator, device)
            )
            known_categories = {
                category.type
                for category in (
                    breakdown_coordinator.data.categories
                    if breakdown_coordinator.last_update_success
                    and breakdown_coordinator.data is not None
                    else []
                )
            }
            flume_entity_list.extend(
                FlumeUsageBreakdownCategorySensor(
                    breakdown_coordinator, device, category.type
                )
                for category in (
                    breakdown_coordinator.data.categories
                    if breakdown_coordinator.last_update_success
                    and breakdown_coordinator.data is not None
                    else []
                )
            )

            @callback
            def async_add_new_breakdown_categories(
                coordinator: FlumeUsageBreakdownDataUpdateCoordinator = breakdown_coordinator,
                flume_device: Device = device,
                known_types: set[str] = known_categories,
            ) -> None:
                """Add newly introduced Flume usage classifications."""
                if coordinator.data is None:
                    return
                new_entities = [
                    FlumeUsageBreakdownCategorySensor(
                        coordinator, flume_device, category.type
                    )
                    for category in coordinator.data.categories
                    if category.type not in known_types
                ]
                known_types.update(
                    category.type
                    for category in coordinator.data.categories
                    if category.type not in known_types
                )
                if new_entities:
                    async_add_entities(new_entities)

            config_entry.async_on_unload(
                breakdown_coordinator.async_add_listener(
                    async_add_new_breakdown_categories
                )
            )

    if flume_devices:
        flume_entity_list.append(
            FlumeRateLimitSensor(
                flume_domain_data.notifications_coordinator,
                flume_devices[0],
            )
        )

    async_add_entities(flume_entity_list)


class FlumeSensor(FlumeEntity[FlumeDeviceDataUpdateCoordinator], SensorEntity):
    """Representation of the Flume sensor."""

    @property
    @override
    def native_value(self) -> StateType:
        """Return the state of the sensor."""
        sensor_key = self.entity_description.key
        if sensor_key not in self.coordinator.flume_device.values:
            return None

        return self.coordinator.flume_device.values[sensor_key]


class FlumeCurrentFlowSensor(
    FlumeEntity[FlumeCurrentFlowDataUpdateCoordinator], SensorEntity
):
    """Representation of Flume's live current-flow reading."""

    @property
    @override
    def native_value(self) -> StateType:
        """Return the current flow in gallons per minute."""
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.gpm

    @property
    @override
    def extra_state_attributes(self) -> dict[str, bool | str | None]:
        """Expose activity and the source reading timestamp."""
        if self.coordinator.data is None:
            return {"active": None, "datetime": None}
        return {
            "active": self.coordinator.data.active,
            "datetime": self.coordinator.data.datetime,
        }


class FlumeUnreadNotificationsSensor(
    FlumeEntity[FlumeNotificationDataUpdateCoordinator], SensorEntity
):
    """Count unread notifications for one water sensor."""

    @property
    @override
    def native_value(self) -> int:
        """Return the unread notification count."""
        return sum(
            notification.device_id is not None
            and str(notification.device_id) == self.device_id
            and not notification.read
            for notification in self.coordinator.notifications
        )

    @property
    @override
    def extra_state_attributes(self) -> dict[str, str | None]:
        """Expose the newest unread notification."""
        unread = [
            notification
            for notification in self.coordinator.notifications
            if notification.device_id is not None
            and str(notification.device_id) == self.device_id
            and not notification.read
        ]
        if not unread:
            return {
                "latest_title": None,
                "latest_message": None,
                "latest_datetime": None,
            }
        latest = unread[0]
        return {
            "latest_title": latest.title,
            "latest_message": latest.message,
            "latest_datetime": latest.created_datetime,
        }


class FlumeBudgetSensor(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SensorEntity
):
    """Show current usage against one Flume budget."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        budget_id: str,
        metric: str,
    ) -> None:
        """Initialize a budget sensor."""
        description = BUDGET_SENSOR_DESCRIPTIONS[metric]
        super().__init__(coordinator, description, device)
        self.budget_id = budget_id
        self.metric = metric
        self._attr_unique_id = f"{description.key}_{self.device_id}_{budget_id}"
        budget_name = self.budget.name or f"Budget {budget_id}"
        self._attr_name = budget_name if metric == "used" else f"{budget_name} {metric}"

    @property
    def budget(self) -> Budget:
        """Return the latest budget data."""
        return self.coordinator.data.budgets[self.budget_id]

    @property
    @override
    def available(self) -> bool:
        """Return whether the budget still exists."""
        return super().available and self.budget_id in self.coordinator.data.budgets

    @property
    @override
    def native_value(self) -> float | None:
        """Return the selected budget progress metric."""
        budget = self.budget
        if self.metric == "used":
            return budget.used
        if self.metric == "target":
            return budget.target
        if self.metric == "remaining":
            return budget.remaining
        return budget.percentage_used

    @property
    @override
    def extra_state_attributes(
        self,
    ) -> dict[str, str | float | int | bool | list[int] | None]:
        """Expose target, period, thresholds, and progress."""
        budget = self.budget
        return {
            "used": budget.used,
            "target": budget.target,
            "remaining": budget.remaining,
            "percentage": (
                round(budget.percentage_used, 1)
                if budget.percentage_used is not None
                else None
            ),
            "over_budget": budget.is_over_budget,
            "period": budget.type,
            "start_date": budget.start_date,
            "end_date": budget.end_date,
            "thresholds": budget.thresholds,
            "recur_multiplier": budget.recur_multiplier,
        }


class FlumeRateLimitSensor(
    FlumeEntity[FlumeNotificationDataUpdateCoordinator], SensorEntity
):
    """Expose the account's latest server-advertised API quota state."""

    def __init__(
        self,
        coordinator: FlumeNotificationDataUpdateCoordinator,
        device: Device,
    ) -> None:
        """Initialize the diagnostic sensor."""
        super().__init__(coordinator, RATE_LIMIT_SENSOR_DESCRIPTION, device)
        self._attr_unique_id = f"rate_limit_remaining_{self.device_id}"

    @property
    @override
    def native_value(self) -> int | None:
        """Return requests remaining in the current Flume quota window."""
        return self.coordinator.client.rate_limit.remaining

    @property
    @override
    def extra_state_attributes(self) -> dict[str, int | str | bool | None]:
        """Expose the complete rate-limit envelope and polling mode."""
        rate_limit = self.coordinator.client.rate_limit
        return {
            "baseline": rate_limit.baseline,
            "limit": rate_limit.limit,
            "reset": rate_limit.reset,
            "reset_at": (
                rate_limit.reset_at.isoformat()
                if rate_limit.reset_at is not None
                else None
            ),
            "retry_after": rate_limit.retry_after,
            "fast_polling": portal_fast_polling_enabled(self.coordinator.client),
        }


class FlumeUsageBreakdownTotalSensor(
    FlumeEntity[FlumeUsageBreakdownDataUpdateCoordinator], SensorEntity
):
    """Expose total usage for the current calendar month."""

    def __init__(
        self,
        coordinator: FlumeUsageBreakdownDataUpdateCoordinator,
        device: Device,
    ) -> None:
        """Initialize the total sensor."""
        super().__init__(coordinator, USAGE_BREAKDOWN_TOTAL_SENSOR_DESCRIPTION, device)

    @property
    @override
    def native_value(self) -> float | None:
        """Return total month-to-date usage."""
        return (
            self.coordinator.data.total_usage
            if self.coordinator.data is not None
            else None
        )

    @property
    @override
    def extra_state_attributes(self) -> dict[str, str | None]:
        """Expose the exact breakdown window."""
        if self.coordinator.data is None:
            return {"since_datetime": None, "until_datetime": None, "units": None}
        return {
            "since_datetime": self.coordinator.data.since_datetime,
            "until_datetime": self.coordinator.data.until_datetime,
            "units": self.coordinator.data.units,
        }


class FlumeUsageBreakdownCategorySensor(
    FlumeEntity[FlumeUsageBreakdownDataUpdateCoordinator], SensorEntity
):
    """Expose one classified usage category for the current calendar month."""

    def __init__(
        self,
        coordinator: FlumeUsageBreakdownDataUpdateCoordinator,
        device: Device,
        category_type: str,
    ) -> None:
        """Initialize a classification sensor."""
        super().__init__(
            coordinator, USAGE_BREAKDOWN_CATEGORY_SENSOR_DESCRIPTION, device
        )
        self.category_type = category_type
        self._attr_unique_id = (
            f"usage_breakdown_{self.device_id}_{category_type.lower()}"
        )
        category = self.category
        assert category is not None
        self._attr_name = category.display_name

    @property
    def category(self) -> UsageBreakdownCategory | None:
        """Return the latest category model."""
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.category(self.category_type)

    @property
    @override
    def available(self) -> bool:
        """Return whether this classification exists in the latest breakdown."""
        return super().available and self.category is not None

    @property
    @override
    def native_value(self) -> float | None:
        """Return usage attributed to this category."""
        category = self.category
        return category.usage if category is not None else None

    @property
    @override
    def extra_state_attributes(self) -> dict[str, float | int | str | None]:
        """Expose category share and span count."""
        category = self.category
        if category is None:
            return {
                "type": self.category_type,
                "percentage": None,
                "rounded_percentage": None,
                "span_count": 0,
            }
        return {
            "type": category.type,
            "percentage": round(category.percentage, 1),
            "rounded_percentage": category.rounded_percentage,
            "span_count": category.span_count,
        }
