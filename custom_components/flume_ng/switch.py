"""Flume switches."""

from typing import Any, override

from pyflumeng import (
    Device,
    Location,
    NotificationPreference,
    Subscription,
    UsageAlertRule,
    UsageAlertSchedule,
)

from homeassistant.components.switch import SwitchEntity, SwitchEntityDescription
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import FLUME_TYPE_SENSOR
from .coordinator import (
    FlumeConfigEntry,
    FlumePortalDeviceDataUpdateCoordinator,
    get_portal_device_coordinator,
)
from .entity import FlumeEntity
from .util import get_valid_flume_devices

USAGE_ALERT_RULE_DESCRIPTION = SwitchEntityDescription(key="usage_alert_rule")
USAGE_ALERT_SCHEDULE_DESCRIPTION = SwitchEntityDescription(key="usage_alert_schedule")
USAGE_ALERT_SHUTOFF_DESCRIPTION = SwitchEntityDescription(key="usage_alert_shutoff")
AWAY_MODE_DESCRIPTION = SwitchEntityDescription(
    key="away_mode",
    translation_key="away_mode",
    icon="mdi:home-export-outline",
)
NOTIFICATION_PREFERENCE_DESCRIPTION = SwitchEntityDescription(
    key="notification_preference",
    icon="mdi:bell-outline",
    entity_category=EntityCategory.CONFIG,
)

NOTIFICATION_PREFERENCES: tuple[tuple[NotificationPreference, str], ...] = (
    (NotificationPreference.USAGE_ALERT, "Usage alerts"),
    (NotificationPreference.BUDGET, "Budget"),
    (NotificationPreference.GENERAL, "General"),
    (NotificationPreference.CONNECTION, "Connection"),
    (NotificationPreference.BATTERY, "Battery"),
    (NotificationPreference.DEVICE_MOVED, "Device moved"),
)


def _notification_channel_name(alert_type: str | None) -> str:
    """Return a readable channel name without exposing destination details."""
    if alert_type is None:
        return "Notification"
    match alert_type.lower():
        case "email":
            return "Email"
        case "ios":
            return "iOS"
        case "android":
            return "Android"
        case _:
            return alert_type.title()


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up usage-alert rule switches for portal-authenticated accounts."""
    runtime_data = config_entry.runtime_data
    if "portal_writes" not in runtime_data.auth.capabilities:
        return

    away_location_ids: set[str] = set()
    notification_subscription_ids: set[str] = set()
    for device in get_valid_flume_devices(runtime_data.devices):
        if device.type != FLUME_TYPE_SENSOR or device.id is None:
            continue
        coordinator = get_portal_device_coordinator(hass, config_entry, str(device.id))
        known_entities: set[tuple[str, ...]] = set()

        @callback
        def async_add_new_entities(
            portal_coordinator: FlumePortalDeviceDataUpdateCoordinator = coordinator,
            flume_device: Device = device,
            seen: set[tuple[str, ...]] = known_entities,
            seen_subscriptions: set[str] = notification_subscription_ids,
        ) -> None:
            """Add switches for rules and associations discovered after setup."""
            new_entities: list[
                FlumeUsageAlertRuleSwitch
                | FlumeUsageAlertScheduleSwitch
                | FlumeUsageAlertShutoffSwitch
                | FlumeNotificationPreferenceSwitch
            ] = []
            for rule_id, rule in portal_coordinator.data.rules.items():
                rule_key = ("rule", rule_id)
                if rule_key not in seen:
                    seen.add(rule_key)
                    new_entities.append(
                        FlumeUsageAlertRuleSwitch(
                            portal_coordinator, flume_device, rule_id
                        )
                    )
                for schedule in rule.schedules:
                    if schedule.schedule_id is None:
                        continue
                    schedule_id = str(schedule.schedule_id)
                    schedule_key = ("schedule", rule_id, schedule_id)
                    if schedule_key in seen:
                        continue
                    seen.add(schedule_key)
                    new_entities.append(
                        FlumeUsageAlertScheduleSwitch(
                            portal_coordinator,
                            flume_device,
                            rule_id,
                            schedule_id,
                        )
                    )
                shutoff_key = ("shutoff", rule_id)
                if rule.shutoff_config is not None and shutoff_key not in seen:
                    seen.add(shutoff_key)
                    new_entities.append(
                        FlumeUsageAlertShutoffSwitch(
                            portal_coordinator, flume_device, rule_id
                        )
                    )
            for subscription_id, subscription in portal_coordinator.data.subscriptions.items():
                if subscription_id in seen_subscriptions:
                    continue
                new_entities.extend(
                    FlumeNotificationPreferenceSwitch(
                        portal_coordinator,
                        flume_device,
                        subscription_id,
                        _notification_channel_name(subscription.alert_type),
                        notification_type,
                        preference_name,
                    )
                    for notification_type, preference_name in NOTIFICATION_PREFERENCES
                )
                seen_subscriptions.add(subscription_id)
            if new_entities:
                async_add_entities(new_entities)

        if (
            coordinator.data.location is not None
            and coordinator.data.location.id is not None
            and str(coordinator.data.location.id) not in away_location_ids
        ):
            async_add_entities([FlumeAwayModeSwitch(coordinator, device)])
            away_location_ids.add(str(coordinator.data.location.id))
        if (
            coordinator.data.location is not None
            and coordinator.data.location.id is not None
        ):
            for subscription_id, subscription in coordinator.data.subscriptions.items():
                if subscription_id in notification_subscription_ids:
                    continue
                async_add_entities(
                    FlumeNotificationPreferenceSwitch(
                        coordinator,
                        device,
                        subscription_id,
                        _notification_channel_name(subscription.alert_type),
                        notification_type,
                        preference_name,
                    )
                    for notification_type, preference_name in NOTIFICATION_PREFERENCES
                )
                notification_subscription_ids.add(subscription_id)
        for rule_id, rule in coordinator.data.rules.items():
            known_entities.add(("rule", rule_id))
            async_add_entities(
                [FlumeUsageAlertRuleSwitch(coordinator, device, rule_id)]
            )
            schedule_entities = []
            for schedule in rule.schedules:
                if schedule.schedule_id is None:
                    continue
                schedule_id = str(schedule.schedule_id)
                known_entities.add(("schedule", rule_id, schedule_id))
                schedule_entities.append(
                    FlumeUsageAlertScheduleSwitch(
                        coordinator,
                        device,
                        rule_id,
                        schedule_id,
                    )
                )
            if schedule_entities:
                async_add_entities(schedule_entities)
            if rule.shutoff_config is not None:
                known_entities.add(("shutoff", rule_id))
                async_add_entities(
                    [FlumeUsageAlertShutoffSwitch(coordinator, device, rule_id)]
                )
        config_entry.async_on_unload(
            coordinator.async_add_listener(async_add_new_entities)
        )


class FlumeUsageAlertRuleSwitch(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SwitchEntity
):
    """Enable or disable a Flume usage-alert rule."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        rule_id: str,
    ) -> None:
        """Initialize a rule switch."""
        super().__init__(coordinator, USAGE_ALERT_RULE_DESCRIPTION, device)
        self.rule_id = rule_id
        self._attr_unique_id = f"usage_alert_rule_{self.device_id}_{rule_id}"
        self._attr_name = self.rule.name or f"Usage alert {rule_id}"

    @property
    def rule(self) -> UsageAlertRule:
        """Return the latest representation of this rule."""
        return self.coordinator.data.rules[self.rule_id]

    @property
    @override
    def available(self) -> bool:
        """Return whether this rule still exists and the coordinator is healthy."""
        return super().available and self.rule_id in self.coordinator.data.rules

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether the rule is enabled."""
        rule = self.coordinator.data.rules.get(self.rule_id)
        return rule.active if rule is not None else None

    @property
    @override
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose the rule fields useful when building automations."""
        rule = self.rule
        return {
            "flow_rate": rule.flow_rate,
            "duration": rule.duration,
            "notify_every": rule.notify_every,
            "advanced_low_flow": rule.advanced_low_flow,
            "notification_type": rule.notification_type,
            "shutoff_active": (
                rule.shutoff_config.active if rule.shutoff_config is not None else None
            ),
            "schedules": [schedule.to_dict() for schedule in rule.schedules],
        }

    async def _async_set_active(self, active: bool) -> None:
        """Set the rule state through the portal write endpoint."""
        await self.hass.async_add_executor_job(
            self.coordinator.client.set_usage_alert_rule_active,
            self.device_id,
            self.rule_id,
            active,
        )
        # Refresh immediately after a user-initiated write so Home Assistant
        # reflects the server's new rule state without waiting for the
        # coordinator's refresh debouncer.
        await self.coordinator.async_refresh()

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable the usage-alert rule."""
        await self._async_set_active(True)

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable the usage-alert rule."""
        await self._async_set_active(False)


class FlumeNotificationPreferenceSwitch(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SwitchEntity
):
    """Control one known bit in a notification-channel subscription."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        subscription_id: str,
        channel_name: str,
        notification_type: NotificationPreference,
        preference_name: str,
    ) -> None:
        """Initialize a notification preference switch."""
        super().__init__(coordinator, NOTIFICATION_PREFERENCE_DESCRIPTION, device)
        self.subscription_id = subscription_id
        self.channel_name = channel_name
        self.notification_type = notification_type
        self.preference_name = preference_name
        self._attr_unique_id = (
            f"notification_preference_{subscription_id}_{int(notification_type)}"
        )
        self._attr_name = f"{channel_name} notifications: {preference_name}"

    @property
    def subscription(self) -> Subscription | None:
        """Return the latest subscription for this notification channel."""
        return self.coordinator.data.subscriptions.get(self.subscription_id)

    @property
    @override
    def available(self) -> bool:
        """Return whether this notification subscription still exists."""
        return super().available and self.subscription_id in self.coordinator.data.subscriptions

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether this notification bit is enabled."""
        subscription = self.subscription
        if subscription is None:
            return None
        return subscription.has_notification_type(int(self.notification_type))

    async def _async_set_active(self, active: bool) -> None:
        """Update only this bit, preserving every other known or unknown bit."""
        await self.coordinator.async_set_notification_type(
            self.subscription_id, self.notification_type, active
        )

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable this notification category."""
        await self._async_set_active(True)

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable this notification category."""
        await self._async_set_active(False)


class FlumeUsageAlertScheduleSwitch(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SwitchEntity
):
    """Control whether a schedule suppresses one usage-alert rule."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        rule_id: str,
        schedule_id: str,
    ) -> None:
        """Initialize a rule schedule switch."""
        super().__init__(coordinator, USAGE_ALERT_SCHEDULE_DESCRIPTION, device)
        self.rule_id = rule_id
        self.schedule_id = schedule_id
        self._attr_unique_id = (
            f"usage_alert_schedule_{self.device_id}_{rule_id}_{schedule_id}"
        )
        self._attr_name = f"{self.rule.name}: mute during {self.schedule.name}"

    @property
    def rule(self) -> UsageAlertRule:
        """Return the latest usage-alert rule."""
        return self.coordinator.data.rules[self.rule_id]

    @property
    def schedule(self) -> UsageAlertSchedule:
        """Return the latest schedule association."""
        return next(
            schedule
            for schedule in self.rule.schedules
            if str(schedule.schedule_id) == self.schedule_id
        )

    @property
    @override
    def available(self) -> bool:
        """Return whether the schedule association still exists."""
        return super().available and any(
            str(schedule.schedule_id) == self.schedule_id
            for rule in self.coordinator.data.rules.values()
            if str(rule.id) == self.rule_id
            for schedule in rule.schedules
        )

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether this schedule currently suppresses the rule."""
        if not self.available:
            return None
        return self.schedule.active

    @property
    @override
    def extra_state_attributes(self) -> dict[str, str]:
        """Expose the schedule description and identifiers."""
        return {
            "description": self.schedule.description,
            "rule_id": self.rule_id,
            "schedule_id": self.schedule_id,
        }

    async def _async_set_active(self, active: bool) -> None:
        """Enable or disable this rule schedule association."""
        await self.hass.async_add_executor_job(
            self.coordinator.client.toggle_usage_alert_schedule,
            self.device_id,
            self.rule_id,
            self.schedule_id,
            active,
        )
        await self.coordinator.async_refresh()

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Suppress the rule during this schedule."""
        await self._async_set_active(True)

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Stop suppressing the rule during this schedule."""
        await self._async_set_active(False)


class FlumeUsageAlertShutoffSwitch(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SwitchEntity
):
    """Control automatic valve shutoff for one usage-alert rule."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
        rule_id: str,
    ) -> None:
        """Initialize an automatic shutoff switch."""
        super().__init__(coordinator, USAGE_ALERT_SHUTOFF_DESCRIPTION, device)
        self.rule_id = rule_id
        self._attr_unique_id = f"usage_alert_shutoff_{self.device_id}_{rule_id}"
        self._attr_name = f"{self.rule.name}: automatic shutoff"

    @property
    def rule(self) -> UsageAlertRule:
        """Return the latest usage-alert rule."""
        return self.coordinator.data.rules[self.rule_id]

    @property
    @override
    def available(self) -> bool:
        """Return whether this rule still has shutoff configuration."""
        return (
            super().available
            and self.rule_id in self.coordinator.data.rules
            and self.rule.shutoff_config is not None
        )

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether automatic shutoff is enabled."""
        if not self.available or self.rule.shutoff_config is None:
            return None
        return self.rule.shutoff_config.active

    async def _async_set_active(self, active: bool) -> None:
        """Set the automatic shutoff state."""
        await self.hass.async_add_executor_job(
            self.coordinator.client.update_rule_shutoff_config,
            self.device_id,
            self.rule_id,
            {"active": active},
        )
        await self.coordinator.async_refresh()

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable automatic shutoff for the rule."""
        await self._async_set_active(True)

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable automatic shutoff for the rule."""
        await self._async_set_active(False)


class FlumeAwayModeSwitch(
    FlumeEntity[FlumePortalDeviceDataUpdateCoordinator], SwitchEntity
):
    """Control Flume away mode for a sensor's location."""

    def __init__(
        self,
        coordinator: FlumePortalDeviceDataUpdateCoordinator,
        device: Device,
    ) -> None:
        """Initialize an away-mode switch."""
        super().__init__(coordinator, AWAY_MODE_DESCRIPTION, device)
        assert self.location.id is not None
        self.location_id = str(self.location.id)
        self._attr_unique_id = f"away_mode_{self.location_id}"

    @property
    def location(self) -> Location:
        """Return the sensor's latest location data."""
        return self.coordinator.data.location

    @property
    @override
    def available(self) -> bool:
        """Return whether location data is available."""
        return super().available and self.location is not None

    @property
    @override
    def is_on(self) -> bool | None:
        """Return whether Flume away mode is enabled."""
        if self.location is None:
            return None
        return self.location.away_mode

    async def _async_set_active(self, active: bool) -> None:
        """Set Flume away mode for this location."""
        await self.hass.async_add_executor_job(
            self.coordinator.client.update_location,
            self.location_id,
            {"away_mode": active},
        )
        await self.coordinator.async_refresh()

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable Flume away mode."""
        await self._async_set_active(True)

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable Flume away mode."""
        await self._async_set_active(False)
