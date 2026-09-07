"""Data update coordinators for the Flume integration."""

import asyncio
from dataclasses import dataclass
from typing import Any, override

from pyflumeng import (
    Budget,
    CurrentFlow,
    Device,
    DoNotAlertSchedule,
    FlumeAuth,
    FlumeClient,
    FlumeData,
    FlumeError,
    FlumePortalAuth,
    Integration,
    Location,
    Notification,
    NotificationPreference,
    Subscription,
    UsageAlertRule,
    UsageBreakdown,
)
from requests import RequestException, Session

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    DEVICE_CONNECTION_SCAN_INTERVAL,
    DEVICE_SCAN_INTERVAL,
    DOMAIN,
    EVENT_NOTIFICATION,
    LOGGER,
    NOTIFICATION_SCAN_INTERVAL,
    PORTAL_CURRENT_FLOW_SCAN_INTERVAL,
    PORTAL_NOTIFICATION_SCAN_INTERVAL,
    PORTAL_RATE_LIMIT,
    PORTAL_SETTINGS_FALLBACK_SCAN_INTERVAL,
    PORTAL_SETTINGS_SCAN_INTERVAL,
    RATE_LIMIT_FAST_POLL_MIN_REMAINING_RATIO,
    USAGE_BREAKDOWN_FALLBACK_SCAN_INTERVAL,
    USAGE_BREAKDOWN_SCAN_INTERVAL,
)
from .util import async_persist_auth_token


def portal_fast_polling_enabled(client: FlumeClient) -> bool:
    """Return whether the latest quota state safely supports fast polling."""
    rate_limit = client.rate_limit
    if rate_limit.limit < PORTAL_RATE_LIMIT:
        return False
    if rate_limit.remaining is None or rate_limit.limit <= 0:
        return True
    return (
        rate_limit.remaining / rate_limit.limit
        >= RATE_LIMIT_FAST_POLL_MIN_REMAINING_RATIO
    )


def usage_breakdown_range(time_zone: str | None) -> tuple[str, str]:
    """Return the current calendar month in the Flume location timezone."""
    zone = dt_util.get_time_zone(time_zone) if time_zone else dt_util.UTC
    now = dt_util.now(zone or dt_util.UTC)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )


@dataclass
class FlumeRuntimeData:
    """Runtime data for the Flume config entry."""

    devices: list[Device]
    auth: FlumeAuth | FlumePortalAuth
    client: FlumeClient
    http_session: Session
    notifications_coordinator: FlumeNotificationDataUpdateCoordinator
    portal_device_coordinators: dict[str, FlumePortalDeviceDataUpdateCoordinator]
    usage_breakdown_coordinators: dict[str, FlumeUsageBreakdownDataUpdateCoordinator]


type FlumeConfigEntry = ConfigEntry[FlumeRuntimeData]


class FlumeDeviceDataUpdateCoordinator(DataUpdateCoordinator[None]):
    """Data update coordinator for an individual flume device."""

    config_entry: FlumeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: FlumeConfigEntry,
        flume_device: FlumeData,
    ) -> None:
        """Initialize the Coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=DOMAIN,
            logger=LOGGER,
            update_interval=DEVICE_SCAN_INTERVAL,
        )

        self.flume_device = flume_device

    @override
    async def _async_update_data(self) -> None:
        """Get the latest data from the Flume."""
        try:
            await self.hass.async_add_executor_job(self.flume_device.update_force)
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex
        async_persist_auth_token(self.hass, self.config_entry)
        LOGGER.debug(
            "Flume Device Data Update values=%s query_payload=%s",
            self.flume_device.values,
            self.flume_device.query_payload,
        )


class FlumeDeviceConnectionUpdateCoordinator(DataUpdateCoordinator[None]):
    """Date update coordinator to read connected status from Devices endpoint."""

    config_entry: FlumeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: FlumeConfigEntry,
        client: FlumeClient,
    ) -> None:
        """Initialize the Coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=DOMAIN,
            logger=LOGGER,
            update_interval=DEVICE_CONNECTION_SCAN_INTERVAL,
        )

        self.client = client
        self.connected: dict[str, bool] = {}
        self.battery_level: dict[str, str] = {}

    def _update_device_status(self) -> None:
        """Update device connectivity and battery level."""
        devices = self.client.list_devices(user=True, location=True)
        self.connected = {
            str(device.id): device.connected
            for device in devices
            if device.id is not None
        }
        # Only sensors report a battery level; bridges omit the key entirely.
        self.battery_level = {
            str(device.id): battery_level
            for device in devices
            if device.id is not None
            and (battery_level := device.battery_level) is not None
        }
        LOGGER.debug(
            "Connectivity %s battery level %s", self.connected, self.battery_level
        )

    @override
    async def _async_update_data(self) -> None:
        """Update the device list."""
        try:
            await self.hass.async_add_executor_job(self._update_device_status)
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex
        async_persist_auth_token(self.hass, self.config_entry)


class FlumeNotificationDataUpdateCoordinator(DataUpdateCoordinator[None]):
    """Data update coordinator for flume notifications."""

    config_entry: FlumeConfigEntry

    def __init__(
        self, hass: HomeAssistant, config_entry: FlumeConfigEntry, client: FlumeClient
    ) -> None:
        """Initialize the Coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=DOMAIN,
            logger=LOGGER,
            update_interval=(
                PORTAL_NOTIFICATION_SCAN_INTERVAL
                if portal_fast_polling_enabled(client)
                else NOTIFICATION_SCAN_INTERVAL
            ),
        )
        self.client = client
        self.active_notifications_by_device: dict[str, set[str]] = {}
        self.notifications: list[Notification] = []
        self.new_notifications: list[Notification] = []
        self.new_notification_payloads: list[dict[str, Any]] = []
        self._seen_notification_ids: set[str] | None = None

    def _fetch_notifications(self) -> list[Notification]:
        """Fetch the latest notifications."""
        return self.client.list_notifications(
            read=None, limit=50, sort_direction="DESC"
        )

    def _update_active_notifications(self) -> None:
        """Update event-rule state used by legacy notification binary sensors."""
        active_notifications_by_device: dict[str, set[str]] = {}
        for notification in self.notifications:
            if notification.device_id is None or notification.extra is None:
                continue
            rule = notification.extra.event_rule_name
            if not rule:
                continue
            device_id = str(notification.device_id)
            active_notifications_by_device.setdefault(device_id, set()).add(rule)

        self.active_notifications_by_device = active_notifications_by_device

    def _notification_payload(self, notification: Notification) -> dict[str, Any]:
        """Build a notification event payload with useful query-derived fields."""
        payload = notification.to_dict()
        extra = notification.extra
        if extra is None:
            return payload

        if extra.event_rule_name:
            payload["event_rule_name"] = extra.event_rule_name
        payload["advanced_low_flow"] = extra.advanced_low_flow
        if extra.budget_start is not None:
            payload["budget_start"] = extra.budget_start
        if extra.budget_type is not None:
            payload["budget_type"] = extra.budget_type
        if extra.percentage is not None:
            payload["percentage"] = extra.percentage

        query = extra.query
        if query is None:
            return payload
        payload.update(
            {
                "since_datetime": query.since_datetime,
                "until_datetime": query.until_datetime,
                "query_timezone": query.tz,
                "query_bucket": query.bucket,
            }
        )
        duration_seconds = query.duration_seconds
        if duration_seconds is not None:
            payload["duration_seconds"] = duration_seconds
            payload["duration_minutes"] = duration_seconds / 60

        try:
            usage_detail = self.client.get_notification_usage_detail(notification)
        except (FlumeError, RequestException):
            LOGGER.debug(
                "Unable to enrich Flume notification %s with usage query data",
                notification.id,
                exc_info=True,
            )
            return payload
        if usage_detail is not None and usage_detail.average_gpm is not None:
            payload["average_gpm"] = usage_detail.average_gpm
            payload["flow_rate_gpm"] = usage_detail.average_gpm
            if usage_detail.duration_minutes is not None:
                payload["usage_gallons"] = (
                    usage_detail.average_gpm * usage_detail.duration_minutes
                )
        return payload

    @override
    async def _async_update_data(self) -> None:
        """Update data."""
        LOGGER.debug("Updating Flume Notification")
        try:
            notifications = await self.hass.async_add_executor_job(
                self._fetch_notifications
            )
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex

        current_by_id = {
            str(notification.id): notification
            for notification in notifications
            if notification.id is not None
        }
        if self._seen_notification_ids is None:
            self.new_notifications = []
            self.new_notification_payloads = []
            self._seen_notification_ids = set(current_by_id)
        else:
            self.new_notifications = [
                notification
                for notification_id, notification in current_by_id.items()
                if notification_id not in self._seen_notification_ids
            ]
            self._seen_notification_ids.update(current_by_id)
            self.new_notification_payloads = await self.hass.async_add_executor_job(
                lambda: [
                    self._notification_payload(notification)
                    for notification in self.new_notifications
                ]
            )

        self.notifications = notifications
        self._update_active_notifications()
        LOGGER.debug("Notifications %s", self.notifications)
        for payload in self.new_notification_payloads:
            self.hass.bus.async_fire(
                EVENT_NOTIFICATION,
                {
                    "config_entry_id": self.config_entry.entry_id,
                    **payload,
                },
            )
        self.update_interval = (
            PORTAL_NOTIFICATION_SCAN_INTERVAL
            if portal_fast_polling_enabled(self.client)
            else NOTIFICATION_SCAN_INTERVAL
        )
        async_persist_auth_token(self.hass, self.config_entry)


class FlumeCurrentFlowDataUpdateCoordinator(DataUpdateCoordinator[CurrentFlow | None]):
    """Update coordinator for a sensor's live current-flow endpoint."""

    config_entry: FlumeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: FlumeConfigEntry,
        client: FlumeClient,
        device_id: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=f"{DOMAIN}_current_flow_{device_id}",
            logger=LOGGER,
            update_interval=(
                PORTAL_CURRENT_FLOW_SCAN_INTERVAL
                if portal_fast_polling_enabled(client)
                else DEVICE_SCAN_INTERVAL
            ),
        )
        self.client = client
        self.device_id = device_id

    @override
    async def _async_update_data(self) -> CurrentFlow | None:
        """Fetch the current flow reading."""
        try:
            current_flow = await self.hass.async_add_executor_job(
                self.client.get_current_flow, self.device_id
            )
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex
        self.update_interval = (
            PORTAL_CURRENT_FLOW_SCAN_INTERVAL
            if portal_fast_polling_enabled(self.client)
            else DEVICE_SCAN_INTERVAL
        )
        async_persist_auth_token(self.hass, self.config_entry)
        return current_flow


@dataclass(frozen=True)
class FlumePortalDeviceData:
    """Settings and controls associated with one Flume water sensor."""

    rules: dict[str, UsageAlertRule]
    schedules: dict[str, DoNotAlertSchedule]
    budgets: dict[str, Budget]
    integrations: dict[str, Integration]
    location: Location | None
    subscriptions: dict[str, Subscription]


class FlumePortalDeviceDataUpdateCoordinator(
    DataUpdateCoordinator[FlumePortalDeviceData]
):
    """Update portal settings and controls for one water sensor."""

    config_entry: FlumeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: FlumeConfigEntry,
        client: FlumeClient,
        device_id: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=f"{DOMAIN}_portal_settings_{device_id}",
            logger=LOGGER,
            update_interval=(
                PORTAL_SETTINGS_SCAN_INTERVAL
                if portal_fast_polling_enabled(client)
                else PORTAL_SETTINGS_FALLBACK_SCAN_INTERVAL
            ),
        )
        self.client = client
        self.device_id = device_id
        self._subscription_lock = asyncio.Lock()

    def _update_data(self) -> FlumePortalDeviceData:
        """Fetch all settings that map cleanly to Home Assistant entities."""
        rules = self.client.list_usage_alert_rules(self.device_id)
        schedules = self.client.list_do_not_alert_schedules(self.device_id)
        budgets = self.client.list_budgets(self.device_id)
        integrations = self.client.list_shutoff_integrations(self.device_id)
        device = next(
            (
                device
                for device in self.config_entry.runtime_data.devices
                if device.id is not None and str(device.id) == self.device_id
            ),
            None,
        )
        location = device.location if device is not None else None
        subscriptions: list[Subscription] = []
        if location is not None and location.id is not None:
            location = self.client.get_location(str(location.id)) or location
            subscriptions = self.client.list_notification_subscriptions(
                location_id=str(location.id)
            )
        return FlumePortalDeviceData(
            rules={str(item.id): item for item in rules if item.id is not None},
            schedules={str(item.id): item for item in schedules if item.id is not None},
            budgets={str(item.id): item for item in budgets if item.id is not None},
            integrations={
                str(item.id): item for item in integrations if item.id is not None
            },
            location=location,
            subscriptions={
                str(subscription.id): subscription
                for subscription in subscriptions
                if subscription.id is not None and not subscription.emergency_contact
            },
        )

    async def async_set_notification_type(
        self,
        subscription_id: str,
        notification_type: NotificationPreference,
        active: bool,
    ) -> None:
        """Set one known notification bit without disturbing any other bits."""
        async with self._subscription_lock:
            subscription = self.data.subscriptions.get(subscription_id)
            if subscription is None or subscription.id is None:
                raise ValueError("Flume notification subscription is no longer available")
            await self.hass.async_add_executor_job(
                self.client.set_subscription_notification_preference,
                subscription.id,
                notification_type,
                active,
            )
            await self.async_refresh()

    @override
    async def _async_update_data(self) -> FlumePortalDeviceData:
        """Fetch the latest portal settings."""
        try:
            data = await self.hass.async_add_executor_job(self._update_data)
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex
        self.update_interval = (
            PORTAL_SETTINGS_SCAN_INTERVAL
            if portal_fast_polling_enabled(self.client)
            else PORTAL_SETTINGS_FALLBACK_SCAN_INTERVAL
        )
        async_persist_auth_token(self.hass, self.config_entry)
        return data


class FlumeUsageBreakdownDataUpdateCoordinator(DataUpdateCoordinator[UsageBreakdown]):
    """Update the portal-style current-month usage breakdown for one sensor."""

    config_entry: FlumeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: FlumeConfigEntry,
        client: FlumeClient,
        device_id: str,
        location_id: str | None,
        time_zone: str | None,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            config_entry=config_entry,
            name=f"{DOMAIN}_usage_breakdown_{device_id}",
            logger=LOGGER,
            update_interval=(
                USAGE_BREAKDOWN_SCAN_INTERVAL
                if portal_fast_polling_enabled(client)
                else USAGE_BREAKDOWN_FALLBACK_SCAN_INTERVAL
            ),
        )
        self.client = client
        self.device_id = device_id
        self.location_id = location_id
        self.time_zone = time_zone

    def _update_data(self) -> UsageBreakdown:
        """Fetch the current-month category totals."""
        since_datetime, until_datetime = usage_breakdown_range(self.time_zone)
        return self.client.get_usage_breakdown(
            self.device_id,
            since_datetime,
            until_datetime,
            location_id=self.location_id,
        )

    @override
    async def _async_update_data(self) -> UsageBreakdown:
        """Fetch the latest usage breakdown."""
        try:
            data = await self.hass.async_add_executor_job(self._update_data)
        except Exception as ex:
            raise UpdateFailed(f"Error communicating with flume API: {ex}") from ex
        self.update_interval = (
            USAGE_BREAKDOWN_SCAN_INTERVAL
            if portal_fast_polling_enabled(self.client)
            else USAGE_BREAKDOWN_FALLBACK_SCAN_INTERVAL
        )
        async_persist_auth_token(self.hass, self.config_entry)
        return data


def get_portal_device_coordinator(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    device_id: str,
) -> FlumePortalDeviceDataUpdateCoordinator:
    """Return the shared portal coordinator for one water sensor."""
    runtime_data = config_entry.runtime_data
    if coordinator := runtime_data.portal_device_coordinators.get(device_id):
        return coordinator
    coordinator = FlumePortalDeviceDataUpdateCoordinator(
        hass,
        config_entry,
        runtime_data.client,
        device_id,
    )
    runtime_data.portal_device_coordinators[device_id] = coordinator
    return coordinator


def get_usage_breakdown_coordinator(
    hass: HomeAssistant,
    config_entry: FlumeConfigEntry,
    device: Device,
) -> FlumeUsageBreakdownDataUpdateCoordinator:
    """Return the shared usage-breakdown coordinator for one water sensor."""
    assert device.id is not None
    device_id = str(device.id)
    runtime_data = config_entry.runtime_data
    if coordinator := runtime_data.usage_breakdown_coordinators.get(device_id):
        return coordinator
    location_id = (
        str(device.location.id)
        if device.location is not None and device.location.id is not None
        else None
    )
    time_zone = device.location.tz if device.location is not None else None
    coordinator = FlumeUsageBreakdownDataUpdateCoordinator(
        hass,
        config_entry,
        runtime_data.client,
        device_id,
        location_id,
        time_zone,
    )
    runtime_data.usage_breakdown_coordinators[device_id] = coordinator
    return coordinator
