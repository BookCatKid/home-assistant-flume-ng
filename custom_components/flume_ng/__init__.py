"""The flume integration."""

from collections.abc import Mapping

from pyflumeng import Device, FlumeAuth, FlumeClient, FlumeHTTPError, FlumePortalAuth
from requests import Session
from requests.exceptions import RequestException

from homeassistant.const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_PASSWORD,
    CONF_USERNAME,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .const import (
    AUTH_TYPE_PERSONAL,
    AUTH_TYPE_PORTAL,
    CONF_AUTH_TYPE,
    CONF_TOKEN,
    DOMAIN,
    FLUME_TYPE_SENSOR,
    PLATFORMS,
)
from .coordinator import (
    FlumeConfigEntry,
    FlumeNotificationDataUpdateCoordinator,
    FlumeRuntimeData,
    get_portal_device_coordinator,
)
from .services import async_setup_services
from .util import async_persist_auth_token

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Flume."""
    async_setup_services(hass)
    return True


def _setup_entry(
    entry: FlumeConfigEntry,
) -> tuple[FlumeAuth | FlumePortalAuth, FlumeClient, list[Device], Session]:
    """Config entry set up in executor."""
    config = entry.data

    username = config[CONF_USERNAME]
    password = config[CONF_PASSWORD]
    http_session = Session()
    token = config.get(CONF_TOKEN)
    flume_token = token if isinstance(token, Mapping) else None

    try:
        if config.get(CONF_AUTH_TYPE, AUTH_TYPE_PERSONAL) == AUTH_TYPE_PORTAL:
            flume_auth = FlumePortalAuth(
                username,
                password,
                flume_token=flume_token,
                http_session=http_session,
            )
        else:
            flume_auth = FlumeAuth(
                username,
                password,
                config[CONF_CLIENT_ID],
                config[CONF_CLIENT_SECRET],
                flume_token=flume_token,
                http_session=http_session,
            )
        client = FlumeClient(flume_auth, http_session=http_session)
        flume_devices = client.list_devices(user=True, location=True)
    except RequestException as ex:
        http_session.close()
        raise ConfigEntryNotReady from ex
    except FlumeHTTPError as ex:
        http_session.close()
        if ex.status_code in (401, 403):
            raise ConfigEntryAuthFailed from ex
        raise ConfigEntryNotReady from ex
    except Exception as ex:
        http_session.close()
        raise ConfigEntryNotReady from ex

    return flume_auth, client, flume_devices, http_session


async def async_migrate_entry(hass: HomeAssistant, entry: FlumeConfigEntry) -> bool:
    """Migrate legacy Personal API entries to the PyFlumeNG config format."""
    if entry.version == 1:
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_AUTH_TYPE: AUTH_TYPE_PERSONAL},
            version=2,
        )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: FlumeConfigEntry) -> bool:
    """Set up flume from a config entry."""

    flume_auth, client, flume_devices, http_session = await hass.async_add_executor_job(
        _setup_entry, entry
    )
    notification_coordinator = FlumeNotificationDataUpdateCoordinator(
        hass=hass, config_entry=entry, client=client
    )

    entry.runtime_data = FlumeRuntimeData(
        devices=flume_devices,
        auth=flume_auth,
        client=client,
        http_session=http_session,
        notifications_coordinator=notification_coordinator,
        portal_device_coordinators={},
        usage_breakdown_coordinators={},
    )
    if "portal_api" in flume_auth.capabilities:
        for device in flume_devices:
            if device.id is None or device.type != FLUME_TYPE_SENSOR:
                continue
            coordinator = get_portal_device_coordinator(hass, entry, str(device.id))
            await coordinator.async_config_entry_first_refresh()
    async_persist_auth_token(hass, entry)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: FlumeConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        entry.runtime_data.http_session.close()
    return unload_ok
