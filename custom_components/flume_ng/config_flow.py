"""Config flow for the Flume integration."""

from collections.abc import Mapping
from dataclasses import dataclass
import logging
from typing import Any, override

from pyflumeng import FlumeAuth, FlumeClient, FlumeHTTPError, FlumePortalAuth
from requests import Session
from requests.exceptions import RequestException
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_PASSWORD,
    CONF_USERNAME,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    AUTH_TYPE_PERSONAL,
    AUTH_TYPE_PORTAL,
    CONF_AUTH_TYPE,
    CONF_TOKEN,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

AUTH_TYPE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_AUTH_TYPE, default=AUTH_TYPE_PORTAL): vol.In(
            {
                AUTH_TYPE_PORTAL: "Flume portal (recommended)",
                AUTH_TYPE_PERSONAL: "Personal API",
            }
        )
    }
)

PORTAL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)

PERSONAL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
        vol.Required(CONF_CLIENT_ID): str,
        vol.Required(CONF_CLIENT_SECRET): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


@dataclass(frozen=True)
class ValidationResult:
    """Validated Flume account information."""

    title: str
    token: dict[str, Any] | None


def _validate_input(data: dict[str, Any]) -> ValidationResult:
    """Validate credentials and fetch devices in an executor thread."""
    http_session = Session()
    try:
        if data[CONF_AUTH_TYPE] == AUTH_TYPE_PORTAL:
            auth = FlumePortalAuth(
                data[CONF_USERNAME],
                data[CONF_PASSWORD],
                http_session=http_session,
            )
        else:
            auth = FlumeAuth(
                data[CONF_USERNAME],
                data[CONF_PASSWORD],
                data[CONF_CLIENT_ID],
                data[CONF_CLIENT_SECRET],
                http_session=http_session,
            )
        client = FlumeClient(auth, http_session=http_session)
        devices = client.list_devices(user=True, location=True)
        if not devices:
            raise CannotConnect
        token = dict(auth.token) if auth.token is not None else None
        return ValidationResult(data[CONF_USERNAME], token)
    finally:
        http_session.close()


async def validate_input(hass: HomeAssistant, data: dict[str, Any]) -> ValidationResult:
    """Validate that the user input can connect to Flume."""
    try:
        return await hass.async_add_executor_job(_validate_input, data)
    except CannotConnect:
        raise
    except RequestException as err:
        raise CannotConnect from err
    except FlumeHTTPError as err:
        if err.status_code in (401, 403):
            raise InvalidAuth from err
        raise CannotConnect from err
    except Exception as err:
        _LOGGER.debug("Flume connection validation failed", exc_info=True)
        raise CannotConnect from err


class FlumeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Flume."""

    VERSION = 2

    def __init__(self) -> None:
        """Initialize the flow."""
        self._reauth_unique_id: str | None = None

    @override
    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Choose the authentication method."""
        if user_input is not None:
            if user_input[CONF_AUTH_TYPE] == AUTH_TYPE_PERSONAL:
                return await self.async_step_personal()
            return await self.async_step_portal()
        return self.async_show_form(step_id="user", data_schema=AUTH_TYPE_SCHEMA)

    async def async_step_portal(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Configure customer-portal authentication."""
        return await self._async_step_credentials(
            step_id="portal",
            data_schema=PORTAL_SCHEMA,
            user_input=user_input,
            auth_type=AUTH_TYPE_PORTAL,
        )

    async def async_step_personal(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Configure Personal API authentication."""
        return await self._async_step_credentials(
            step_id="personal",
            data_schema=PERSONAL_SCHEMA,
            user_input=user_input,
            auth_type=AUTH_TYPE_PERSONAL,
        )

    async def _async_step_credentials(
        self,
        *,
        step_id: str,
        data_schema: vol.Schema,
        user_input: dict[str, Any] | None,
        auth_type: str,
    ) -> ConfigFlowResult:
        """Validate one of the supported authentication methods."""
        errors: dict[str, str] = {}
        if user_input is not None:
            data = {**user_input, CONF_AUTH_TYPE: auth_type}
            await self.async_set_unique_id(data[CONF_USERNAME])
            self._abort_if_unique_id_configured()
            try:
                info = await validate_input(self.hass, data)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except InvalidAuth:
                errors[CONF_PASSWORD] = "invalid_auth"
            else:
                entry_data = data
                if info.token is not None:
                    entry_data = {**data, CONF_TOKEN: info.token}
                return self.async_create_entry(title=info.title, data=entry_data)

        return self.async_show_form(
            step_id=step_id,
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "api_url": "https://portal.flumetech.com/settings#token"
            },
        )

    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        """Handle reauthentication."""
        self._reauth_unique_id = self.context["unique_id"]
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Update credentials for an existing entry."""
        errors: dict[str, str] = {}
        existing_entry = await self.async_set_unique_id(self._reauth_unique_id)
        assert existing_entry
        auth_type = existing_entry.data.get(CONF_AUTH_TYPE, AUTH_TYPE_PERSONAL)

        if auth_type == AUTH_TYPE_PERSONAL:
            schema = vol.Schema(
                {
                    vol.Required(CONF_PASSWORD): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    ),
                    vol.Required(
                        CONF_CLIENT_ID,
                        default=existing_entry.data.get(CONF_CLIENT_ID, ""),
                    ): str,
                    vol.Required(CONF_CLIENT_SECRET): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    ),
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Required(CONF_PASSWORD): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    )
                }
            )

        if user_input is not None:
            new_data = {
                **existing_entry.data,
                **user_input,
                CONF_AUTH_TYPE: auth_type,
            }
            new_data.pop(CONF_TOKEN, None)
            try:
                info = await validate_input(self.hass, new_data)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except InvalidAuth:
                errors[CONF_PASSWORD] = "invalid_auth"
            else:
                if info.token is not None:
                    new_data[CONF_TOKEN] = info.token
                self.hass.config_entries.async_update_entry(
                    existing_entry, data=new_data
                )
                await self.hass.config_entries.async_reload(existing_entry.entry_id)
                return self.async_abort(reason="reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            description_placeholders={
                CONF_USERNAME: existing_entry.data[CONF_USERNAME]
            },
            data_schema=schema,
            errors=errors,
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate that Flume cannot be reached."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate invalid Flume authentication."""
