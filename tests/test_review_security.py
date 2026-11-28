"""Regression tests for credential forms and sensitive subscription actions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import probatio
import pytest
from homeassistant.const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_PASSWORD,
    CONF_USERNAME,
)
from homeassistant.core import Context, HomeAssistant
from homeassistant.exceptions import Unauthorized
from homeassistant.helpers import config_validation as cv
from pyflumeng import Subscription

from custom_components.flume_ng import config_flow, services
from custom_components.flume_ng.const import (
    AUTH_TYPE_PERSONAL,
    AUTH_TYPE_PORTAL,
    CONF_AUTH_TYPE,
    DOMAIN,
)


def _fields(schema):
    return {
        field["name"]: field
        for field in probatio.to_field_list(
            schema, custom_serializer=cv.custom_serializer
        )
    }


def _assert_password(field):
    assert field["selector"]["text"]["type"] == "password"
    assert field["required"] is True
    assert "default" not in field
    assert "suggested_value" not in field


@pytest.mark.parametrize(
    ("schema", "secrets"),
    [
        (config_flow.PORTAL_SCHEMA, (CONF_PASSWORD,)),
        (config_flow.PERSONAL_SCHEMA, (CONF_PASSWORD, CONF_CLIENT_SECRET)),
    ],
)
def test_initial_credentials(schema, secrets):
    fields = _fields(schema)
    for key in secrets:
        _assert_password(fields[key])
    data = {CONF_USERNAME: "test@example.invalid", CONF_PASSWORD: "password"}
    if CONF_CLIENT_SECRET in secrets:
        data.update({CONF_CLIENT_ID: "client", CONF_CLIENT_SECRET: "secret"})
    assert schema(data) == data


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_type", [AUTH_TYPE_PORTAL, AUTH_TYPE_PERSONAL, None])
async def test_reauth_credentials(auth_type):
    data = {CONF_USERNAME: "test@example.invalid", CONF_PASSWORD: "old-password"}
    if auth_type is not None:
        data[CONF_AUTH_TYPE] = auth_type
    if auth_type != AUTH_TYPE_PORTAL:
        data.update({CONF_CLIENT_ID: "client", CONF_CLIENT_SECRET: "old-secret"})
    flow = config_flow.FlumeConfigFlow()
    flow.flow_id = "test-flow"
    flow.handler = DOMAIN
    flow.context = {"source": "reauth", "unique_id": data[CONF_USERNAME]}
    with patch.object(
        flow, "async_set_unique_id", AsyncMock(return_value=SimpleNamespace(data=data))
    ):
        result = await flow.async_step_reauth(data)
    fields = _fields(result["data_schema"])
    _assert_password(fields[CONF_PASSWORD])
    submitted = {CONF_PASSWORD: "new-password"}
    if auth_type != AUTH_TYPE_PORTAL:
        _assert_password(fields[CONF_CLIENT_SECRET])
        assert fields[CONF_CLIENT_ID]["default"] == "client"
        submitted[CONF_CLIENT_SECRET] = "new-secret"
        submitted[CONF_CLIENT_ID] = "client"
    else:
        assert CONF_CLIENT_SECRET not in fields
    assert result["data_schema"](submitted) == submitted
    assert "old-secret" not in repr(fields)
    assert "old-password" not in repr(fields)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action",
    [services.SERVICE_LIST_EMERGENCY_CONTACTS, services.SERVICE_LIST_SUBSCRIPTIONS],
)
async def test_subscription_actions_require_admin(tmp_path, action):
    hass = HomeAssistant(str(tmp_path))
    hass.auth = SimpleNamespace(
        async_get_user=AsyncMock(return_value=SimpleNamespace(is_admin=False))
    )
    services.async_setup_services(hass)
    call = {services.CONF_CONFIG_ENTRY: "test-entry"}
    entry = SimpleNamespace(
        runtime_data=SimpleNamespace(
            auth=SimpleNamespace(capabilities={"portal_api"}),
            client=SimpleNamespace(rate_limit=SimpleNamespace(to_dict=dict)),
        )
    )
    contact = Subscription(
        {"id": "1", "emergency_contact": True, "contact_name": "Example contact"}
    )
    with (
        patch.object(services, "_entry_from_call", return_value=entry) as lookup,
        patch.object(
            services, "_async_list_subscriptions", AsyncMock(return_value=[contact])
        ) as fetch,
    ):
        with pytest.raises(Unauthorized):
            await hass.services.async_call(
                DOMAIN,
                action,
                call,
                blocking=True,
                return_response=True,
                context=Context(user_id="non-admin"),
            )
        lookup.assert_not_called()
        fetch.assert_not_awaited()
        hass.auth.async_get_user.return_value.is_admin = True
        response = await hass.services.async_call(
            DOMAIN,
            action,
            call,
            blocking=True,
            return_response=True,
            context=Context(user_id="admin"),
        )
        key = (
            "contacts"
            if action == services.SERVICE_LIST_EMERGENCY_CONTACTS
            else "subscriptions"
        )
        assert response[key][0]["contact_name"] == "Example contact"
        fetch.assert_awaited_once()
