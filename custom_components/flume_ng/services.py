"""Services for Flume."""

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from functools import partial
import inspect
import re
from typing import cast

from pyflumeng import (
    Device,
    DoNotAlertSchedule,
    FlumeCapabilityError,
    FlumeClient,
    FlumeHTTPError,
    FlumeModel,
    NotificationPreference,
    Subscription,
    UsageAlertRule,
)
import voluptuous as vol

from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
    callback,
)
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import device_registry as dr, service
from homeassistant.helpers.selector import ConfigEntrySelector, DeviceSelector
from homeassistant.util.json import JsonObjectType, JsonValueType

from .const import DOMAIN, FLUME_TYPE_SENSOR
from .coordinator import (
    FlumeConfigEntry,
    get_portal_device_coordinator,
    usage_breakdown_range,
)
from .util import async_persist_auth_token

SERVICE_LIST_NOTIFICATIONS = "list_notifications"
SERVICE_LIST_USAGE_ALERT_RULES = "list_usage_alert_rules"
SERVICE_CREATE_USAGE_ALERT_RULE = "create_usage_alert_rule"
SERVICE_UPDATE_USAGE_ALERT_RULE = "update_usage_alert_rule"
SERVICE_DELETE_USAGE_ALERT_RULE = "delete_usage_alert_rule"
SERVICE_SET_USAGE_ALERT_RULE_ACTIVE = "set_usage_alert_rule_active"
SERVICE_LIST_DO_NOT_ALERT_SCHEDULES = "list_do_not_alert_schedules"
SERVICE_CREATE_DO_NOT_ALERT_SCHEDULE = "create_do_not_alert_schedule"
SERVICE_UPDATE_DO_NOT_ALERT_SCHEDULE = "update_do_not_alert_schedule"
SERVICE_DELETE_DO_NOT_ALERT_SCHEDULE = "delete_do_not_alert_schedule"
SERVICE_GET_USAGE_BREAKDOWN = "get_usage_breakdown"
SERVICE_LIST_BUDGETS = "list_budgets"
SERVICE_CREATE_BUDGET = "create_budget"
SERVICE_UPDATE_BUDGET = "update_budget"
SERVICE_DELETE_BUDGET = "delete_budget"
SERVICE_GET_NOTIFICATION_PREFERENCES = "get_notification_preferences"
SERVICE_UPDATE_NOTIFICATION_PREFERENCES = "update_notification_preferences"
SERVICE_LIST_USAGE_ALERT_HISTORY = "list_usage_alert_history"
SERVICE_LIST_USAGE_SPANS = "list_usage_spans"
SERVICE_RELABEL_SPAN = "relabel_span"
SERVICE_LIST_LOCATION_ACCESS = "list_location_access"
SERVICE_GRANT_LOCATION_ACCESS = "grant_location_access"
SERVICE_REVOKE_LOCATION_ACCESS = "revoke_location_access"
SERVICE_LIST_SUBSCRIPTIONS = "list_subscriptions"
SERVICE_LIST_EMERGENCY_CONTACTS = "list_emergency_contacts"
SERVICE_CREATE_EMERGENCY_CONTACT = "create_emergency_contact"
SERVICE_UPDATE_EMERGENCY_CONTACT = "update_emergency_contact"
SERVICE_DELETE_EMERGENCY_CONTACT = "delete_emergency_contact"
SERVICE_GET_METER_ACCURACY = "get_meter_accuracy"
SERVICE_CHECK_METER_ACCURACY = "check_meter_accuracy"
SERVICE_GET_LOCATION_DIAGNOSTICS = "get_location_diagnostics"
SERVICE_SET_NOTIFICATION_READ = "set_notification_read"
SERVICE_DELETE_NOTIFICATION = "delete_notification"
SERVICE_LIST_API_METHODS = "list_api_methods"
SERVICE_CALL_API = "call_api"

CONF_CONFIG_ENTRY = "config_entry"
CONF_DEVICE = "device"
CONF_RULE_ID = "rule_id"
CONF_SCHEDULE_ID = "schedule_id"
CONF_NOTIFICATION_ID = "notification_id"
CONF_READ = "read"
CONF_ACTIVE = "active"
CONF_PAYLOAD = "payload"
CONF_NAME = "name"
CONF_DESCRIPTION = "description"
CONF_FLOW_RATE = "flow_rate"
CONF_DURATION = "duration"
CONF_NOTIFY_EVERY = "notify_every"
CONF_START_TIME = "start_time"
CONF_END_TIME = "end_time"
CONF_WEEKDAYS = "weekdays"
CONF_INTERVAL = "interval"
CONF_SPAN_TYPES = "span_types"
CONF_BUDGET_ID = "budget_id"
CONF_BUDGET_TYPE = "type"
CONF_VALUE = "value"
CONF_THRESHOLDS = "thresholds"
CONF_THRESHOLD_PERCENTAGES = "threshold_percentages"
CONF_RECUR_MULTIPLIER = "recur_multiplier"
CONF_SUBSCRIPTION_ID = "subscription_id"
CONF_PREFERENCE = "preference"
CONF_ENABLED = "enabled"
CONF_CONTACT_NAME = "contact_name"
CONF_ACCESS_ID = "access_id"
CONF_EMAIL_ADDRESS = "email_address"
CONF_SPAN_ID = "span_id"
CONF_SPAN_TYPE = "span_type"
CONF_SINCE_DATETIME = "since_datetime"
CONF_UNTIL_DATETIME = "until_datetime"
CONF_METHOD = "method"
CONF_ARGUMENTS = "arguments"

_CLIENT_INTERNAL_METHODS = frozenset(
    {
        "request",
        "data",
        "response",
        "data_one",
        "list_all",
        "iter_pages",
        "raw_response",
    }
)

# PyFlumeNG is pinned in the manifest. Keep the complete public method set for
# classification, but generic ``call_api`` deliberately exposes only harmless
# reads. Mutations belong behind named Home Assistant actions where inputs can
# be validated and dangerous/credential-bearing operations can stay hidden.
PUBLIC_CLIENT_METHODS = frozenset(
    name
    for name, value in vars(FlumeClient).items()
    if callable(value)
    and not name.startswith("_")
    and name not in _CLIENT_INTERNAL_METHODS
)

_GENERIC_EXPLICITLY_BLOCKED_READS = frozenset(
    {
        # API-client records may contain generated client credentials.
        "list_clients",
        "list_portal_clients",
        # Subscription alert_info may contain APNS/FCM device tokens or an
        # email address. Friendly subscription actions redact it intentionally.
        "get_subscription",
        "get_portal_subscription",
        "list_emergency_contacts",
        "list_notification_subscriptions",
        "list_subscriptions",
        "list_portal_subscriptions",
        # Commerce/support catalog endpoints are outside HA's automation use
        # case and should not be reachable through an escape-hatch action.
        "get_contact_info",
        "get_purchase_options",
        "list_insurers",
        "list_portal_insurers",
        "list_pro_services",
        "list_purchase_options",
    }
)

GENERIC_READ_METHODS = frozenset(
    method_name
    for method_name in PUBLIC_CLIENT_METHODS
    if method_name.startswith(("get_", "list_"))
    and method_name not in _GENERIC_EXPLICITLY_BLOCKED_READS
)
GENERIC_BLOCKED_METHODS = PUBLIC_CLIENT_METHODS - GENERIC_READ_METHODS

# Backwards-compatible module name used by tests/integrators that imported the
# old constant. It now means "safe through call_api", not every client method.
CLIENT_METHODS = GENERIC_READ_METHODS

_SENSITIVE_KEY_PARTS = frozenset(
    {
        "accesstoken",
        "alertinfo",
        "apikey",
        "authorization",
        "bearer",
        "clientsecret",
        "credential",
        "jwt",
        "password",
        "refreshtoken",
        "secret",
        "token",
    }
)
_SENSITIVE_EXACT_KEYS = frozenset({"sinceurl", "untilurl"})
_REDACTED = "***REDACTED***"

_WEEKDAYS = ("MO", "TU", "WE", "TH", "FR", "SA", "SU")
_TIME_RE = re.compile(
    r"^(?P<hour>[01]\d|2[0-3]):(?P<minute>[0-5]\d)(?::(?P<second>[0-5]\d))?$"
)

_USAGE_ALERT_TYPED_FIELDS = (
    CONF_NAME,
    CONF_ACTIVE,
    CONF_FLOW_RATE,
    CONF_DURATION,
    CONF_NOTIFY_EVERY,
)
_BUDGET_TYPED_FIELDS = (
    CONF_BUDGET_TYPE,
    CONF_VALUE,
    CONF_THRESHOLDS,
    CONF_RECUR_MULTIPLIER,
)
BASE_SERVICE_SCHEMA = {
    vol.Required(CONF_CONFIG_ENTRY): ConfigEntrySelector({"integration": DOMAIN})
}
LIST_NOTIFICATIONS_SERVICE_SCHEMA = vol.Schema(BASE_SERVICE_SCHEMA)
LIST_USAGE_ALERT_RULES_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Optional(CONF_DEVICE): DeviceSelector({"integration": DOMAIN}),
    }
)
DEVICE_SERVICE_SCHEMA = {
    vol.Required(CONF_DEVICE): DeviceSelector({"integration": DOMAIN}),
}
CREATE_USAGE_ALERT_RULE_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_NAME): str,
        vol.Optional(CONF_ACTIVE): bool,
        vol.Optional(CONF_FLOW_RATE): vol.Coerce(float),
        vol.Optional(CONF_DURATION): vol.Coerce(int),
        vol.Optional(CONF_NOTIFY_EVERY): vol.Coerce(int),
    }
)
UPDATE_USAGE_ALERT_RULE_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_RULE_ID): str,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_NAME): str,
        vol.Optional(CONF_ACTIVE): bool,
        vol.Optional(CONF_FLOW_RATE): vol.Coerce(float),
        vol.Optional(CONF_DURATION): vol.Coerce(int),
        vol.Optional(CONF_NOTIFY_EVERY): vol.Coerce(int),
    }
)
DELETE_USAGE_ALERT_RULE_SERVICE_SCHEMA = vol.Schema(
    {**DEVICE_SERVICE_SCHEMA, vol.Required(CONF_RULE_ID): str}
)
SET_USAGE_ALERT_RULE_ACTIVE_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_RULE_ID): str,
        vol.Required(CONF_ACTIVE): bool,
    }
)
LIST_DO_NOT_ALERT_SCHEDULES_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
CREATE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_NAME): str,
        vol.Optional(CONF_DESCRIPTION): str,
        vol.Optional(CONF_START_TIME): str,
        vol.Optional(CONF_END_TIME): str,
        vol.Optional(CONF_WEEKDAYS): [vol.In(_WEEKDAYS)],
        vol.Optional(CONF_INTERVAL): vol.Coerce(int),
        vol.Optional(CONF_SPAN_TYPES): [str],
    }
)
UPDATE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_SCHEDULE_ID): str,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_NAME): str,
        vol.Optional(CONF_DESCRIPTION): str,
        vol.Optional(CONF_START_TIME): str,
        vol.Optional(CONF_END_TIME): str,
        vol.Optional(CONF_WEEKDAYS): [vol.In(_WEEKDAYS)],
        vol.Optional(CONF_INTERVAL): vol.Coerce(int),
        vol.Optional(CONF_SPAN_TYPES): [str],
    }
)
DELETE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA = vol.Schema(
    {**DEVICE_SERVICE_SCHEMA, vol.Required(CONF_SCHEDULE_ID): str}
)
GET_USAGE_BREAKDOWN_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Optional(CONF_SINCE_DATETIME): str,
        vol.Optional(CONF_UNTIL_DATETIME): str,
    }
)
LIST_BUDGETS_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
CREATE_BUDGET_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_BUDGET_TYPE): vol.In(("DAILY", "WEEKLY", "MONTHLY")),
        vol.Optional(CONF_VALUE): vol.Coerce(float),
        vol.Optional(CONF_THRESHOLDS): [vol.Coerce(int)],
        vol.Optional(CONF_THRESHOLD_PERCENTAGES): [vol.Coerce(float)],
        vol.Optional(CONF_RECUR_MULTIPLIER): vol.Coerce(int),
    }
)
UPDATE_BUDGET_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_BUDGET_ID): str,
        vol.Optional(CONF_PAYLOAD): dict,
        vol.Optional(CONF_VALUE): vol.Coerce(float),
        vol.Optional(CONF_THRESHOLDS): [vol.Coerce(int)],
        vol.Optional(CONF_THRESHOLD_PERCENTAGES): [vol.Coerce(float)],
    }
)
DELETE_BUDGET_SERVICE_SCHEMA = vol.Schema(
    {**DEVICE_SERVICE_SCHEMA, vol.Required(CONF_BUDGET_ID): str}
)
GET_NOTIFICATION_PREFERENCES_SERVICE_SCHEMA = vol.Schema(BASE_SERVICE_SCHEMA)
UPDATE_NOTIFICATION_PREFERENCES_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Required(CONF_SUBSCRIPTION_ID): str,
        vol.Required(CONF_PREFERENCE): vol.In(
            tuple(preference.name for preference in NotificationPreference)
        ),
        vol.Required(CONF_ENABLED): bool,
    }
)
LIST_USAGE_ALERT_HISTORY_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
LIST_USAGE_SPANS_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Optional(CONF_SINCE_DATETIME): str,
        vol.Optional(CONF_UNTIL_DATETIME): str,
    }
)
RELABEL_SPAN_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_SPAN_ID): str,
        vol.Required(CONF_SPAN_TYPE): str,
    }
)
LIST_LOCATION_ACCESS_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
GRANT_LOCATION_ACCESS_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_EMAIL_ADDRESS): str,
    }
)
REVOKE_LOCATION_ACCESS_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_ACCESS_ID): str,
    }
)
LIST_SUBSCRIPTIONS_SERVICE_SCHEMA = vol.Schema(BASE_SERVICE_SCHEMA)
LIST_EMERGENCY_CONTACTS_SERVICE_SCHEMA = vol.Schema(BASE_SERVICE_SCHEMA)
CREATE_EMERGENCY_CONTACT_SERVICE_SCHEMA = vol.Schema(
    {
        **DEVICE_SERVICE_SCHEMA,
        vol.Required(CONF_CONTACT_NAME): str,
        vol.Required(CONF_EMAIL_ADDRESS): str,
    }
)
UPDATE_EMERGENCY_CONTACT_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Required(CONF_SUBSCRIPTION_ID): str,
        vol.Optional(CONF_CONTACT_NAME): str,
        vol.Optional(CONF_EMAIL_ADDRESS): str,
    }
)
DELETE_EMERGENCY_CONTACT_SERVICE_SCHEMA = vol.Schema(
    {**BASE_SERVICE_SCHEMA, vol.Required(CONF_SUBSCRIPTION_ID): str}
)
GET_METER_ACCURACY_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
CHECK_METER_ACCURACY_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
GET_LOCATION_DIAGNOSTICS_SERVICE_SCHEMA = vol.Schema(DEVICE_SERVICE_SCHEMA)
SET_NOTIFICATION_READ_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Required(CONF_NOTIFICATION_ID): str,
        vol.Required(CONF_READ, default=True): bool,
    }
)
DELETE_NOTIFICATION_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Required(CONF_NOTIFICATION_ID): str,
    }
)
CALL_API_SERVICE_SCHEMA = vol.Schema(
    {
        **BASE_SERVICE_SCHEMA,
        vol.Required(CONF_METHOD): vol.In(sorted(GENERIC_READ_METHODS)),
        vol.Optional(CONF_ARGUMENTS, default={}): dict,
    }
)


def _is_sensitive_key(key: object) -> bool:
    """Return whether a response key may contain credentials or private tokens."""
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return normalized in _SENSITIVE_EXACT_KEYS or any(
        part in normalized for part in _SENSITIVE_KEY_PARTS
    )


def _serialize(value: object) -> JsonValueType:
    """Convert PyFlumeNG values to JSON while recursively removing secrets."""
    if isinstance(value, FlumeModel):
        value = value.to_dict()
    if isinstance(value, Mapping):
        return {
            str(key): _REDACTED if _is_sensitive_key(key) else _serialize(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_serialize(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _payload_from_call(
    call: ServiceCall,
    fields: Sequence[str],
) -> dict[str, JsonValueType]:
    """Merge optional legacy payload data with discoverable typed fields."""
    payload = cast(dict[str, JsonValueType], dict(call.data.get(CONF_PAYLOAD, {})))
    for field in fields:
        if field in call.data:
            payload[field] = cast(JsonValueType, call.data[field])
    return payload


def _require_nonempty_payload(payload: Mapping[str, JsonValueType]) -> None:
    """Reject create/update actions that contain no actual changes."""
    if not payload:
        raise ServiceValidationError("At least one field must be supplied")


def _validate_usage_alert_payload(
    payload: Mapping[str, JsonValueType],
    existing: UsageAlertRule | None = None,
) -> None:
    """Apply the customer portal's usage-alert input constraints."""
    if CONF_NAME in payload:
        name = payload[CONF_NAME]
        if not isinstance(name, str) or len(name) > 32:
            raise ServiceValidationError(
                "Usage-alert rule name must be 32 characters or fewer"
            )
    if CONF_DURATION in payload:
        duration = payload[CONF_DURATION]
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise ServiceValidationError("Usage-alert duration must be a number")
        if duration < 5 or duration > 1439:
            raise ServiceValidationError(
                "Usage-alert duration must be between 5 and 1439 minutes"
            )
    if CONF_FLOW_RATE in payload:
        flow_rate = payload[CONF_FLOW_RATE]
        if isinstance(flow_rate, bool) or not isinstance(flow_rate, (int, float)):
            raise ServiceValidationError("Usage-alert flow rate must be a number")
        if flow_rate < 0 or flow_rate > 40.9:
            raise ServiceValidationError(
                "Usage-alert flow rate must be between 0 and 40.9 GPM"
            )
    if CONF_NOTIFY_EVERY in payload:
        notify_every = payload[CONF_NOTIFY_EVERY]
        if isinstance(notify_every, bool) or not isinstance(notify_every, (int, float)):
            raise ServiceValidationError("Usage-alert repeat interval must be a number")
        if notify_every < 0 or notify_every > 20100:
            raise ServiceValidationError(
                "Usage-alert repeat interval must be between 0 and 20100 minutes"
            )

    duration = payload.get(
        CONF_DURATION, existing.duration if existing is not None else None
    )
    notify_every = payload.get(
        CONF_NOTIFY_EVERY, existing.notify_every if existing is not None else None
    )
    if (
        isinstance(duration, (int, float))
        and not isinstance(duration, bool)
        and isinstance(notify_every, (int, float))
        and not isinstance(notify_every, bool)
        and notify_every < 2 * duration
    ):
        raise ServiceValidationError(
            "Usage-alert repeat interval must be at least twice the duration"
        )


def _normalize_time(value: object, field: str) -> str:
    """Validate and normalize a portal time-of-day value."""
    if not isinstance(value, str) or (match := _TIME_RE.fullmatch(value)) is None:
        raise ServiceValidationError(f"{field} must be HH:MM or HH:MM:SS")
    return (
        f"{match.group('hour')}:{match.group('minute')}:{match.group('second') or '00'}"
    )


def _time_minutes(value: str) -> int:
    """Return minutes after midnight for a validated time string."""
    parsed = datetime.strptime(value, "%H:%M:%S")
    return parsed.hour * 60 + parsed.minute


def _parse_rrule(rrule_str: str) -> dict[str, str]:
    """Parse the simple weekly recurrence shape used by Flume DNA schedules."""
    rule = rrule_str.removeprefix("RRULE:")
    result: dict[str, str] = {}
    for item in rule.split(";"):
        if not item:
            continue
        key, separator, value = item.partition("=")
        if separator:
            result[key.upper()] = value
    return result


def _validate_rrule(rrule_str: object) -> None:
    """Validate the weekly recurrence constraints used by the portal."""
    if not isinstance(rrule_str, str):
        raise ServiceValidationError("rrule_str must be a string")
    parts = _parse_rrule(rrule_str)
    if parts.get("FREQ") != "WEEKLY":
        raise ServiceValidationError("Do-not-alert recurrence frequency must be WEEKLY")
    try:
        interval = int(parts.get("INTERVAL", "1"))
    except ValueError as err:
        raise ServiceValidationError(
            "Do-not-alert interval must be an integer"
        ) from err
    if not 1 <= interval <= 10:
        raise ServiceValidationError(
            "Do-not-alert WEEKLY interval must be between 1 and 10"
        )
    if byday := parts.get("BYDAY"):
        days = byday.split(",")
        if not days or any(day not in _WEEKDAYS for day in days):
            raise ServiceValidationError("Do-not-alert weekdays contain an invalid day")


def _build_dna_payload(
    call: ServiceCall,
    existing: DoNotAlertSchedule | None = None,
) -> dict[str, JsonValueType]:
    """Build a DNA payload from typed fields and optional legacy payload data."""
    payload = cast(dict[str, JsonValueType], dict(call.data.get(CONF_PAYLOAD, {})))
    for field in (CONF_NAME, CONF_DESCRIPTION, CONF_START_TIME, CONF_END_TIME):
        if field in call.data:
            payload[field] = cast(JsonValueType, call.data[field])

    if CONF_SPAN_TYPES in call.data:
        span_types = cast(list[str], call.data[CONF_SPAN_TYPES])
        if span_types:
            payload[CONF_SPAN_TYPES] = span_types
        else:
            payload.pop(CONF_SPAN_TYPES, None)
    elif payload.get(CONF_SPAN_TYPES) == []:
        payload.pop(CONF_SPAN_TYPES, None)

    typed_recurrence = CONF_WEEKDAYS in call.data or CONF_INTERVAL in call.data
    if typed_recurrence:
        current_rrule = cast(
            str | None,
            payload.get("rrule_str")
            or (existing.rrule_str if existing is not None else None),
        )
        current_parts = _parse_rrule(current_rrule) if current_rrule else {}
        interval = int(call.data.get(CONF_INTERVAL, current_parts.get("INTERVAL", 1)))
        weekdays = cast(
            list[str] | None,
            call.data.get(CONF_WEEKDAYS)
            or (
                current_parts.get("BYDAY", "").split(",")
                if current_parts.get("BYDAY")
                else None
            ),
        )
        parts = ["FREQ=WEEKLY", f"INTERVAL={interval}"]
        if weekdays:
            parts.append(f"BYDAY={','.join(weekdays)}")
        payload["rrule_str"] = f"RRULE:{';'.join(parts)}"

    return payload


def _validate_dna_payload(
    payload: dict[str, JsonValueType],
    existing: DoNotAlertSchedule | None = None,
) -> None:
    """Validate portal DNA naming, time, recurrence, and span constraints."""
    if CONF_NAME in payload:
        name = payload[CONF_NAME]
        if not isinstance(name, str) or len(name) > 32:
            raise ServiceValidationError(
                "Do-not-alert schedule name must be 32 characters or fewer"
            )

    if CONF_START_TIME in payload:
        payload[CONF_START_TIME] = _normalize_time(
            payload[CONF_START_TIME], CONF_START_TIME
        )
    if CONF_END_TIME in payload:
        payload[CONF_END_TIME] = _normalize_time(payload[CONF_END_TIME], CONF_END_TIME)

    start_time = cast(str | None, payload.get(CONF_START_TIME))
    end_time = cast(str | None, payload.get(CONF_END_TIME))
    if start_time is None and existing is not None and existing.start_time:
        start_time = _normalize_time(existing.start_time, CONF_START_TIME)
    if end_time is None and existing is not None and existing.end_time:
        end_time = _normalize_time(existing.end_time, CONF_END_TIME)
    if (start_time is None) != (end_time is None):
        raise ServiceValidationError(
            "start_time and end_time must be supplied together"
        )
    if start_time is not None and end_time is not None:
        duration = (_time_minutes(end_time) - _time_minutes(start_time)) % (24 * 60)
        if duration < 4:
            raise ServiceValidationError(
                "Do-not-alert end_time must be at least 4 minutes after start_time"
            )

    if "rrule_str" in payload:
        _validate_rrule(payload["rrule_str"])
    if CONF_SPAN_TYPES in payload:
        span_types = payload[CONF_SPAN_TYPES]
        if not isinstance(span_types, list) or any(
            not isinstance(span_type, str) or not span_type for span_type in span_types
        ):
            raise ServiceValidationError("span_types must contain non-empty strings")


def _validate_budget_payload(payload: Mapping[str, JsonValueType]) -> None:
    """Validate simple budget fields before portal writes."""
    if CONF_VALUE in payload:
        value = payload[CONF_VALUE]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ServiceValidationError("Budget value must be greater than zero")
    if CONF_RECUR_MULTIPLIER in payload:
        multiplier = payload[CONF_RECUR_MULTIPLIER]
        if (
            isinstance(multiplier, bool)
            or not isinstance(multiplier, int)
            or multiplier < 1
        ):
            raise ServiceValidationError("Budget recur_multiplier must be at least 1")


def _apply_budget_threshold_percentages(
    call: ServiceCall,
    payload: dict[str, JsonValueType],
    current_value: float | None = None,
) -> None:
    """Convert user-friendly threshold percentages to Flume's gallon values."""
    if CONF_THRESHOLD_PERCENTAGES not in call.data:
        return
    percentages = cast(list[float], call.data[CONF_THRESHOLD_PERCENTAGES])
    if any(percentage < 0 or percentage > 100 for percentage in percentages):
        raise ServiceValidationError(
            "Budget threshold percentages must be between 0 and 100"
        )
    target = payload.get(CONF_VALUE, current_value)
    if isinstance(target, bool) or not isinstance(target, (int, float)) or target <= 0:
        raise ServiceValidationError(
            "Budget value is required when threshold_percentages are supplied"
        )
    payload[CONF_THRESHOLDS] = [
        round(float(target) * percentage / 100.0) for percentage in percentages
    ]


def _location_id(device: Device) -> str:
    """Return the selected sensor's Flume location ID."""
    location_id = (
        device.location.id
        if device.location is not None and device.location.id is not None
        else device.location_id
    )
    if location_id is None:
        raise ServiceValidationError("Selected Flume sensor has no location ID")
    return str(location_id)


def _format_annotation(annotation: object) -> str | None:
    """Format an inspect annotation for a JSON service response."""
    if annotation is inspect.Signature.empty:
        return None
    return inspect.formatannotation(annotation)


def _describe_client_method(method_name: str) -> JsonObjectType:
    """Return callable metadata for one public PyFlumeNG client method."""
    method = getattr(FlumeClient, method_name)
    method_signature = inspect.signature(method)
    parameters = list(method_signature.parameters.values())
    if parameters and parameters[0].name == "self":
        parameters = parameters[1:]
    public_signature = method_signature.replace(parameters=parameters)

    parameter_details: list[JsonValueType] = []
    for parameter in parameters:
        detail: JsonObjectType = {
            "name": parameter.name,
            "kind": parameter.kind.name.lower(),
            "required": (
                parameter.default is inspect.Parameter.empty
                and parameter.kind
                not in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                )
            ),
        }
        annotation = _format_annotation(parameter.annotation)
        if annotation is not None:
            detail["type"] = annotation
        if parameter.default is not inspect.Parameter.empty:
            detail["default"] = _serialize(parameter.default)
        parameter_details.append(detail)

    doc = inspect.getdoc(method)
    result: JsonObjectType = {
        "signature": str(public_signature),
        "parameters": parameter_details,
    }
    return_annotation = _format_annotation(public_signature.return_annotation)
    if return_annotation is not None:
        result["returns"] = return_annotation
    if doc:
        result["description"] = doc.splitlines()[0]
    return result


def _entry_from_call(call: ServiceCall) -> FlumeConfigEntry:
    """Resolve and validate the selected Flume config entry."""
    return service.async_get_config_entry(
        call.hass,
        DOMAIN,
        call.data[CONF_CONFIG_ENTRY],
    )


def _device_from_call(call: ServiceCall) -> tuple[FlumeConfigEntry, Device, str]:
    """Resolve an HA device selection to its Flume config entry and device ID."""
    device_entry = dr.async_get(call.hass).async_get(call.data[CONF_DEVICE])
    if device_entry is None:
        raise ServiceValidationError("Selected Home Assistant device was not found")

    entry = next(
        (
            cast(FlumeConfigEntry, config_entry)
            for entry_id in device_entry.config_entries
            if (config_entry := call.hass.config_entries.async_get_entry(entry_id))
            is not None
            and config_entry.domain == DOMAIN
        ),
        None,
    )
    if entry is None:
        raise ServiceValidationError(
            "Selected Home Assistant device is not managed by this Flume integration"
        )
    flume_ids = [
        identifier[1]
        for identifier in device_entry.identifiers
        if identifier[0] == DOMAIN
    ]
    if not flume_ids:
        raise ServiceValidationError(
            "Selected Home Assistant device is not a Flume device"
        )
    flume_id = flume_ids[0]
    flume_device = next(
        (
            device
            for device in entry.runtime_data.devices
            if device.id is not None and str(device.id) == flume_id
        ),
        None,
    )
    if flume_device is None or flume_device.type != FLUME_TYPE_SENSOR:
        raise ServiceValidationError("Selected device is not a Flume water sensor")
    return entry, flume_device, flume_id


def _require_capability(entry: FlumeConfigEntry, capability: str) -> None:
    """Validate that the selected account supports a required API capability."""
    if capability not in entry.runtime_data.auth.capabilities:
        raise ServiceValidationError(
            f"This Flume action requires the {capability} capability"
        )


def _generic_method_access(method_name: str) -> str:
    """Classify one public PyFlumeNG method for generic HA access."""
    return "read_only" if method_name in GENERIC_READ_METHODS else "blocked"


def _validate_generic_method(method_name: str) -> None:
    """Reject any method outside the deliberately read-only generic surface."""
    if method_name not in GENERIC_READ_METHODS:
        raise ServiceValidationError(
            "This PyFlumeNG method is blocked from generic call_api access; "
            "use a dedicated Flume action instead"
        )


async def _async_refresh_portal_device(
    hass: HomeAssistant, entry: FlumeConfigEntry, device_id: str
) -> None:
    """Refresh portal entity data immediately after a user-initiated write."""
    await get_portal_device_coordinator(hass, entry, device_id).async_refresh()


async def _async_list_subscriptions(
    hass: HomeAssistant, entry: FlumeConfigEntry
) -> list[Subscription]:
    """List subscriptions without ever serializing alert_info unredacted."""
    result = await _async_call_client_method(hass, entry, "list_subscriptions", {})
    return cast(list[Subscription], result)


async def _async_get_subscription(
    hass: HomeAssistant,
    entry: FlumeConfigEntry,
    subscription_id: str,
    *,
    emergency_contact: bool | None = None,
) -> Subscription:
    """Resolve a subscription owned by this account and optionally its kind."""
    subscription = next(
        (
            item
            for item in await _async_list_subscriptions(hass, entry)
            if item.id is not None and str(item.id) == subscription_id
        ),
        None,
    )
    if subscription is None:
        raise ServiceValidationError("Subscription was not found on this Flume account")
    if (
        emergency_contact is not None
        and subscription.emergency_contact != emergency_contact
    ):
        expected = (
            "emergency contact" if emergency_contact else "notification subscription"
        )
        raise ServiceValidationError(f"Selected subscription is not a {expected}")
    return subscription


def _subscription_email(subscription: Subscription) -> str | None:
    """Extract an emergency-contact email from the typed subscription model."""
    alert_info = subscription.alert_info
    if isinstance(alert_info, str):
        return alert_info
    info = getattr(alert_info, "info", None)
    return info if isinstance(info, str) else None


def _validate_raw_arguments(arguments: Mapping[str, JsonValueType]) -> None:
    """Keep raw calls on the configured Flume API host."""
    path = arguments.get("path")
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        raise ServiceValidationError(
            "Raw Flume API calls require a relative path beginning with '/'"
        )


async def _async_call_client_method(
    hass: HomeAssistant,
    entry: FlumeConfigEntry,
    method_name: str,
    arguments: dict[str, JsonValueType],
) -> object:
    """Call one allowlisted PyFlumeNG client method in the executor."""
    if method_name == "raw":
        _validate_raw_arguments(arguments)
    client_method = cast(
        "Callable[..., object]", getattr(entry.runtime_data.client, method_name)
    )
    try:
        result = await hass.async_add_executor_job(partial(client_method, **arguments))
    except FlumeCapabilityError as err:
        raise ServiceValidationError(str(err)) from err
    except FlumeHTTPError as err:
        raise ServiceValidationError(str(err)) from err
    except (TypeError, ValueError) as err:
        raise ServiceValidationError(str(err)) from err
    async_persist_auth_token(hass, entry)
    return result


@callback
def async_setup_services(hass: HomeAssistant) -> None:  # noqa: C901
    """Register Flume services."""
    if hass.services.has_service(DOMAIN, SERVICE_CALL_API):
        return

    @callback
    def list_notifications(call: ServiceCall) -> ServiceResponse:
        """Return the latest fetched notifications."""
        entry = _entry_from_call(call)
        return {
            "notifications": _serialize(
                entry.runtime_data.notifications_coordinator.notifications
            )
        }

    async def list_usage_alert_rules(call: ServiceCall) -> ServiceResponse:
        """Return usage-alert rules for one device or all Flume sensors."""
        entry = _entry_from_call(call)
        if CONF_DEVICE in call.data:
            selected_entry, _, device_id = _device_from_call(call)
            if selected_entry.entry_id != entry.entry_id:
                raise ServiceValidationError(
                    "Selected Flume device does not belong to the selected config entry"
                )
            device_ids = [device_id]
        else:
            device_ids = [
                str(device.id)
                for device in entry.runtime_data.devices
                if device.id is not None and device.type == FLUME_TYPE_SENSOR
            ]
        result: JsonObjectType = {}
        for device_id in device_ids:
            rules = await hass.async_add_executor_job(
                entry.runtime_data.client.list_usage_alert_rules, device_id
            )
            result[device_id] = _serialize(rules)
        async_persist_auth_token(hass, entry)
        return {"devices": result}

    async def create_usage_alert_rule(call: ServiceCall) -> ServiceResponse:
        """Create a usage-alert rule and refresh its Home Assistant entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        payload = _payload_from_call(call, _USAGE_ALERT_TYPED_FIELDS)
        if CONF_DURATION in payload and CONF_NOTIFY_EVERY not in payload:
            duration = payload[CONF_DURATION]
            if isinstance(duration, (int, float)) and not isinstance(duration, bool):
                payload[CONF_NOTIFY_EVERY] = int(duration * 2)
        _require_nonempty_payload(payload)
        _validate_usage_alert_payload(payload)
        result = await _async_call_client_method(
            hass,
            entry,
            "create_usage_alert_rule",
            {
                "device_id": device_id,
                "payload": payload,
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "rules": _serialize(list(coordinator.data.rules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def update_usage_alert_rule(call: ServiceCall) -> ServiceResponse:
        """Update a usage-alert rule and refresh its Home Assistant entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        payload = _payload_from_call(call, _USAGE_ALERT_TYPED_FIELDS)
        _require_nonempty_payload(payload)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        existing = coordinator.data.rules.get(call.data[CONF_RULE_ID])
        _validate_usage_alert_payload(payload, existing)
        result = await _async_call_client_method(
            hass,
            entry,
            "update_usage_alert_rule",
            {
                "device_id": device_id,
                "rule_id": call.data[CONF_RULE_ID],
                "payload": payload,
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "rules": _serialize(list(coordinator.data.rules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def delete_usage_alert_rule(call: ServiceCall) -> ServiceResponse:
        """Delete a usage-alert rule and refresh its Home Assistant entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        result = await _async_call_client_method(
            hass,
            entry,
            "delete_usage_alert_rule",
            {
                "device_id": device_id,
                "rule_id": call.data[CONF_RULE_ID],
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "rules": _serialize(list(coordinator.data.rules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    @callback
    def list_api_methods(call: ServiceCall) -> ServiceResponse:
        """Describe the read-only PyFlumeNG surface allowed through call_api."""
        entry = _entry_from_call(call)
        return {
            "methods": sorted(GENERIC_READ_METHODS),
            "method_details": {
                method_name: {
                    **_describe_client_method(method_name),
                    "access": _generic_method_access(method_name),
                }
                for method_name in sorted(GENERIC_READ_METHODS)
            },
            "blocked_methods": sorted(GENERIC_BLOCKED_METHODS),
            "capabilities": sorted(entry.runtime_data.auth.capabilities),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def set_usage_alert_rule_active(call: ServiceCall) -> None:
        """Enable or disable a usage-alert rule."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        await _async_call_client_method(
            hass,
            entry,
            "set_usage_alert_rule_active",
            {
                "device_id": device_id,
                "rule_id": call.data[CONF_RULE_ID],
                "active": call.data[CONF_ACTIVE],
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)

    async def list_do_not_alert_schedules(call: ServiceCall) -> ServiceResponse:
        """Return all do-not-alert schedules for one Flume sensor."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        schedules = await _async_call_client_method(
            hass,
            entry,
            "list_do_not_alert_schedules",
            {"device_id": device_id},
        )
        return {
            "schedules": _serialize(schedules),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def create_do_not_alert_schedule(call: ServiceCall) -> ServiceResponse:
        """Create a do-not-alert schedule and refresh schedule entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        payload = _build_dna_payload(call)
        _require_nonempty_payload(payload)
        _validate_dna_payload(payload)
        result = await _async_call_client_method(
            hass,
            entry,
            "create_do_not_alert_schedule",
            {
                "device_id": device_id,
                "payload": payload,
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "schedules": _serialize(list(coordinator.data.schedules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def update_do_not_alert_schedule(call: ServiceCall) -> ServiceResponse:
        """Update a do-not-alert schedule and refresh schedule entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        existing = coordinator.data.schedules.get(call.data[CONF_SCHEDULE_ID])
        payload = _build_dna_payload(call, existing)
        _require_nonempty_payload(payload)
        _validate_dna_payload(payload, existing)
        result = await _async_call_client_method(
            hass,
            entry,
            "update_do_not_alert_schedule",
            {
                "device_id": device_id,
                "schedule_id": call.data[CONF_SCHEDULE_ID],
                "payload": payload,
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "schedules": _serialize(list(coordinator.data.schedules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def delete_do_not_alert_schedule(call: ServiceCall) -> ServiceResponse:
        """Delete a do-not-alert schedule and refresh schedule entities."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        result = await _async_call_client_method(
            hass,
            entry,
            "delete_do_not_alert_schedule",
            {
                "device_id": device_id,
                "schedule_id": call.data[CONF_SCHEDULE_ID],
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "schedules": _serialize(list(coordinator.data.schedules.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def get_usage_breakdown(call: ServiceCall) -> ServiceResponse:
        """Return typed portal-style usage categories for a selected time range."""
        entry, device, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        since_datetime = call.data.get(CONF_SINCE_DATETIME)
        until_datetime = call.data.get(CONF_UNTIL_DATETIME)
        if (since_datetime is None) != (until_datetime is None):
            raise ServiceValidationError(
                "since_datetime and until_datetime must be supplied together"
            )
        if since_datetime is None:
            time_zone = device.location.tz if device.location is not None else None
            since_datetime, until_datetime = usage_breakdown_range(time_zone)
        location_id = (
            str(device.location.id)
            if device.location is not None and device.location.id is not None
            else None
        )
        result = await _async_call_client_method(
            hass,
            entry,
            "get_usage_breakdown",
            {
                "device_id": device_id,
                "since_datetime": since_datetime,
                "until_datetime": until_datetime,
                "location_id": location_id,
            },
        )
        return {
            "breakdown": _serialize(result),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_budgets(call: ServiceCall) -> ServiceResponse:
        """List water budgets for one selected Flume sensor."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        budgets = await _async_call_client_method(
            hass, entry, "list_budgets", {"device_id": device_id}
        )
        return {
            "budgets": _serialize(budgets),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def create_budget(call: ServiceCall) -> ServiceResponse:
        """Create a water budget using validated portal fields."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        payload = _payload_from_call(call, _BUDGET_TYPED_FIELDS)
        _apply_budget_threshold_percentages(call, payload)
        _require_nonempty_payload(payload)
        _validate_budget_payload(payload)
        result = await _async_call_client_method(
            hass,
            entry,
            "create_budget",
            {"device_id": device_id, "payload": payload},
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "budgets": _serialize(list(coordinator.data.budgets.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def update_budget(call: ServiceCall) -> ServiceResponse:
        """Update a water budget using validated portal fields."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        budget_id = call.data[CONF_BUDGET_ID]
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        current_budget = coordinator.data.budgets.get(budget_id)
        current_value = current_budget.target if current_budget is not None else None
        payload = _payload_from_call(call, (CONF_VALUE, CONF_THRESHOLDS))
        _apply_budget_threshold_percentages(call, payload, current_value)
        _require_nonempty_payload(payload)
        _validate_budget_payload(payload)
        result = await _async_call_client_method(
            hass,
            entry,
            "update_budget",
            {
                "device_id": device_id,
                "budget_id": budget_id,
                "payload": payload,
            },
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "budgets": _serialize(list(coordinator.data.budgets.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def delete_budget(call: ServiceCall) -> ServiceResponse:
        """Delete one selected water budget."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        budget_id = call.data[CONF_BUDGET_ID]
        coordinator = get_portal_device_coordinator(hass, entry, device_id)
        if budget_id not in coordinator.data.budgets:
            raise ServiceValidationError(
                "Budget was not found on the selected Flume sensor"
            )
        result = await _async_call_client_method(
            hass,
            entry,
            "delete_budget",
            {"device_id": device_id, "budget_id": budget_id},
        )
        await _async_refresh_portal_device(hass, entry, device_id)
        return {
            "result": _serialize(result),
            "budgets": _serialize(list(coordinator.data.budgets.values())),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def get_notification_preferences(call: ServiceCall) -> ServiceResponse:
        """Return notification-channel preferences without exposing alert_info."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_api")
        subscriptions = [
            item
            for item in await _async_list_subscriptions(hass, entry)
            if not item.emergency_contact
        ]
        return {
            "preferences": _serialize(subscriptions),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def update_notification_preferences(call: ServiceCall) -> ServiceResponse:
        """Toggle one known preference while preserving all other/unknown bits."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_writes")
        subscription_id = call.data[CONF_SUBSCRIPTION_ID]
        await _async_get_subscription(
            hass, entry, subscription_id, emergency_contact=False
        )
        result = await _async_call_client_method(
            hass,
            entry,
            "set_subscription_notification_preference",
            {
                "subscription_id": subscription_id,
                "preference": NotificationPreference[call.data[CONF_PREFERENCE]],
                "enabled": call.data[CONF_ENABLED],
            },
        )
        return {
            "result": _serialize(result),
            "preferences": _serialize(
                [
                    item
                    for item in await _async_list_subscriptions(hass, entry)
                    if not item.emergency_contact
                ]
            ),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_usage_alert_history(call: ServiceCall) -> ServiceResponse:
        """Return triggered usage-alert history for one selected sensor."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        alerts = await _async_call_client_method(
            hass, entry, "list_usage_alert_history", {"device_id": device_id}
        )
        return {
            "alerts": _serialize(alerts),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_usage_spans(call: ServiceCall) -> ServiceResponse:
        """Return classified usage spans and the location's relabel choices."""
        entry, device, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        since_datetime = call.data.get(CONF_SINCE_DATETIME)
        until_datetime = call.data.get(CONF_UNTIL_DATETIME)
        if (since_datetime is None) != (until_datetime is None):
            raise ServiceValidationError(
                "since_datetime and until_datetime must be supplied together"
            )
        if since_datetime is None:
            time_zone = device.location.tz if device.location is not None else None
            since_datetime, until_datetime = usage_breakdown_range(time_zone)
        spans = await _async_call_client_method(
            hass,
            entry,
            "list_spans",
            {
                "device_id": device_id,
                "since_datetime": since_datetime,
                "until_datetime": until_datetime,
            },
        )
        span_types = await _async_call_client_method(
            hass,
            entry,
            "list_span_types",
            {"location_id": _location_id(device)},
        )
        return {
            "spans": _serialize(spans),
            "span_types": _serialize(span_types),
            "since_datetime": since_datetime,
            "until_datetime": until_datetime,
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def relabel_span(call: ServiceCall) -> ServiceResponse:
        """Relabel one usage span to a portal-advertised editable type."""
        entry, device, device_id = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        span_type = call.data[CONF_SPAN_TYPE].upper()
        span_types = await _async_call_client_method(
            hass,
            entry,
            "list_span_types",
            {"location_id": _location_id(device)},
        )
        target = next(
            (
                item
                for item in cast(Sequence[FlumeModel], span_types)
                if str(item.get("name", "")).upper() == span_type
            ),
            None,
        )
        if target is None or not bool(target.get("can_relabel", False)):
            raise ServiceValidationError(
                "Requested span type is not advertised as relabelable for this location"
            )
        result = await _async_call_client_method(
            hass,
            entry,
            "update_span_type",
            {
                "device_id": device_id,
                "span_id": call.data[CONF_SPAN_ID],
                "span_type": span_type,
            },
        )
        breakdown_coordinator = entry.runtime_data.usage_breakdown_coordinators.get(
            device_id
        )
        if breakdown_coordinator is not None:
            await breakdown_coordinator.async_refresh()
        return {
            "result": _serialize(result),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_location_access(call: ServiceCall) -> ServiceResponse:
        """List people with access to the selected Flume location."""
        entry, device, _ = _device_from_call(call)
        _require_capability(entry, "portal_api")
        access = await _async_call_client_method(
            hass,
            entry,
            "list_location_access",
            {"location_id": _location_id(device)},
        )
        return {
            "access": _serialize(access),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def grant_location_access(call: ServiceCall) -> ServiceResponse:
        """Grant one email address access to the selected Flume location."""
        entry, device, _ = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        location_id = _location_id(device)
        result = await _async_call_client_method(
            hass,
            entry,
            "share_location",
            {
                "location_id": location_id,
                "email_address": call.data[CONF_EMAIL_ADDRESS],
            },
        )
        access = await _async_call_client_method(
            hass, entry, "list_location_access", {"location_id": location_id}
        )
        return {
            "result": _serialize(result),
            "access": _serialize(access),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def revoke_location_access(call: ServiceCall) -> ServiceResponse:
        """Revoke an existing location sharing record."""
        entry, device, _ = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        location_id = _location_id(device)
        access_id = call.data[CONF_ACCESS_ID]
        existing = await _async_call_client_method(
            hass, entry, "list_location_access", {"location_id": location_id}
        )
        if not any(
            item.get("id") is not None and str(item.get("id")) == access_id
            for item in cast(Sequence[FlumeModel], existing)
        ):
            raise ServiceValidationError("Location access record was not found")
        result = await _async_call_client_method(
            hass,
            entry,
            "unshare_location",
            {"location_id": location_id, "access_id": access_id},
        )
        access = await _async_call_client_method(
            hass, entry, "list_location_access", {"location_id": location_id}
        )
        return {
            "result": _serialize(result),
            "access": _serialize(access),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_subscriptions(call: ServiceCall) -> ServiceResponse:
        """List notification subscriptions with alert_info always redacted."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_api")
        subscriptions = await _async_list_subscriptions(hass, entry)
        return {
            "subscriptions": _serialize(subscriptions),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def list_emergency_contacts(call: ServiceCall) -> ServiceResponse:
        """List emergency-contact subscriptions with destination data redacted."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_api")
        contacts = [
            item
            for item in await _async_list_subscriptions(hass, entry)
            if item.emergency_contact
        ]
        return {
            "contacts": _serialize(contacts),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def create_emergency_contact(call: ServiceCall) -> ServiceResponse:
        """Create an emergency-contact subscription for the selected location."""
        entry, device, _ = _device_from_call(call)
        _require_capability(entry, "portal_writes")
        result = await _async_call_client_method(
            hass,
            entry,
            "create_emergency_contact",
            {
                "location_id": _location_id(device),
                "contact_name": call.data[CONF_CONTACT_NAME],
                "email_address": call.data[CONF_EMAIL_ADDRESS],
            },
        )
        return {
            "result": _serialize(result),
            "contacts": _serialize(
                [
                    item
                    for item in await _async_list_subscriptions(hass, entry)
                    if item.emergency_contact
                ]
            ),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def update_emergency_contact(call: ServiceCall) -> ServiceResponse:
        """Update fields on an existing emergency-contact subscription."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_writes")
        subscription_id = call.data[CONF_SUBSCRIPTION_ID]
        subscription = await _async_get_subscription(
            hass, entry, subscription_id, emergency_contact=True
        )
        contact_name = cast(
            str | None, call.data.get(CONF_CONTACT_NAME, subscription.contact_name)
        )
        email_address = cast(
            str | None,
            call.data.get(CONF_EMAIL_ADDRESS, _subscription_email(subscription)),
        )
        if not contact_name or not email_address:
            raise ServiceValidationError(
                "Emergency contact update requires a contact name and email address"
            )
        result = await _async_call_client_method(
            hass,
            entry,
            "update_emergency_contact",
            {
                "subscription_id": subscription_id,
                "contact_name": contact_name,
                "email_address": email_address,
            },
        )
        return {
            "result": _serialize(result),
            "contacts": _serialize(
                [
                    item
                    for item in await _async_list_subscriptions(hass, entry)
                    if item.emergency_contact
                ]
            ),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def delete_emergency_contact(call: ServiceCall) -> ServiceResponse:
        """Delete an existing emergency-contact subscription."""
        entry = _entry_from_call(call)
        _require_capability(entry, "portal_writes")
        subscription_id = call.data[CONF_SUBSCRIPTION_ID]
        await _async_get_subscription(
            hass, entry, subscription_id, emergency_contact=True
        )
        result = await _async_call_client_method(
            hass,
            entry,
            "delete_emergency_contact",
            {"subscription_id": subscription_id},
        )
        return {
            "result": _serialize(result),
            "contacts": _serialize(
                [
                    item
                    for item in await _async_list_subscriptions(hass, entry)
                    if item.emergency_contact
                ]
            ),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def get_meter_accuracy(call: ServiceCall) -> ServiceResponse:
        """Read meter-accuracy diagnostics without starting a support conversation."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        result = await _async_call_client_method(
            hass, entry, "get_meter_accuracy", {"device_id": device_id}
        )
        return {
            "accuracy": _serialize(result),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def check_meter_accuracy(call: ServiceCall) -> ServiceResponse:
        """Run the read-only meter-accuracy precheck without support side effects."""
        entry, _, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        result = await _async_call_client_method(
            hass,
            entry,
            "check_meter_accuracy",
            {"device_id": device_id},
        )
        return {
            "accuracy": _serialize(result),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def get_location_diagnostics(call: ServiceCall) -> ServiceResponse:
        """Return fresh location/device/profile metadata for troubleshooting."""
        entry, device, device_id = _device_from_call(call)
        _require_capability(entry, "portal_api")
        location_id = _location_id(device)
        fresh_device = await _async_call_client_method(
            hass,
            entry,
            "get_device",
            {"device_id": device_id},
        )
        location = await _async_call_client_method(
            hass, entry, "get_location", {"location_id": location_id}
        )
        profile_catalog = await _async_call_client_method(
            hass, entry, "get_location_profiles", {}
        )
        return {
            "device": _serialize(fresh_device),
            "location": _serialize(location),
            "profile_catalog": _serialize(profile_catalog),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    async def set_notification_read(call: ServiceCall) -> None:
        """Mark a notification read or unread."""
        entry = _entry_from_call(call)
        await _async_call_client_method(
            hass,
            entry,
            "set_notification_read",
            {
                "notification_id": call.data[CONF_NOTIFICATION_ID],
                "read": call.data[CONF_READ],
                "portal": "portal_writes" in entry.runtime_data.auth.capabilities,
            },
        )
        await entry.runtime_data.notifications_coordinator.async_refresh()

    async def delete_notification(call: ServiceCall) -> None:
        """Delete a notification from the Flume account."""
        entry = _entry_from_call(call)
        await _async_call_client_method(
            hass,
            entry,
            (
                "delete_portal_notification"
                if "portal_writes" in entry.runtime_data.auth.capabilities
                else "delete_notification"
            ),
            {"notification_id": call.data[CONF_NOTIFICATION_ID]},
        )
        await entry.runtime_data.notifications_coordinator.async_refresh()

    async def call_api(call: ServiceCall) -> ServiceResponse:
        """Invoke an explicitly read-only PyFlumeNG endpoint wrapper."""
        entry = _entry_from_call(call)
        method_name: str = call.data[CONF_METHOD]
        _validate_generic_method(method_name)
        arguments = cast(dict[str, JsonValueType], dict(call.data[CONF_ARGUMENTS]))
        result = await _async_call_client_method(hass, entry, method_name, arguments)
        return {
            "result": _serialize(result),
            "rate_limit": entry.runtime_data.client.rate_limit.to_dict(),
        }

    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_NOTIFICATIONS,
        list_notifications,
        schema=LIST_NOTIFICATIONS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_USAGE_ALERT_RULES,
        list_usage_alert_rules,
        schema=LIST_USAGE_ALERT_RULES_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_DO_NOT_ALERT_SCHEDULES,
        list_do_not_alert_schedules,
        schema=LIST_DO_NOT_ALERT_SCHEDULES_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_USAGE_BREAKDOWN,
        get_usage_breakdown,
        schema=GET_USAGE_BREAKDOWN_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_BUDGETS,
        list_budgets,
        schema=LIST_BUDGETS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_NOTIFICATION_PREFERENCES,
        get_notification_preferences,
        schema=GET_NOTIFICATION_PREFERENCES_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_USAGE_ALERT_HISTORY,
        list_usage_alert_history,
        schema=LIST_USAGE_ALERT_HISTORY_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_USAGE_SPANS,
        list_usage_spans,
        schema=LIST_USAGE_SPANS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_LIST_LOCATION_ACCESS,
        list_location_access,
        schema=LIST_LOCATION_ACCESS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_SUBSCRIPTIONS,
        list_subscriptions,
        schema=LIST_SUBSCRIPTIONS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_EMERGENCY_CONTACTS,
        list_emergency_contacts,
        schema=LIST_EMERGENCY_CONTACTS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_METER_ACCURACY,
        get_meter_accuracy,
        schema=GET_METER_ACCURACY_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_CHECK_METER_ACCURACY,
        check_meter_accuracy,
        schema=CHECK_METER_ACCURACY_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_GET_LOCATION_DIAGNOSTICS,
        get_location_diagnostics,
        schema=GET_LOCATION_DIAGNOSTICS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_API_METHODS,
        list_api_methods,
        schema=vol.Schema(BASE_SERVICE_SCHEMA),
        supports_response=SupportsResponse.ONLY,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_CREATE_USAGE_ALERT_RULE,
        create_usage_alert_rule,
        schema=CREATE_USAGE_ALERT_RULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_UPDATE_USAGE_ALERT_RULE,
        update_usage_alert_rule,
        schema=UPDATE_USAGE_ALERT_RULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_DELETE_USAGE_ALERT_RULE,
        delete_usage_alert_rule,
        schema=DELETE_USAGE_ALERT_RULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_SET_USAGE_ALERT_RULE_ACTIVE,
        set_usage_alert_rule_active,
        schema=SET_USAGE_ALERT_RULE_ACTIVE_SERVICE_SCHEMA,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_CREATE_DO_NOT_ALERT_SCHEDULE,
        create_do_not_alert_schedule,
        schema=CREATE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_UPDATE_DO_NOT_ALERT_SCHEDULE,
        update_do_not_alert_schedule,
        schema=UPDATE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_DELETE_DO_NOT_ALERT_SCHEDULE,
        delete_do_not_alert_schedule,
        schema=DELETE_DO_NOT_ALERT_SCHEDULE_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_CREATE_BUDGET,
        create_budget,
        schema=CREATE_BUDGET_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_UPDATE_BUDGET,
        update_budget,
        schema=UPDATE_BUDGET_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_DELETE_BUDGET,
        delete_budget,
        schema=DELETE_BUDGET_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_UPDATE_NOTIFICATION_PREFERENCES,
        update_notification_preferences,
        schema=UPDATE_NOTIFICATION_PREFERENCES_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_RELABEL_SPAN,
        relabel_span,
        schema=RELABEL_SPAN_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_GRANT_LOCATION_ACCESS,
        grant_location_access,
        schema=GRANT_LOCATION_ACCESS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_REVOKE_LOCATION_ACCESS,
        revoke_location_access,
        schema=REVOKE_LOCATION_ACCESS_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_CREATE_EMERGENCY_CONTACT,
        create_emergency_contact,
        schema=CREATE_EMERGENCY_CONTACT_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_UPDATE_EMERGENCY_CONTACT,
        update_emergency_contact,
        schema=UPDATE_EMERGENCY_CONTACT_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_DELETE_EMERGENCY_CONTACT,
        delete_emergency_contact,
        schema=DELETE_EMERGENCY_CONTACT_SERVICE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_SET_NOTIFICATION_READ,
        set_notification_read,
        schema=SET_NOTIFICATION_READ_SERVICE_SCHEMA,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_DELETE_NOTIFICATION,
        delete_notification,
        schema=DELETE_NOTIFICATION_SERVICE_SCHEMA,
    )
    service.async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_CALL_API,
        call_api,
        schema=CALL_API_SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
