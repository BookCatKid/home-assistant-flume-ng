"""The Flume component."""

from datetime import timedelta
import logging

from homeassistant.const import Platform

DOMAIN = "flume_ng"

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.EVENT,
    Platform.SENSOR,
    Platform.SWITCH,
    Platform.VALVE,
]

# Personal API auth is limited to 120 requests/hour. Portal auth advertises a
# much larger 72,000-request quota for its server-defined response-header window.
NOTIFICATION_SCAN_INTERVAL = timedelta(minutes=5)
PORTAL_NOTIFICATION_SCAN_INTERVAL = timedelta(seconds=15)
DEVICE_SCAN_INTERVAL = timedelta(minutes=1)
PORTAL_CURRENT_FLOW_SCAN_INTERVAL = timedelta(seconds=15)
DEVICE_CONNECTION_SCAN_INTERVAL = timedelta(minutes=60)
PORTAL_SETTINGS_SCAN_INTERVAL = timedelta(minutes=1)
PORTAL_SETTINGS_FALLBACK_SCAN_INTERVAL = timedelta(minutes=15)
USAGE_BREAKDOWN_SCAN_INTERVAL = timedelta(minutes=1)
USAGE_BREAKDOWN_FALLBACK_SCAN_INTERVAL = timedelta(minutes=15)

PORTAL_RATE_LIMIT = 72_000
RATE_LIMIT_FAST_POLL_MIN_REMAINING_RATIO = 0.05

LOGGER = logging.getLogger(__package__)

FLUME_TYPE_BRIDGE = 1
FLUME_TYPE_SENSOR = 2

CONF_AUTH_TYPE = "auth_type"
CONF_TOKEN = "token"
AUTH_TYPE_PORTAL = "portal"
AUTH_TYPE_PERSONAL = "personal"

EVENT_NOTIFICATION = f"{DOMAIN}_notification"


NOTIFICATION_HIGH_FLOW = "High Flow Alert"
NOTIFICATION_LEAK_DETECTED = "Flume Smart Leak Alert"
